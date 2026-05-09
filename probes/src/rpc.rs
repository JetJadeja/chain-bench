use alloy::primitives::{Address, TxHash, U256};
use eyre::{Result, WrapErr, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

pub struct RpcClient {
    http: reqwest::Client,
    url: String,
}

#[derive(Serialize)]
struct JsonRpcRequest<'a> {
    jsonrpc: &'a str,
    id: u64,
    method: &'a str,
    params: Vec<Value>,
}

#[derive(Deserialize)]
struct JsonRpcResponse {
    id: u64,
    result: Value,
    error: Option<String>,
}

impl RpcClient {
    pub fn new(url: &str) -> Self {
        Self {
            http: reqwest::Client::new(),
            url: url.to_string(),
        }
    }

    async fn send_batch(&self, requests: Vec<JsonRpcRequest<'_>>) -> Result<Vec<JsonRpcResponse>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        for attempt in 0u32..=4 {
            let resp = self
                .http
                .post(&self.url)
                .json(&requests)
                .send()
                .await
                .wrap_err("batch RPC request failed")?;

            let status = resp.status();
            if status.as_u16() == 429 {
                if attempt < 4 {
                    let delay_ms = 200u64 * 2u64.pow(attempt);
                    eprintln!("  RPC 429 (attempt {}/5), backoff {delay_ms}ms", attempt + 1);
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    continue;
                }
                bail!("RPC rate limited after 5 retries");
            }
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                bail!("batch RPC returned {status}: {body}");
            }
            let body = resp.text().await.wrap_err("failed to read response body")?;
            return serde_json::from_str(&body)
                .wrap_err_with(|| format!("failed to parse batch response: {}", &body[..body.len().min(200)]));
        }
        unreachable!()
    }

    async fn single_call(&self, method: &str, params: Vec<Value>) -> Result<Value> {
        let responses = self
            .send_batch(vec![JsonRpcRequest {
                jsonrpc: "2.0",
                id: 0,
                method,
                params,
            }])
            .await?;
        let resp = responses
            .into_iter()
            .next()
            .ok_or_else(|| eyre::eyre!("empty response"))?;
        if let Some(err) = resp.error {
            bail!("{method} error: {err}");
        }
        Ok(resp.result)
    }

    fn parse_u64_hex(val: &Value) -> Result<u64> {
        let s = val.as_str().ok_or_else(|| eyre::eyre!("expected hex string"))?;
        Ok(u64::from_str_radix(s.trim_start_matches("0x"), 16)?)
    }

    pub async fn get_nonce(&self, addr: Address) -> Result<u64> {
        let val = self
            .single_call(
                "eth_getTransactionCount",
                vec![Value::String(format!("{addr}")), Value::String("pending".into())],
            )
            .await?;
        Self::parse_u64_hex(&val)
    }

    pub async fn get_confirmed_nonce(&self, addr: Address) -> Result<u64> {
        let val = self
            .single_call(
                "eth_getTransactionCount",
                vec![Value::String(format!("{addr}")), Value::String("latest".into())],
            )
            .await?;
        Self::parse_u64_hex(&val)
    }

    pub async fn get_balance(&self, addr: Address) -> Result<U256> {
        let val = self
            .single_call(
                "eth_getBalance",
                vec![Value::String(format!("{addr}")), Value::String("latest".into())],
            )
            .await?;
        let s = val.as_str().ok_or_else(|| eyre::eyre!("expected hex string"))?;
        Ok(U256::from_str_radix(s.trim_start_matches("0x"), 16)?)
    }

    pub async fn get_gas_price(&self) -> Result<u128> {
        let val = self.single_call("eth_gasPrice", vec![]).await?;
        Ok(Self::parse_u64_hex(&val)? as u128)
    }

    pub async fn get_block_number(&self) -> Result<u64> {
        let val = self.single_call("eth_blockNumber", vec![]).await?;
        Self::parse_u64_hex(&val)
    }

    pub async fn get_block(&self, number: u64) -> Result<Option<BlockInfo>> {
        let val = self
            .single_call(
                "eth_getBlockByNumber",
                vec![Value::String(format!("0x{number:x}")), Value::Bool(false)],
            )
            .await?;
        if val.is_null() {
            return Ok(None);
        }
        let timestamp = val
            .get("timestamp")
            .and_then(|v| v.as_str())
            .map(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).unwrap_or(0))
            .unwrap_or(0);
        let tx_count = val
            .get("transactions")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        Ok(Some(BlockInfo {
            number,
            timestamp,
            tx_count,
        }))
    }

    pub async fn batch_send_raw(&self, raw_txs: &[Vec<u8>]) -> Result<Vec<SendResult>> {
        let requests: Vec<_> = raw_txs
            .iter()
            .enumerate()
            .map(|(i, tx)| JsonRpcRequest {
                jsonrpc: "2.0",
                id: i as u64,
                method: "eth_sendRawTransaction",
                params: vec![Value::String(format!("0x{}", alloy::hex::encode(tx)))],
            })
            .collect();

        let responses = self.send_batch(requests).await?;

        let mut results: Vec<Option<SendResult>> = (0..raw_txs.len()).map(|_| None).collect();
        for resp in responses {
            let idx = resp.id as usize;
            if idx >= raw_txs.len() {
                continue;
            }
            let result = if let Some(err) = resp.error {
                SendResult::Rejected(err)
            } else if let Value::String(hash_str) = &resp.result {
                match hash_str.parse::<TxHash>() {
                    Ok(hash) => SendResult::Accepted(hash),
                    Err(e) => SendResult::Rejected(format!("invalid hash: {e}")),
                }
            } else {
                SendResult::Rejected(format!("unexpected response: {:?}", resp.result))
            };
            results[idx] = Some(result);
        }

        Ok(results
            .into_iter()
            .enumerate()
            .map(|(i, r)| r.unwrap_or(SendResult::Rejected(format!("no response for tx {i}"))))
            .collect())
    }
}

pub enum SendResult {
    Accepted(TxHash),
    Rejected(String),
}

pub struct BlockInfo {
    pub number: u64,
    pub timestamp: u64,
    pub tx_count: usize,
}
