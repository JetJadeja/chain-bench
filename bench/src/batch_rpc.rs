use alloy::primitives::{Address, TxHash, U256};
use alloy::rpc::types::TransactionReceipt;
use eyre::{Result, WrapErr};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone)]
pub struct BatchRpcClient {
    http: reqwest::Client,
    url: String,
}

#[derive(Serialize)]
struct JsonRpcRequest<'a> {
    jsonrpc: &'static str,
    id: u64,
    method: &'a str,
    params: Vec<Value>,
}

#[derive(Deserialize)]
struct JsonRpcResponse {
    id: u64,
    #[serde(default)]
    result: Value,
    error: Option<Value>,
}

impl BatchRpcClient {
    pub fn new(url: &str) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(120))
                .build()
                .expect("failed to build HTTP client"),
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
                    tracing::warn!(
                        "RPC 429 (attempt {}/5), backoff {delay_ms}ms",
                        attempt + 1,
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    continue;
                }
                eyre::bail!("RPC rate limited after 5 retries");
            }

            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                eyre::bail!("batch RPC returned {status}: {body}");
            }

            return resp.json().await.wrap_err("failed to parse batch response");
        }

        unreachable!()
    }

    pub async fn batch_receipts(
        &self,
        hashes: &[TxHash],
    ) -> Result<Vec<(TxHash, Option<TransactionReceipt>)>> {
        let requests: Vec<_> = hashes
            .iter()
            .enumerate()
            .map(|(i, hash)| JsonRpcRequest {
                jsonrpc: "2.0",
                id: i as u64,
                method: "eth_getTransactionReceipt",
                params: vec![Value::String(format!("{hash}"))],
            })
            .collect();

        let responses = self.send_batch(requests).await?;

        let mut results = Vec::with_capacity(hashes.len());
        for resp in responses {
            let idx = resp.id as usize;
            if idx >= hashes.len() {
                continue;
            }
            let receipt: Option<TransactionReceipt> = match resp.result {
                Value::Null => None,
                val => serde_json::from_value(val).ok(),
            };
            results.push((hashes[idx], receipt));
        }

        results.sort_by_key(|(_, r)| r.is_none());
        Ok(results)
    }

    pub async fn batch_send_raw(
        &self,
        raw_txs: &[Vec<u8>],
    ) -> Result<Vec<Result<TxHash>>> {
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

        let mut results: Vec<Option<Result<TxHash>>> =
            (0..raw_txs.len()).map(|_| None).collect();
        for resp in responses {
            let idx = resp.id as usize;
            if idx >= raw_txs.len() {
                continue;
            }
            let result = if let Some(err) = resp.error {
                Err(eyre::eyre!("sendRawTransaction error: {err}"))
            } else if let Value::String(hash_str) = &resp.result {
                hash_str
                    .parse::<TxHash>()
                    .wrap_err("invalid tx hash in response")
            } else {
                Err(eyre::eyre!("unexpected response: {:?}", resp.result))
            };
            results[idx] = Some(result);
        }

        Ok(results
            .into_iter()
            .enumerate()
            .map(|(i, r)| r.unwrap_or_else(|| Err(eyre::eyre!("no response for tx {i}"))))
            .collect())
    }
    pub async fn batch_get_balances(&self, addresses: &[Address]) -> Result<Vec<U256>> {
        let requests: Vec<_> = addresses
            .iter()
            .enumerate()
            .map(|(i, addr)| JsonRpcRequest {
                jsonrpc: "2.0",
                id: i as u64,
                method: "eth_getBalance",
                params: vec![
                    Value::String(format!("{addr}")),
                    Value::String("latest".into()),
                ],
            })
            .collect();

        let responses = self.send_batch(requests).await?;
        parse_u256_responses(responses, addresses.len())
    }

    pub async fn batch_get_nonces(&self, addresses: &[Address]) -> Result<Vec<u64>> {
        let requests: Vec<_> = addresses
            .iter()
            .enumerate()
            .map(|(i, addr)| JsonRpcRequest {
                jsonrpc: "2.0",
                id: i as u64,
                method: "eth_getTransactionCount",
                params: vec![
                    Value::String(format!("{addr}")),
                    Value::String("latest".into()),
                ],
            })
            .collect();

        let responses = self.send_batch(requests).await?;
        let mut results = vec![0u64; addresses.len()];
        for resp in responses {
            let idx = resp.id as usize;
            if idx >= addresses.len() {
                continue;
            }
            if let Some(err) = resp.error {
                eyre::bail!("eth_getTransactionCount error for {}: {err}", addresses[idx]);
            }
            if let Value::String(hex) = &resp.result {
                results[idx] = u64::from_str_radix(hex.trim_start_matches("0x"), 16)
                    .wrap_err("invalid nonce hex")?;
            }
        }
        Ok(results)
    }

    /// Batch eth_call. Takes (to_address, calldata) pairs, returns raw hex-decoded response bytes.
    pub async fn batch_eth_calls(
        &self,
        calls: &[(Address, Vec<u8>)],
    ) -> Result<Vec<Vec<u8>>> {
        let requests: Vec<_> = calls
            .iter()
            .enumerate()
            .map(|(i, (to, data))| {
                let call_obj = serde_json::json!({
                    "to": format!("{to}"),
                    "data": format!("0x{}", alloy::hex::encode(data)),
                });
                JsonRpcRequest {
                    jsonrpc: "2.0",
                    id: i as u64,
                    method: "eth_call",
                    params: vec![call_obj, Value::String("latest".into())],
                }
            })
            .collect();

        let responses = self.send_batch(requests).await?;
        let mut results = vec![Vec::new(); calls.len()];
        for resp in responses {
            let idx = resp.id as usize;
            if idx >= calls.len() {
                continue;
            }
            if let Some(err) = resp.error {
                eyre::bail!("eth_call error at index {idx}: {err}");
            }
            if let Value::String(hex) = &resp.result {
                results[idx] = alloy::hex::decode(hex.trim_start_matches("0x"))
                    .wrap_err("invalid eth_call response hex")?;
            }
        }
        Ok(results)
    }
}

fn parse_u256_responses(responses: Vec<JsonRpcResponse>, expected: usize) -> Result<Vec<U256>> {
    let mut results = vec![U256::ZERO; expected];
    for resp in responses {
        let idx = resp.id as usize;
        if idx >= expected {
            continue;
        }
        if let Some(err) = resp.error {
            eyre::bail!("RPC error at index {idx}: {err}");
        }
        if let Value::String(hex) = &resp.result {
            results[idx] = U256::from_str_radix(hex.trim_start_matches("0x"), 16)
                .wrap_err("invalid U256 hex")?;
        }
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_rpc_request_serializes() {
        let req = JsonRpcRequest {
            jsonrpc: "2.0",
            id: 1,
            method: "eth_getTransactionReceipt",
            params: vec![Value::String(
                "0x0000000000000000000000000000000000000000000000000000000000000001".into(),
            )],
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["method"], "eth_getTransactionReceipt");
        assert_eq!(json["id"], 1);
    }

    #[test]
    fn json_rpc_response_parses_null_result() {
        let raw = r#"{"jsonrpc":"2.0","id":0,"result":null}"#;
        let resp: JsonRpcResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(resp.id, 0);
        assert_eq!(resp.result, Value::Null);
        assert!(resp.error.is_none());
    }

    #[test]
    fn json_rpc_response_parses_error() {
        let raw = r#"{"jsonrpc":"2.0","id":0,"error":{"code":-32000,"message":"nonce too low"}}"#;
        let resp: JsonRpcResponse = serde_json::from_str(raw).unwrap();
        assert!(resp.error.is_some());
    }
}
