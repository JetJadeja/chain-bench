// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {ERC20} from "solady/tokens/ERC20.sol";
import {OwnableRoles} from "solady/auth/OwnableRoles.sol";
import {SafeTransferLib} from "solady/utils/SafeTransferLib.sol";

contract Vault is OwnableRoles {
    uint256 public constant OPERATOR_ROLE = _ROLE_0;

    ERC20 public immutable token;
    mapping(address => uint256) private _balances;

    event Match(address indexed userA, address indexed userB, uint256 amountA, uint256 amountB);
    event Withdraw(address indexed user, uint256 amount);

    error InsufficientBalance();

    constructor(address token_) {
        _initializeOwner(msg.sender);
        token = ERC20(token_);
    }

    function matchOrders(address a, address b, uint256 amountA, uint256 amountB)
        external
        onlyRoles(OPERATOR_ROLE)
    {
        SafeTransferLib.safeTransferFrom(address(token), a, address(this), amountA);
        SafeTransferLib.safeTransferFrom(address(token), b, address(this), amountB);

        _balances[a] += amountB;
        _balances[b] += amountA;

        emit Match(a, b, amountA, amountB);
    }

    function withdraw(uint256 amount) external {
        if (_balances[msg.sender] < amount) revert InsufficientBalance();

        _balances[msg.sender] -= amount;
        SafeTransferLib.safeTransfer(address(token), msg.sender, amount);

        emit Withdraw(msg.sender, amount);
    }

    function balanceOf(address user) external view returns (uint256) {
        return _balances[user];
    }
}
