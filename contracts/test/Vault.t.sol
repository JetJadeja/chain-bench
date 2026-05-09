// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Test} from "forge-std/Test.sol";
import {MockToken} from "../src/MockToken.sol";
import {Vault} from "../src/Vault.sol";

contract VaultTest is Test {
    MockToken token;
    Vault vault;

    address owner = address(this);
    address operator = makeAddr("operator");
    address alice = makeAddr("alice");
    address bob = makeAddr("bob");

    function setUp() public {
        token = new MockToken("Mock", "MCK", 18);
        vault = new Vault(address(token));
        vault.grantRoles(operator, vault.OPERATOR_ROLE());

        token.mint(alice, 1000e18);
        token.mint(bob, 1000e18);

        vm.prank(alice);
        token.approve(address(vault), type(uint256).max);
        vm.prank(bob);
        token.approve(address(vault), type(uint256).max);
    }

    function test_matchOrders() public {
        vm.prank(operator);
        vault.matchOrders(alice, bob, 100e18, 200e18);

        assertEq(vault.balanceOf(alice), 200e18);
        assertEq(vault.balanceOf(bob), 100e18);
        assertEq(token.balanceOf(address(vault)), 300e18);
    }

    function test_matchOrders_onlyOperator() public {
        vm.prank(alice);
        vm.expectRevert();
        vault.matchOrders(alice, bob, 100e18, 200e18);
    }

    function test_matchOrders_insufficientBalance() public {
        vm.prank(operator);
        vm.expectRevert();
        vault.matchOrders(alice, bob, 2000e18, 100e18);
    }

    function test_withdraw() public {
        vm.prank(operator);
        vault.matchOrders(alice, bob, 100e18, 200e18);

        uint256 balanceBefore = token.balanceOf(alice);
        vm.prank(alice);
        vault.withdraw(200e18);

        assertEq(vault.balanceOf(alice), 0);
        assertEq(token.balanceOf(alice), balanceBefore + 200e18);
    }

    function test_withdraw_insufficientBalance() public {
        vm.prank(alice);
        vm.expectRevert(Vault.InsufficientBalance.selector);
        vault.withdraw(1e18);
    }

    function test_withdraw_partial() public {
        vm.prank(operator);
        vault.matchOrders(alice, bob, 100e18, 200e18);

        vm.prank(alice);
        vault.withdraw(50e18);
        assertEq(vault.balanceOf(alice), 150e18);
    }
}
