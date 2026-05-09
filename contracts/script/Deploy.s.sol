// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Script, console} from "forge-std/Script.sol";
import {MockToken} from "../src/MockToken.sol";
import {Vault} from "../src/Vault.sol";

contract Deploy is Script {
    function run() external {
        uint256 operatorKey = vm.envUint("OPERATOR_KEY");
        address operator = vm.addr(operatorKey);

        vm.startBroadcast();

        MockToken token = new MockToken("Mock Token", "MOCK", 18);
        Vault vault = new Vault(address(token));
        vault.grantRoles(operator, vault.OPERATOR_ROLE());

        vm.stopBroadcast();

        console.log("token", address(token));
        console.log("vault", address(vault));
        console.log("operator", operator);
    }
}
