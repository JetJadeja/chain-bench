use alloy::sol;

sol! {
    #[sol(rpc)]
    contract MockToken {
        function mint(address to, uint256 amount) external;
        function balanceOf(address account) external view returns (uint256);
        function approve(address spender, uint256 amount) external returns (bool);
        function allowance(address owner, address spender) external view returns (uint256);
    }

    #[sol(rpc)]
    contract Vault {
        function matchOrders(address a, address b, uint256 amountA, uint256 amountB) external;
        function balanceOf(address user) external view returns (uint256);
        function grantRoles(address user, uint256 roles) external;
        function hasAnyRole(address user, uint256 roles) external view returns (bool);
        function OPERATOR_ROLE() external view returns (uint256);
    }
}
