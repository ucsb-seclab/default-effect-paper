"""
Uniswap v2 exchange pricer
"""

import decimal
import web3
import web3.types
import web3.contract
import typing
import logging

l = logging.getLogger(__name__)

import constants

RESERVES_SLOT = '0x0000000000000000000000000000000000000000000000000000000000000008'


class UniswapV2Pricer:
    address: str
    token0: str
    token1: str
    known_token0_bal: int
    known_token1_bal: int
    client: web3.Web3

    def __init__(self, w3: web3.Web3, address: str, token0: str, token1: str) -> None:
        self.address = web3.Web3.to_checksum_address(address)
        self.token0 = token0
        self.token1 = token1
        self.client = w3
        self.known_token0_bal = None
        self.known_token1_bal = None

    #
    # Getters
    #

    def get_tokens(self, _) -> typing.Set[str]:
        return set([self.token0, self.token1])

    def get_spot_price(self, token_in: str, token_out: str, block_identifier: int) -> decimal.Decimal:
        assert token_in in [self.token0, self.token1]
        assert token_out in [self.token0, self.token1]

        bal0, bal1 = self.get_balances(block_identifier)
        if bal0 == 0 or bal1 == 0:
            return 0
        
        if token_in == self.token0 and token_out == self.token1:
            numerator = bal1 * 1000
            denominator = bal0 * 997
        elif token_in == self.token1 and token_out == self.token0:
            numerator = bal0 * 1000
            denominator = bal1 * 997

        return decimal.Decimal(numerator) / decimal.Decimal(denominator)

    def get_balances(self, block_identifier) -> typing.Tuple[int, int]:
        if self.known_token0_bal is None or self.known_token1_bal is None:
            breserves = self.client.eth.get_storage_at(self.address, RESERVES_SLOT, block_identifier)

            reserve1 = int.from_bytes(breserves[4:18], byteorder='big', signed=False)
            reserve0 = int.from_bytes(breserves[18:32], byteorder='big', signed=False)

            self.known_token0_bal = reserve0
            self.known_token1_bal = reserve1
        return (self.known_token0_bal, self.known_token1_bal)

    def token_out_for_exact_in(
            self,
            token_in: str,
            token_out: str,
            amount_in: int,
            block_identifier: int,
            **_
    ) -> typing.Tuple[int, float]:
        if token_in == self.token0 and token_out == self.token1:
            amt_out = self.exact_token0_to_token1(amount_in, block_identifier)
            new_reserve_in = self.known_token0_bal + amount_in
            new_reserve_out = self.known_token1_bal - amt_out
        elif token_in == self.token1 and token_out == self.token0:
            amt_out = self.exact_token1_to_token0(amount_in, block_identifier)
            new_reserve_in = self.known_token1_bal + amount_in
            new_reserve_out = self.known_token0_bal - amt_out
        else:
            raise NotImplementedError()

        if new_reserve_out == 0:
            spot = 0
        else:
            # how much out do we get for 1 unit in?
            # https://github.com/Uniswap/v2-periphery/blob/master/contracts/libraries/UniswapV2Library.sol#L43
            amount_in_with_fee = 1 * 997
            numerator = amount_in_with_fee * new_reserve_out
            denominator = new_reserve_in * 1_000 + amount_in_with_fee
            spot = numerator / denominator

        return (amt_out, spot)

    def exact_token0_to_token1(self, token0_amount, block_identifier: int) -> int:
        # based off https://github.com/Uniswap/v2-periphery/blob/master/contracts/libraries/UniswapV2Library.sol#L43
        bal0, bal1 = self.get_balances(block_identifier)
        if bal0 == 0 or bal1 == 0:
            return 0  # no amount can be taken out
        amt_in_with_fee = token0_amount * 997
        numerator = amt_in_with_fee * bal1
        denominator = bal0 * 1000 + amt_in_with_fee
        ret = numerator // denominator
        return ret

    def exact_token1_to_token0(self, token1_amount, block_identifier: int) -> int:
        # based off https://github.com/Uniswap/v2-periphery/blob/master/contracts/libraries/UniswapV2Library.sol#L43
        bal0, bal1 = self.get_balances(block_identifier)
        if bal0 == 0 or bal1 == 0:
            return 0  # no amount can be taken out
        amt_in_with_fee = token1_amount * 997
        numerator = amt_in_with_fee * bal0
        denominator = bal1 * 1000 + amt_in_with_fee
        return numerator // denominator

    def token1_in_for_exact_token0_out(self, token0_out_amount, block_identifier: int) -> int:
        bal0, bal1 = self.get_balances(block_identifier)
        if bal0 == 0 or bal1 == 0:
            return 0
        numerator = bal1 * token0_out_amount * 1000
        denominator = (bal0 - token0_out_amount) * 997
        denominator = max(denominator, 1)
        return numerator // denominator + 1

    def token0_in_for_exact_token1_out(self, token1_out_amount, block_identifier: int) -> int:
        bal0, bal1 = self.get_balances(block_identifier)
        if bal0 == 0 or bal1 == 0:
            return 0
        numerator = bal0 * token1_out_amount * 1000
        denominator = (bal1 - token1_out_amount) * 997
        denominator = max(denominator, 1)
        return numerator // denominator + 1

    def observe_block(self, receipts: typing.List[web3.types.LogReceipt], **_):
        """
        Observe the logs emitted in a block and update internal state appropriately.
        NOTE: receipts _must_ be in sorted order of increasing log index
        """
        if len(receipts) == 0:
            return

        block_num = receipts[0]['blockNumber']
        # all we care about are Syncs
        for log in reversed(receipts):
            if log['address'] == self.address and len(log['topics']) > 0 and bytes(log['topics'][0]).endswith(utils.constants.UNISWAP_V2_SYNC_TOPIC()):
                sync = constants._univ2().events.Sync().processLog(log)
                bal0 = sync['args']['reserve0']
                assert bal0 >= 0
                bal1 = sync['args']['reserve1']
                assert bal1 >= 0
                self.known_token0_bal = bal0
                self.known_token1_bal = bal1

    def __str__(self) -> str:
        return f'<UniswapV2Pricer {self.address} token0={self.token0} token1={self.token1}>'
