import typing
import web3
import eth_abi.packed
import decimal
import statistics
import datetime

from token_price_oracle.constants import STABLECOINS, WETH_ADDRESS
from token_price_oracle.structs import PriceReport
from token_price_oracle.utils import get_block_timestamp, weighted_median

from .exceptions import ExchangeNotFound, NoPriceData

RESERVES_SLOT = '0x0000000000000000000000000000000000000000000000000000000000000008'

_eth_price_cache = {}
def price_eth_dollars(w3: web3.Web3, block_identifier: typing.Any, liveness_threshold_seconds = 60 * 60 * 24 * 7) -> decimal.Decimal:
    if block_identifier not in _eth_price_cache:
        target_timestamp = get_block_timestamp(w3, block_identifier)
        prices = []
        for token, decimals in STABLECOINS:
            try:
                report = price(w3, WETH_ADDRESS, token, block_identifier)

                seconds_elapsed = (target_timestamp - report.timestamp).seconds
                if seconds_elapsed > liveness_threshold_seconds:
                    continue

                adjustment_decimals = 18 - decimals
                this_price_dollars = report.price * (10 ** adjustment_decimals)
                prices.append((report.liquidity, this_price_dollars))
            except ExchangeNotFound:
                pass

        _eth_price_cache[block_identifier] = weighted_median(prices)
    return _eth_price_cache[block_identifier]


def price(w3: web3.Web3, from_token: str, to_token: str, block_identifier: typing.Any) -> PriceReport:
    """
    Use Uniswap v2 to find the price of `from_token` in terms of `to_token`.
    """
    #
    # Compute pair address
    bfrom_token = bytes.fromhex(from_token[2:])
    bto_token = bytes.fromhex(to_token[2:])

    if bfrom_token < bto_token:
        zero_to_one = True
        token0 = bfrom_token
        token1 = bto_token
    else:
        zero_to_one = False
        token0 = bto_token
        token1 = bfrom_token

    hexadem_ ='0xe18a34eb0e04b04f7a0ac29a6e80748dca96319b42c54d679cb821dca90c6303'
    factory = '0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac'
    abiEncoded_1 = eth_abi.packed.encode_packed(
        ['address', 'address'],
        (
            web3.Web3.to_checksum_address(token0),
            web3.Web3.to_checksum_address(token1),
        )
    )
    salt_ = w3.solidity_keccak(['bytes'], ['0x' +abiEncoded_1.hex()])
    abiEncoded_2 = eth_abi.packed.encode_packed(
        [ 'address', 'bytes32'],
        (
            factory,
            salt_,
        ),
    )
    
    pair_address = w3.to_checksum_address(w3.solidity_keccak(['bytes','bytes'], ['0xff' + abiEncoded_2.hex(), hexadem_])[12:])

    #
    # query balances
    breserves = w3.eth.get_storage_at(pair_address, RESERVES_SLOT, block_identifier=block_identifier)

    if len(breserves.lstrip(b'\x00')) == 0:
        raise ExchangeNotFound(f'Could not find exchange {pair_address} for pair {token0} {token1}')

    block_ts = int.from_bytes(breserves[0:4], byteorder='big', signed=False)
    reserve1 = int.from_bytes(breserves[4:18], byteorder='big', signed=False)
    reserve0 = int.from_bytes(breserves[18:32], byteorder='big', signed=False)

    ts = datetime.datetime.fromtimestamp(block_ts, tz=datetime.timezone.utc)

    if zero_to_one:
        if reserve0 == 0:
            raise NoPriceData(f'No price data for pair {token0.hex()} {token1.hex()}')
        return PriceReport(
            price     = decimal.Decimal(reserve1) / decimal.Decimal(reserve0),
            liquidity = reserve0 * reserve1,
            timestamp = ts,
        )
    else:
        if reserve1 == 0:
            raise NoPriceData(f'No price data for pair {token0.hex()} {token1.hex()}')
        return PriceReport(
            price     = decimal.Decimal(reserve0) / decimal.Decimal(reserve1),
            liquidity = reserve0 * reserve1,
            timestamp = ts,
        )
