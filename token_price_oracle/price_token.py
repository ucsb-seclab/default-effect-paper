import datetime
import decimal
import statistics
import typing
import web3
from token_price_oracle.constants import STABLECOINS, WETH_ADDRESS

import token_price_oracle.uniswap_v2_pricer
import token_price_oracle.uniswap_v3_pricer
import token_price_oracle.sushiswap_v2_pricer
import token_price_oracle.chainlink_eth_pricer
import token_price_oracle.exceptions
from token_price_oracle.utils import get_block_timestamp, weighted_median

def price_token_dollars(
        w3: web3.Web3,
        token_address: str,
        block_identifier: typing.Union[str, int],
        liveness_threshold_seconds: int = 60 * 60 * 24 * 7
    ) -> decimal.Decimal:
    """
    Get the price of a token in terms of dollars.

    Computes the price by taking the weighted median price of
    (1) all DeX exchanges that pair this token with a dollar stablecoin, and
    (2) all DeX exchanges that pair this token with WETH, which we convert to dollars using the
        Chainlink price oracle.
    where the weights are determined by available liquidity.

    NOTE: The price is accurate only if the number of decimals in the token is 18.
          To adjust, divide the price by power(10, 18 - decimals)

    Arguments:
        w3: web3 connection
        token_address: the contract address of the token to price
        block_identifier: the block identifier for which we will get the price
        liveness_threshold_seconds: how many seconds of exchange inactivity may elapse before we disregard the exchange's price, default 7 days
    """
    target_timestamp = get_block_timestamp(w3, block_identifier)
    prices = []

    for stablecoin, decimals in STABLECOINS:
        this_token_prices = []
        try:
            report = token_price_oracle.uniswap_v2_pricer.price(w3, token_address, stablecoin, block_identifier)
            seconds_elapsed = (target_timestamp - report.timestamp).seconds
            if seconds_elapsed <= liveness_threshold_seconds:
                this_token_prices.append((report.liquidity, report.price))
        except (token_price_oracle.exceptions.ExchangeNotFound, token_price_oracle.exceptions.NoPriceData):
            pass

        try:
            report = token_price_oracle.sushiswap_v2_pricer.price(w3, token_address, stablecoin, block_identifier)
            seconds_elapsed = (target_timestamp - report.timestamp).seconds
            if seconds_elapsed <= liveness_threshold_seconds:
                this_token_prices.append((report.liquidity, report.price))
        except (token_price_oracle.exceptions.ExchangeNotFound, token_price_oracle.exceptions.NoPriceData):
            pass

        for fee in [100, 500, 3_000, 10_000]:
            try:
                report = token_price_oracle.uniswap_v3_pricer.price(w3, token_address, stablecoin, fee, block_identifier)
                seconds_elapsed = (target_timestamp - report.timestamp).seconds
                if seconds_elapsed <= liveness_threshold_seconds:
                    this_token_prices.append((report.liquidity, report.price))
            except (token_price_oracle.exceptions.ExchangeNotFound, token_price_oracle.exceptions.NoPriceData):
                pass
        
        for liquidity, p in this_token_prices:
            adjustment_decimals = 18 - decimals
            this_price_dollars = p * (10 ** adjustment_decimals)
            this_liquidity = liquidity * (10 ** adjustment_decimals)
            prices.append((this_liquidity, this_price_dollars))

    dollars_per_eth = token_price_oracle.chainlink_eth_pricer.price_eth_dollars(w3, block_identifier)

    try:
        report = token_price_oracle.uniswap_v2_pricer.price(w3, token_address, WETH_ADDRESS, block_identifier)
        seconds_elapsed = (target_timestamp - report.timestamp).seconds
        if seconds_elapsed <= liveness_threshold_seconds:
            prices.append((int(report.liquidity * dollars_per_eth), report.price * dollars_per_eth))
    except (token_price_oracle.exceptions.ExchangeNotFound, token_price_oracle.exceptions.NoPriceData):
        pass

    try:
        report = token_price_oracle.sushiswap_v2_pricer.price(w3, token_address, WETH_ADDRESS, block_identifier)
        seconds_elapsed = (target_timestamp - report.timestamp).seconds
        if seconds_elapsed <= liveness_threshold_seconds:
            prices.append((int(report.liquidity * dollars_per_eth), report.price * dollars_per_eth))
    except (token_price_oracle.exceptions.ExchangeNotFound, token_price_oracle.exceptions.NoPriceData):
        pass

    for fee in [100, 500, 3_000, 10_000]:
        try:
            report = token_price_oracle.uniswap_v3_pricer.price(w3, token_address, WETH_ADDRESS, fee, block_identifier)
            seconds_elapsed = (target_timestamp - report.timestamp).seconds
            if seconds_elapsed <= liveness_threshold_seconds:
                prices.append((int(report.liquidity * dollars_per_eth), report.price * dollars_per_eth))
        except (token_price_oracle.exceptions.ExchangeNotFound, token_price_oracle.exceptions.NoPriceData):
            pass

    if len(prices) == 0:
        raise token_price_oracle.exceptions.NoPriceData(f'Could not find any fresh price feed for token {token_address}')

    return weighted_median(prices)


def price_token_eth(
        w3: web3.Web3,
        token_address: str,
        block_identifier: typing.Union[str, int],
        liveness_threshold_seconds: int = 60 * 60 * 24 * 7
    ) -> decimal.Decimal:
    """
    Get the price of a token in terms of ETH (returned in units of wei).

    Computes the price by taking the median price of all DeX exchanges that pair
    this token with ETH.

    NOTE: The price is accurate only if the number of decimals in the token is 18.
          To adjust, divide the price by power(10, 18 - decimals)

    Arguments:
        w3: web3 connection
        token_address: the contract address of the token to price
        block_identifier: the block identifier for which we will get the price
        liveness_threshold_seconds: how many seconds of exchange inactivity may elapse before we disregard the exchange's price, default 7 days
    """
    target_timestamp = get_block_timestamp(w3, block_identifier)
    prices = []

    try:
        report = token_price_oracle.uniswap_v2_pricer.price(w3, token_address, WETH_ADDRESS, block_identifier)
        seconds_elapsed = (target_timestamp - report.timestamp).seconds
        if seconds_elapsed <= liveness_threshold_seconds:
            prices.append((report.liquidity, report.price))
    except (token_price_oracle.exceptions.ExchangeNotFound, token_price_oracle.exceptions.NoPriceData):
        pass

    try:
        report = token_price_oracle.sushiswap_v2_pricer.price(w3, token_address, WETH_ADDRESS, block_identifier)
        seconds_elapsed = (target_timestamp - report.timestamp).seconds
        if seconds_elapsed <= liveness_threshold_seconds:
            prices.append((report.liquidity, report.price))
    except (token_price_oracle.exceptions.ExchangeNotFound, token_price_oracle.exceptions.NoPriceData):
        pass

    for fee in [100, 500, 3_000, 10_000]:
        try:
            report = token_price_oracle.uniswap_v3_pricer.price(w3, token_address, WETH_ADDRESS, fee, block_identifier)
            seconds_elapsed = (target_timestamp - report.timestamp).seconds
            if seconds_elapsed <= liveness_threshold_seconds:
                prices.append((report.liquidity, report.price))
        except (token_price_oracle.exceptions.ExchangeNotFound, token_price_oracle.exceptions.NoPriceData):
            pass

    if len(prices) == 0:
        raise token_price_oracle.exceptions.NoPriceData(f'Could not find any fresh price feed for token {token_address}')

    return weighted_median(prices)
