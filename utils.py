import typing
import web3
import dotenv
import os
import functools

from web3.middleware import geth_poa_middleware

from eth_utils import keccak
import constants


def connect_web3(host: str = None) -> web3.Web3:
    if host is None:
        host = os.environ.get("WEB3_HOST", "ws://localhost:8546")
    return web3.Web3(
        web3.WebsocketProvider(
            host, websocket_kwargs={"timeout": 5 * 60, "max_size": 1024 * 1024 * 1024}
        )
    )


def connect_web3_goerli() -> web3.Web3:
    host = os.environ.get("WEB3_HOST_GOERLI")
    w3 = web3.Web3(
        web3.HTTPProvider(
            host,
        )
    )
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    return w3


@functools.lru_cache(maxsize=10_000)
def uniswap_v2_pool_address(token0, token1) -> bytes:
    """
    Compute the uniswap v2 pool address for a given pair of tokens
    """
    if isinstance(token0, str):
        if token0.startswith("0x"):
            token0 = token0[2:]
        token0 = bytes.fromhex(token0)

    if isinstance(token1, str):
        if token1.startswith("0x"):
            token1 = token1[2:]
        token1 = bytes.fromhex(token1)

    token0, token1 = sorted([token0, token1])

    salt = keccak(token0 + token1)
    h = keccak(
        b"\xff"
        + bytes.fromhex("0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"[2:])
        + salt
        + bytes.fromhex(
            "96e8ac4277198ff8b6f785478aa9a39f403cb768dd02cbee326c3e7da348845f"
        )
    )
    return h[-20:]


@functools.lru_cache(maxsize=10_000)
def uniswap_v3_pool_address(token0, token1, fee) -> bytes:
    """
    Compute the uniswap v3 pool address for a given pair of tokens
    """
    if isinstance(token0, str):
        if token0.startswith("0x"):
            token0 = token0[2:]
        token0 = bytes.fromhex(token0)

    if isinstance(token1, str):
        if token1.startswith("0x"):
            token1 = token1[2:]
        token1 = bytes.fromhex(token1)

    token0, token1 = sorted([token0, token1])

    token0 = token0.rjust(32, b"\x00")
    token1 = token1.rjust(32, b"\x00")
    fee = int.to_bytes(fee, length=32, byteorder="big", signed=False)

    salt = keccak(token0 + token1 + fee)
    h = keccak(
        b"\xff"
        + bytes.fromhex("0x1F98431c8aD98523631AE4a59f267346ea31F984"[2:])
        + salt
        + bytes.fromhex(
            "e34f199b19b2b4f47f68442619d555527d244f78a3297ea89325f843f87b8b54"
        )
    )
    return h[-20:]


def sushiswap_pool_address(token0, token1) -> bytes:
    """
    Compute the sushiswap pool address for a given pair of tokens
    """
    if isinstance(token0, str):
        if token0.startswith("0x"):
            token0 = token0[2:]
        token0 = bytes.fromhex(token0)

    if isinstance(token1, str):
        if token1.startswith("0x"):
            token1 = token1[2:]
        token1 = bytes.fromhex(token1)

    token0, token1 = sorted([token0, token1])

    salt = keccak(token0 + token1)
    h = keccak(
        b"\xff"
        + bytes.fromhex("0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac"[2:])
        + salt
        + bytes.fromhex(
            "e18a34eb0e04b04f7a0ac29a6e80748dca96319b42c54d679cb821dca90c6303"
        )
    )
    return h[-20:]


@functools.lru_cache(maxsize=1_000)
def infer_uniswap_v3_pool_fee(
    address: str, token0: str, token1: str
) -> typing.Optional[int]:
    # infer the fee
    for fee in [100, 500, 3_000, 10_000]:
        if address.endswith(uniswap_v3_pool_address(token0, token1, fee).hex()):
            # found it
            return fee

    return None


@functools.lru_cache(maxsize=10_000)
def is_uniswap_v2(token0, token1, address) -> bool:
    if isinstance(token0, str):
        if token0.startswith("0x"):
            token0 = token0[2:]
        token0 = bytes.fromhex(token0)

    if isinstance(token1, str):
        if token1.startswith("0x"):
            token1 = token1[2:]
        token1 = bytes.fromhex(token1)

    if isinstance(address, str):
        if address.startswith("0x"):
            address = address[2:]
        address = bytes.fromhex(address)

    return address == uniswap_v2_pool_address(token0, token1)


@functools.lru_cache(maxsize=10_000)
def is_uniswap_v3(token0, token1, address) -> bool:
    return get_uniswap_v3_fee(token0, token1, address) is not None


@functools.lru_cache(maxsize=10_000)
def get_uniswap_v3_fee(token0, token1, address) -> typing.Optional[int]:
    """
    Returns the fee if the address is a uniswap v3 pool, otherwise None
    """
    if isinstance(token0, str):
        if token0.startswith("0x"):
            token0 = token0[2:]
        token0 = bytes.fromhex(token0)

    if isinstance(token1, str):
        if token1.startswith("0x"):
            token1 = token1[2:]
        token1 = bytes.fromhex(token1)

    if isinstance(address, str):
        if address.startswith("0x"):
            address = address[2:]
        address = bytes.fromhex(address)

    for fee in constants.UNISWAP_V3_FEES_BIPS:
        if address == uniswap_v3_pool_address(token0, token1, fee):
            return fee

    return None
