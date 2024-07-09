import web3
import web3.contract
import json
import functools
import datetime

from eth_utils import event_abi_to_log_topic, function_abi_to_4byte_selector, keccak

UNISWAP_V2_ROUTER_2 = '0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D'
UNISWAP_V3_ROUTER_2 = '0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45'
UNISWAP_UNIVERSAL_ROUTER = '0xEf1c6E67703c7BD7107eed8303Fbe6EC2554BF6B'
UNISWAP_UNIVERSAL_ROUTER_NEW = '0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD'
SUSHISWAP_ROUTER = '0xd9e1cE17f2641f24aE83637ab66a2cca9C378B9F'
BALANCER_V2_VAULT = '0xBA12222222228d8Ba445958a75a0704d566BF2C8'
UNISWAP_V3_FEES = [100, 500, 3_000, 10_000]


WETH_ADDRESS = '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'

UNISWAP_UNIVERSAL_ERROR_TOO_LITTLE_RECEIVED = '0x' + bytes(keccak(b'V3TooLittleReceived()')[:4]).hex()
UNISWAP_UNIVERSAL_ERROR_TOO_MUCH_REQUESTED = '0x' + bytes(keccak(b'V3TooMuchRequested()')[:4]).hex()

START_BLOCK = 16730072
END_BLOCK = 16950602

START_DATETIME = datetime.datetime(
    year=2023,
    month=3,
    day=1,
    hour=0,
    minute=0,
    second=0,
)
END_DATETIME = datetime.datetime(
    year=2023,
    month=4,
    day=1,
    hour=0,
    minute=0,
    second=0,
)

END_TRANSITION_DATETIME = datetime.datetime(
    year=2023,
    month=3,
    day=20,
    hour=0,
    minute=0,
    second=0,
)

UNISWAP_V3_FEES_BIPS = [100, 500, 3_000, 10_000]

@functools.cache
def _univ2() -> web3.contract.Contract:
    with open('abis/IUniswapV2Pair.json') as fin:
        abi = json.load(fin)
        return web3.Web3().eth.contract(address=b'\x00'*20, abi=abi['abi'])

@functools.cache
def _univ3() -> web3.contract.Contract:
    with open('abis/IUniswapV3Pool.json') as fin:
        abi = json.load(fin)
    return web3.Web3().eth.contract(address=b'\x00'*20, abi=abi['abi'])

@functools.cache
def UNISWAP_V2_SWAP_TOPIC() -> bytes:
    return event_abi_to_log_topic(_univ2().events.Swap().abi)

@functools.cache
def UNISWAP_V2_SYNC_TOPIC() -> bytes:
    return event_abi_to_log_topic(_univ2().events.Sync().abi)

@functools.cache
def UNISWAP_V3_SWAP_TOPIC() -> bytes:
    return event_abi_to_log_topic(_univ3().events.Swap().abi)

@functools.cache
def UNISWAP_V3_BURN_TOPIC() -> bytes:
    return event_abi_to_log_topic(_univ3().events.Burn().abi)

@functools.cache
def UNISWAP_V3_MINT_TOPIC() -> bytes:
    return event_abi_to_log_topic(_univ3().events.Mint().abi)

@functools.cache
def UNISWAP_V3_TOKEN0_SEL() -> bytes:
    return function_abi_to_4byte_selector(_univ3().functions.token0().abi)

@functools.cache
def UNISWAP_V3_TOKEN1_SEL() -> bytes:
    return function_abi_to_4byte_selector(_univ3().functions.token1().abi)

@functools.cache
def UNISWAP_V3_FEE_SEL() -> bytes:
    return function_abi_to_4byte_selector(_univ3().functions.fee().abi)

@functools.cache
def UNISWAP_V2_SWAP_SEL() -> bytes:
    for item in _univ2().abi:
        if item['type'] == 'function' and item['name'] == 'swap':
            return function_abi_to_4byte_selector(
                item
            )

@functools.cache
def UNISWAP_V3_SWAP_SEL() -> bytes:
    for item in _univ3().abi:
        if item['type'] == 'function' and item['name'] == 'swap':
            return function_abi_to_4byte_selector(
                item
            )

@functools.cache
def ERC20_TRANSFER_TOPIC() -> bytes:
    return keccak(b'Transfer(address,address,uint256)')
