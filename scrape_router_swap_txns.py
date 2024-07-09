import psycopg
import logging
import datetime
import typing
import itertools
import functools
import collections
import ujson
import time
import web3
import web3.types
import web3.exceptions
import web3.contract
import eth_abi
import eth_abi.exceptions
import dotenv
import multiprocessing


from db import connect_db
from utils import (
    connect_web3,
    uniswap_v2_pool_address,
    uniswap_v3_pool_address,
    sushiswap_pool_address,
)

import constants


N_WORKERS = 23

l = logging.getLogger(__name__)

def _stub_process_txn(txn):
    w3 = get_local_w3()
    return process_uni_universal_router(w3, txn)

def main():
    print(f'Scraping swap transactions')
    dotenv.load_dotenv()

    db = connect_db()
    curr = db.cursor()

    #
    # Iterate over block range
    txns = []

    with multiprocessing.Pool(N_WORKERS) as pool:
        n_blocks = constants.END_BLOCK - constants.START_BLOCK + 1
        n_processed = 0
        times = collections.deque(maxlen=10)
        for block_report in pool.imap_unordered(scrape_block, range(constants.START_BLOCK, constants.END_BLOCK + 1), chunksize=100):
            txns.extend(block_report)
            n_processed += 1
            if len(times) == 0:
                times.append((time.time(), n_processed))
            elif time.time() - times[-1][0] > 10:
                times.append((time.time(), n_processed))
                old_time, old_n_processed = times[0]
                speed = (n_processed - old_n_processed) / (time.time() - old_time)
                remaining = n_blocks - n_processed
                eta = remaining / speed
                eta_td = datetime.timedelta(seconds=eta)
                print(f'Processed {n_processed:,} blocks ({n_processed / n_blocks * 100:.2f}%), {speed:.2f} blocks/sec -- ETA {eta_td}')

    # collect set of already-inserted transactions so we do not duplicate
    curr.execute(
        '''
        SELECT block_number, txn_idx
        FROM uniswap_v2_router_2_txns
        UNION
        SELECT block_number, txn_idx
        FROM uniswap_v3_router_2_txns
        UNION
        SELECT block_number, txn_idx
        FROM uniswap_universal_router_txns
        '''
    )
    txns_already_stored = set(curr.fetchall())

    l.info('Storing...')

    # avoid inserting duplicates (should have none...?)
    seen_txn_hashes = set()

    with curr.copy(
        '''
            COPY uniswap_v2_router_2_txns (
                txn_hash,
                block_number,
                txn_idx,
                failed,
                from_address,
                to_address,
                exact_input,
                input_token,
                output_token,
                amount_input,
                amount_output,
                amount_bound,
                supports_fee_on_transfer,
                pool
            ) FROM STDIN
        ''') as cpy:

        for item in txns:
            if isinstance(item, UniswapV2Router2Swap):
                if (item.block_number, item.transaction_index) in txns_already_stored:
                    continue
                if item.txn_hash in seen_txn_hashes:
                    print('duplicate txn hash')
                    continue
                seen_txn_hashes.add(item.txn_hash)
                cpy.write_row((
                    item.txn_hash,
                    item.block_number,
                    item.transaction_index,
                    item.failed,
                    bytes.fromhex(item.from_address[2:]),
                    bytes.fromhex(item.to_address[2:]),
                    item.exact_input,
                    bytes.fromhex(item.input_token[2:]),
                    bytes.fromhex(item.output_token[2:]),
                    item.amount_input,
                    item.amount_output,
                    item.amount_bound,
                    item.supports_fee_on_transfer,
                    bytes.fromhex(item.pool[2:])
                ))

    with curr.copy(
        '''
            COPY uniswap_v3_router_2_txns (
                txn_hash,
                block_number,
                txn_idx,
                failed,
                from_address,
                to_address,
                exact_input,
                input_token,
                output_token,
                amount_input,
                amount_output,
                amount_bound,
                supports_fee_on_transfer,
                pool,
                pool_is_v2
            ) FROM STDIN
        ''') as cpy:

        for item in txns:
            if isinstance(item, UniswapV3Router2Swap):
                if (item.block_number, item.transaction_index) in txns_already_stored:
                    continue
                if item.txn_hash in seen_txn_hashes:
                    print('duplicate txn hash')
                    continue
                seen_txn_hashes.add(item.txn_hash)
                cpy.write_row((
                    item.txn_hash,
                    item.block_number,
                    item.transaction_index,
                    item.failed,
                    bytes.fromhex(item.from_address[2:]),
                    bytes.fromhex(item.to_address[2:]),
                    item.exact_input,
                    bytes.fromhex(item.input_token[2:]),
                    bytes.fromhex(item.output_token[2:]),
                    item.amount_input,
                    item.amount_output,
                    item.amount_bound,
                    item.supports_fee_on_transfer,
                    bytes.fromhex(item.pool[2:]),
                    item.is_pool_uniswap_v2,
                ))

    with curr.copy(
        '''
            COPY uniswap_universal_router_txns (
                txn_hash,
                block_number,
                txn_idx,
                failed,
                from_address,
                to_address,
                exact_input,
                input_token,
                output_token,
                amount_input,
                amount_output,
                amount_bound,
                supports_fee_on_transfer,
                pool,
                pool_is_v2
            ) FROM STDIN
        ''') as cpy:

        for item in txns:
            if isinstance(item, UniswapUniversalRouterSwap):
                if (item.block_number, item.transaction_index) in txns_already_stored:
                    continue
                if item.txn_hash in seen_txn_hashes:
                    print('duplicate txn hash')
                    continue
                seen_txn_hashes.add(item.txn_hash)
                cpy.write_row((
                    item.txn_hash,
                    item.block_number,
                    item.transaction_index,
                    item.failed,
                    bytes.fromhex(item.from_address[2:]),
                    bytes.fromhex(item.to_address[2:]),
                    item.exact_input,
                    bytes.fromhex(item.input_token[2:]),
                    bytes.fromhex(item.output_token[2:]),
                    item.amount_input,
                    item.amount_output,
                    item.amount_bound,
                    item.supports_fee_on_transfer,
                    bytes.fromhex(item.pool[2:]),
                    item.is_pool_uniswap_v2,
                ))

    with curr.copy(
        '''
            COPY sushiswap_router_txns (
                txn_hash,
                block_number,
                txn_idx,
                failed,
                from_address,
                to_address,
                exact_input,
                input_token,
                output_token,
                amount_input,
                amount_output,
                amount_bound,
                supports_fee_on_transfer,
                pool
            ) FROM STDIN
        ''') as cpy:

        for item in txns:
            if isinstance(item, SushiswapRouterSwap):
                if (item.block_number, item.transaction_index) in txns_already_stored:
                    continue
                if item.txn_hash in seen_txn_hashes:
                    print('duplicate txn hash')
                    continue
                seen_txn_hashes.add(item.txn_hash)
                cpy.write_row((
                    item.txn_hash,
                    item.block_number,
                    item.transaction_index,
                    item.failed,
                    bytes.fromhex(item.from_address[2:]),
                    bytes.fromhex(item.to_address[2:]),
                    item.exact_input,
                    bytes.fromhex(item.input_token[2:]),
                    bytes.fromhex(item.output_token[2:]),
                    item.amount_input,
                    item.amount_output,
                    item.amount_bound,
                    item.supports_fee_on_transfer,
                    bytes.fromhex(item.pool[2:])
                ))
    db.commit()

@functools.cache
def get_local_w3() -> web3.Web3:
    return connect_web3()

def scrape_block(block_number: int) -> typing.List[typing.Union['UniswapV2Router2Swap', 'UniswapV3Router2Swap', 'UniswapUniversalRouterSwap', 'SushiswapRouterSwap']]:
    ret = []

    w3 = get_local_w3()

    block = w3.eth.get_block(
        block_number,
        full_transactions=True
    )
    for transaction in block['transactions']:
        if transaction['to'] == constants.UNISWAP_V2_ROUTER_2:
            parsed = process_univ2_router_2(w3, transaction)
            if parsed is not None:
                ret.append(parsed)
        if transaction['to'] == constants.UNISWAP_V3_ROUTER_2:
            parsed = process_univ3_router_2(w3, transaction)
            if parsed is not None:
                ret.append(parsed)
        if transaction['to'] == constants.SUSHISWAP_ROUTER:
            parsed = process_sushiswap_router(w3, transaction)
            if parsed is not None:
                ret.append(parsed)
        if transaction['to'] == constants.UNISWAP_UNIVERSAL_ROUTER:
            parsed = process_uni_universal_router(w3, transaction)
            if parsed is not None:
                ret.append(parsed)

    return ret


class UniswapV2Router2Swap(typing.NamedTuple):
    txn_hash: bytes
    block_number: int
    transaction_index: int
    from_address: str
    to_address: str
    exact_input: bool
    input_token: str
    output_token: str
    amount_input: int
    amount_output: int
    supports_fee_on_transfer: bool
    amount_bound: int # either min out or max in
    pool: str
    failed: bool

class UniswapV3Router2Swap(typing.NamedTuple):
    txn_hash: bytes
    block_number: int
    transaction_index: int
    from_address: str
    to_address: str
    exact_input: bool
    input_token: str
    output_token: str
    amount_input: int
    amount_output: int
    supports_fee_on_transfer: bool
    amount_bound: int # either min out or max in
    pool: str
    is_pool_uniswap_v2: bool
    failed: bool

class UniswapUniversalRouterSwap(typing.NamedTuple):
    txn_hash: bytes
    block_number: int
    transaction_index: int
    from_address: str
    to_address: str
    exact_input: bool
    input_token: str
    output_token: str
    amount_input: int
    amount_output: int
    supports_fee_on_transfer: bool
    amount_bound: int # either min out or max in
    pool: str
    is_pool_uniswap_v2: bool
    failed: bool

class BalancerV2Swap(typing.NamedTuple):
    txn_hash: bytes
    block_number: int
    transaction_index: int
    from_address: str
    to_address: str
    exact_input: bool
    input_token: str
    output_token: str
    amount_input: int
    amount_output: int
    amount_bound: int # either min out or max in
    pool: str
    failed: bool

class SushiswapRouterSwap(typing.NamedTuple):
    txn_hash: bytes
    block_number: int
    transaction_index: int
    from_address: str
    to_address: str
    exact_input: bool
    input_token: str
    output_token: str
    amount_input: int
    amount_output: int
    supports_fee_on_transfer: bool
    amount_bound: int # either min out or max in
    pool: str
    failed: bool


def process_univ2_router_2(w3: web3.Web3, txn: web3.types.TxData) -> typing.Optional[UniswapV2Router2Swap]:
    assert txn['to'] == constants.UNISWAP_V2_ROUTER_2
    b_input = txn.input

    try:
        func, args = _uv2_router2().decode_function_input(b_input)
    except:
        print('cannot decode function')
        return

    if 'liquidity' in func.fn_name.lower():
        # boring
        return

    receipt = w3.eth.get_transaction_receipt(txn['hash'])

    if func.fn_name == 'swapExactETHForTokens':
        if len(args['path']) > 2:
            # no complex paths please thanks
            return

        if len(args['path']) < 2:
            # dumb
            return

        exact_input = True
        input_token = constants.WETH_ADDRESS
        input_amount = txn['value']
        output_token = args['path'][-1]
        output_amount = None
        amount_bound = args['amountOutMin']
        to_address = args['to']
        supports_fee_on_transfer = False

    elif func.fn_name == 'swapExactTokensForETH':
        if len(args['path']) > 2:
            # no complex paths please thanks
            return

        if len(args['path']) < 2:
            # dumb
            return

        exact_input = True
        input_token = args['path'][0]
        input_amount = args['amountIn']
        output_token = constants.WETH_ADDRESS
        output_amount = None
        amount_bound = args['amountOutMin']
        to_address = args['to']
        supports_fee_on_transfer = False

    elif func.fn_name == 'swapExactTokensForTokens':
        if len(args['path']) > 2:
            # no complex paths please thanks
            return

        exact_input = True
        input_token = args['path'][0]
        input_amount = args['amountIn']
        output_token = args['path'][-1]
        output_amount = None
        amount_bound = args['amountOutMin']
        to_address = args['to']
        supports_fee_on_transfer = False

    elif func.fn_name == 'swapExactTokensForETHSupportingFeeOnTransferTokens':
        if len(args['path']) > 2:
            # no complex paths please thanks
            return

        exact_input = True
        input_token = args['path'][0]
        input_amount = args['amountIn']
        output_token = constants.WETH_ADDRESS
        output_amount = None
        amount_bound = args['amountOutMin']
        to_address = args['to']
        supports_fee_on_transfer = True

    elif func.fn_name == 'swapExactETHForTokensSupportingFeeOnTransferTokens':
        if len(args['path']) > 2:
            # no complex paths please thanks
            return

        exact_input = True
        input_token = constants.WETH_ADDRESS
        input_amount = txn['value']
        output_token = args['path'][-1]
        output_amount = None
        amount_bound = args['amountOutMin']
        to_address = args['to']
        supports_fee_on_transfer = True

    elif func.fn_name == 'swapExactTokensForTokensSupportingFeeOnTransferTokens':
        if len(args['path']) > 2:
            # no complex paths please thanks
            return

        exact_input = True
        input_token = args['path'][0]
        input_amount = args['amountIn']
        output_token = args['path'][-1]
        output_amount = None
        amount_bound = args['amountOutMin']
        to_address = args['to']
        supports_fee_on_transfer = True

    elif func.fn_name == 'swapETHForExactTokens':
        if len(args['path']) > 2:
            return

        exact_input = False
        input_token = constants.WETH_ADDRESS
        input_amount = None
        output_token = args['path'][-1]
        output_amount = args['amountOut']
        amount_bound = txn['value']
        to_address = args['to']
        supports_fee_on_transfer = False

    elif func.fn_name == 'swapTokensForExactETH':
        if len(args['path']) > 2:
            return

        exact_input = False
        input_token = args['path'][0]
        input_amount = None
        output_token = constants.WETH_ADDRESS
        output_amount = args['amountOut']
        amount_bound = args['amountInMax']
        to_address = args['to']
        supports_fee_on_transfer = False

    elif func.fn_name == 'swapTokensForExactTokens':
        if len(args['path']) > 2:
            return

        exact_input = False
        input_token = args['path'][0]
        input_amount = None
        output_token = args['path'][-1]
        output_amount = args['amountOut']
        amount_bound = args['amountInMax']
        to_address = args['to']
        supports_fee_on_transfer = False

    else:
        # unknown function
        return


    # compute pool address
    binput_token, boutput_token = [bytes.fromhex(input_token[2:]), bytes.fromhex(output_token[2:])]
    btoken0, btoken1 = sorted([binput_token, boutput_token])
    zero_for_one = btoken0 == binput_token

    pool= web3.Web3.to_checksum_address(uniswap_v2_pool_address(btoken0, btoken1))

    if receipt['status'] == 0x1:
        # success, find output (input) amount

        # where is the swap log? should be just one (we'll check to ensure it's just one)
        relevant_logs = []
        for log in receipt['logs']:
            if log['address'] == pool and \
                len(log['topics']) > 0 and \
                log['topics'][0].endswith(constants.UNISWAP_V2_SWAP_TOPIC()):
                relevant_logs.append(log)

        if len(relevant_logs) != 1:
            # probably a token's transfer() method calls swap, ignore
            return

        log = relevant_logs[0]

        # found the log, find needed amount
        payload = log['data']
        amt0_in = int.from_bytes(payload[:32], byteorder='big', signed=False)
        amt1_in = int.from_bytes(payload[32:64], byteorder='big', signed=False)
        amt0_out = int.from_bytes(payload[64:96], byteorder='big', signed=False)
        amt1_out = int.from_bytes(payload[96:128], byteorder='big', signed=False)
        if output_amount is None:
            if zero_for_one:
                output_amount = amt1_out
            else:
                output_amount = amt0_out
        else:
            assert input_amount is None
            if zero_for_one:
                input_amount = amt0_in
            else:
                input_amount = amt1_in
    else:
        # failed txn, do nothing...
        pass

    return UniswapV2Router2Swap(
        txn_hash=bytes(txn['hash']),
        block_number=txn['blockNumber'],
        transaction_index=txn['transactionIndex'],
        amount_bound=amount_bound,
        amount_input=input_amount,
        amount_output=output_amount,
        input_token=input_token,
        output_token=output_token,
        exact_input=exact_input,
        failed=receipt['status'] != 0x1,
        from_address=receipt['from'],
        pool=pool,
        to_address=to_address,
        supports_fee_on_transfer=supports_fee_on_transfer
    )


def process_univ3_router_2(w3: web3.Web3, txn: web3.types.TxData) -> typing.Optional[UniswapV3Router2Swap]:
    assert txn['to'] == constants.UNISWAP_V3_ROUTER_2
    b_input = txn.input

    try:
        func, args = _uv3_router2().decode_function_input(b_input)
    except:
        return

    if 'liquidity' in func.fn_name.lower():
        # boring
        return

    while func.fn_name == 'multicall':
        # get real func
        if len(args['data']) > 1:
            # too complex, ignore
            return

        try:
            func, args = _uv3_router2().decode_function_input(args['data'][0])
        except ValueError as e:
            if 'Could not find any function with matching selector' in str(e):
                # not sure what function this is
                return
            raise
        except OverflowError:
            # ignore
            return

    if func.fn_name == 'swapExactTokensForTokens':
        if len(args['path']) > 2:
            # no complex paths please thanks
            return

        exact_input = True
        input_token = args['path'][0]
        input_amount = args['amountIn']
        output_token = args['path'][-1]
        output_amount = None
        amount_bound = args['amountOutMin']
        to_address = args['to']
        supports_fee_on_transfer = False
        pool = '0x' + uniswap_v2_pool_address(input_token, output_token).hex()
        is_pool_uniswap_v2 = True

    elif func.fn_name == 'swapTokensForExactTokens':
        if len(args['path']) > 2:
            return

        exact_input = False
        input_token = args['path'][0]
        input_amount = None
        output_token = args['path'][-1]
        output_amount = args['amountOut']
        amount_bound = args['amountInMax']
        to_address = args['to']
        supports_fee_on_transfer = False
        pool = '0x' + uniswap_v2_pool_address(input_token, output_token).hex()
        is_pool_uniswap_v2 = True

    elif func.fn_name == 'exactInputSingle':
        input_token = args['params']['tokenIn']
        output_token = args['params']['tokenOut']
        fee = args['params']['fee']
        to_address = args['params']['recipient']
        input_amount = args['params']['amountIn']
        amount_bound = args['params']['amountOutMinimum']


        pool = '0x' + uniswap_v3_pool_address(input_token, output_token, fee).hex()
        is_pool_uniswap_v2 = False
        output_amount = None
        exact_input = True
        supports_fee_on_transfer = False

    elif func.fn_name == 'exactOutputSingle':
        input_token = args['params']['tokenIn']
        output_token = args['params']['tokenOut']
        fee = args['params']['fee']
        to_address = args['params']['recipient']
        output_amount = args['params']['amountOut']
        amount_bound = args['params']['amountInMaximum']

        pool = '0x' + uniswap_v3_pool_address(input_token, output_token, fee).hex()
        is_pool_uniswap_v2 = False
        input_amount = None
        exact_input = False
        supports_fee_on_transfer = False

    elif func.fn_name == 'exactInput':
        path = args['params']['path']
        to_address = args['params']['recipient']
        input_amount = args['params']['amountIn']
        amount_bound = args['params']['amountOutMinimum']


        n_pools = (len(path) - 20) // 23
        if n_pools > 1:
            # too complex
            return

        input_token = '0x' + path[:20].hex()
        fee = int.from_bytes(path[20:23], byteorder='big', signed=False)
        output_amount = None
        output_token = '0x' + path[23:43].hex()
        pool = '0x' + uniswap_v3_pool_address(input_token, output_token, fee).hex()
        is_pool_uniswap_v2 = False
        exact_input = True
        supports_fee_on_transfer = False

    elif func.fn_name == 'exactOutput':
        path = args['params']['path']
        to_address = args['params']['recipient']
        output_amount = args['params']['amountOut']
        amount_bound = args['params']['amountInMaximum']

        n_pools = (len(path) - 20) // 23
        if n_pools > 1:
            # too complex
            return

        output_token = '0x' + path[:20].hex()
        fee = int.from_bytes(path[20:23], byteorder='big', signed=False)
        input_token = '0x' + path[23:43].hex()
        pool = '0x' + uniswap_v3_pool_address(input_token, output_token, fee).hex()
        input_amount = None
        is_pool_uniswap_v2 = False
        exact_input = False
        supports_fee_on_transfer = False
    else:
        return

    receipt = w3.eth.get_transaction_receipt(txn['hash'])
    pool = web3.Web3.to_checksum_address(pool)

    if receipt['status'] == 0x1:
        # success, find output (input) amount
        if is_pool_uniswap_v2:
            swap_topic = constants.UNISWAP_V2_SWAP_TOPIC()
        else:
            swap_topic = constants.UNISWAP_V3_SWAP_TOPIC()

        # where is the swap log? should be just one (we'll check to ensure it's just one)
        relevant_logs = []
        for log in receipt['logs']:
            if log['address'] == pool and \
                len(log['topics']) > 0 and \
                log['topics'][0].endswith(swap_topic):
                relevant_logs.append(log)

        if len(relevant_logs) != 1:
            # probably a token's transfer() method calls swap, ignore
            return

        log = relevant_logs[0]

        zero_for_one = bytes.fromhex(input_token[2:]) < bytes.fromhex(output_token[2:])
        payload = log['data']

        # found the log, find needed amount
        if is_pool_uniswap_v2:
            amt0_in = int.from_bytes(payload[:32], byteorder='big', signed=False)
            amt1_in = int.from_bytes(payload[32:64], byteorder='big', signed=False)
            amt0_out = int.from_bytes(payload[64:96], byteorder='big', signed=False)
            amt1_out = int.from_bytes(payload[96:128], byteorder='big', signed=False)
        else:
            amount0 = int.from_bytes(payload[:32], byteorder='big', signed=True)
            amount1 = int.from_bytes(payload[32:32+32], byteorder='big', signed=True)
            if amount0 <= 0:
                assert amount1 >= 0
                amt0_in = 0
                amt0_out = -amount0
                amt1_in = amount1
                amt1_out = 0
            else:
                assert amount1 <= 0
                amt0_in = amount0
                amt0_out = 0
                amt1_in = 0
                amt1_out = -amount1

        if output_amount is None:
            if zero_for_one:
                output_amount = amt1_out
            else:
                output_amount = amt0_out
        else:
            assert input_amount is None
            if zero_for_one:
                input_amount = amt0_in
            else:
                input_amount = amt1_in

    else:
        # failed txn, do nothing...
        pass

    return UniswapV3Router2Swap(
        txn_hash=bytes(txn['hash']),
        block_number=txn['blockNumber'],
        transaction_index=txn['transactionIndex'],
        amount_bound=amount_bound,
        amount_input=input_amount,
        amount_output=output_amount,
        input_token=input_token,
        output_token=output_token,
        exact_input=exact_input,
        failed=receipt['status'] != 0x1,
        from_address=receipt['from'],
        pool=pool,
        to_address=to_address,
        supports_fee_on_transfer=supports_fee_on_transfer,
        is_pool_uniswap_v2=is_pool_uniswap_v2
    )

UNI_UNIVERSAL_COMMAND_MASK = 0x1f
UNI_UNIVERSAL_COMMAND_WRAP_ETH = 0x0b
UNI_UNIVERSAL_COMMAND_UNWRAP_WETH = 0x0c
UNI_UNIVERSAL_COMMAND_TRANSFER = 0x05
UNI_UNIVERSAL_COMMAND_SWEEP = 0x04
UNI_UNIVERSAL_COMMAND_V3_SWAP_EXACT_IN = 0x00
UNI_UNIVERSAL_COMMAND_V3_SWAP_EXACT_OUT = 0x01
UNI_UNIVERSAL_COMMAND_V2_SWAP_EXACT_IN = 0x08
UNI_UNIVERSAL_COMMAND_V2_SWAP_EXACT_OUT = 0x09
UNI_UNIVERSAL_COMMAND_PERMIT2_PERMIT = 0x0a
UNI_UNIVERSAL_RECIPIENT_MSG_SENDER = int.to_bytes(1, length=20, byteorder='big', signed=False)
UNI_UNIVERSAL_RECIPIENT_ADDRESS_THIS = int.to_bytes(2, length=20, byteorder='big', signed=False)

class TooManyPools(Exception):
    pass

class TooManyLogs(Exception):
    pass

class MalformedTxn(Exception):
    pass

class UnsupportedCommand(Exception):
    pass

def process_uni_universal_router(w3: web3.Web3, txn: web3.types.TxData, strict = False) -> typing.Optional[UniswapUniversalRouterSwap]:
    assert txn['to'] == constants.UNISWAP_UNIVERSAL_ROUTER
    b_input = txn.input

    try:
        func, args = _uni_universal_router().decode_function_input(b_input)
        func: web3.contract.ContractFunction
    except:
        if strict:
            raise MalformedTxn()
        return

    if func.fn_name == 'execute':
        important_cmds: typing.List[typing.Tuple[int, bytes]] = []
        if len(args['commands']) != len(args['inputs']):
            l.warning(f'Uneven args universal router, ignoring')
            if strict:
                raise MalformedTxn()
            return

        for cmd, input in zip(args['commands'], args['inputs']):
            masked = cmd & UNI_UNIVERSAL_COMMAND_MASK
            if masked in [UNI_UNIVERSAL_COMMAND_WRAP_ETH, UNI_UNIVERSAL_COMMAND_UNWRAP_WETH, UNI_UNIVERSAL_COMMAND_TRANSFER, UNI_UNIVERSAL_COMMAND_SWEEP, UNI_UNIVERSAL_COMMAND_PERMIT2_PERMIT]:
                # boring, ignore
                continue

            if masked not in [
                UNI_UNIVERSAL_COMMAND_V3_SWAP_EXACT_IN, UNI_UNIVERSAL_COMMAND_V3_SWAP_EXACT_OUT, \
                UNI_UNIVERSAL_COMMAND_V2_SWAP_EXACT_IN, UNI_UNIVERSAL_COMMAND_V2_SWAP_EXACT_OUT
                ]:
                # probably NFT bullshit in this txn, ignore
                if strict:
                    raise UnsupportedCommand()
                return

            important_cmds.append((masked, input))

        if len(important_cmds) != 1:
            # too many (few) things to do
            if strict:
                raise TooManyPools()
            return

        (cmd, input) = important_cmds[0]
        def decode_recipient(recipient: bytes):
            if recipient == UNI_UNIVERSAL_RECIPIENT_ADDRESS_THIS:
                return constants.UNISWAP_UNIVERSAL_ROUTER
            elif recipient == UNI_UNIVERSAL_RECIPIENT_MSG_SENDER:
                return txn['from']
            return recipient

        if cmd == UNI_UNIVERSAL_COMMAND_V3_SWAP_EXACT_IN:
            try:
                (
                    recipient, input_amount, amount_bound, path, payer_is_user
                ) = eth_abi.decode(('address', 'uint256', 'uint256', 'bytes', 'bool'), input)
            except:
                if strict:
                    raise MalformedTxn()
                return
            to_address = decode_recipient(recipient)

            n_pools = (len(path) - 20) // 23
            if n_pools > 1:
                # too complex
                if strict:
                    raise TooManyPools()
                return

            input_token = '0x' + path[:20].hex()
            fee = int.from_bytes(path[20:23], byteorder='big', signed=False)
            output_amount = None
            output_token = '0x' + path[23:43].hex()
            pool = '0x' + uniswap_v3_pool_address(input_token, output_token, fee).hex()
            is_pool_uniswap_v2 = False
            exact_input = True
            supports_fee_on_transfer = False

        elif cmd == UNI_UNIVERSAL_COMMAND_V3_SWAP_EXACT_OUT:
            try:
                (
                    recipient, output_amount, amount_bound, path, payer_is_user
                ) = eth_abi.decode(('address', 'uint256', 'uint256', 'bytes', 'bool'), input)
            except:
                if strict:
                    raise MalformedTxn()
                return
            to_address = decode_recipient(recipient)

            n_pools = (len(path) - 20) // 23
            if n_pools > 1:
                # too complex
                if strict:
                    raise TooManyPools()
                return

            output_token = '0x' + path[:20].hex()
            fee = int.from_bytes(path[20:23], byteorder='big', signed=False)
            input_token = '0x' + path[23:43].hex()
            pool = '0x' + uniswap_v3_pool_address(input_token, output_token, fee).hex()
            input_amount = None
            is_pool_uniswap_v2 = False
            exact_input = False
            supports_fee_on_transfer = False
        elif cmd == UNI_UNIVERSAL_COMMAND_V2_SWAP_EXACT_IN:
            try:
                (
                    recipient, input_amount, amount_bound, path, payer_is_user
                ) = eth_abi.decode(('address', 'uint256', 'uint256', 'address[]', 'bool'), input)
            except:
                if strict:
                    raise MalformedTxn()
                return
            to_address = decode_recipient(recipient)

            if len(path) != 2:
                # too complex
                if strict:
                    raise TooManyPools()
                return

            exact_input = True
            input_token = path[0]
            output_token = path[-1]
            output_amount = None
            supports_fee_on_transfer = False
            pool = '0x' + uniswap_v2_pool_address(input_token, output_token).hex()
            is_pool_uniswap_v2 = True

        elif cmd == UNI_UNIVERSAL_COMMAND_V2_SWAP_EXACT_OUT:
            try:
                (
                    recipient, output_amount, amount_bound, path, payer_is_user
                ) = eth_abi.decode(('address', 'uint256', 'uint256', 'address[]', 'bool'), input)
            except:
                if strict:
                    raise MalformedTxn()
                return
            to_address = decode_recipient(recipient)

            if len(path) != 2:
                # too complex
                if strict:
                    raise TooManyPools()
                return

            exact_input = False
            input_token = path[0]
            input_amount = None
            output_token = path[-1]
            supports_fee_on_transfer = False
            pool = '0x' + uniswap_v2_pool_address(input_token, output_token).hex()
            is_pool_uniswap_v2 = True
        else:
            raise NotImplementedError(f'unknown cmd {cmd}; should be unreachable')

        pool = web3.Web3.to_checksum_address(pool)

        try:
            receipt = None
            receipt = w3.eth.get_transaction_receipt(txn['hash'])

            if receipt['status'] == 0x1:
                # where is the swap log? should be just one (we'll check to ensure it's just one)
                relevant_logs = []
                if is_pool_uniswap_v2:
                    topic_needed = constants.UNISWAP_V2_SWAP_TOPIC()
                else:
                    topic_needed = constants.UNISWAP_V3_SWAP_TOPIC()
                for log in receipt['logs']:
                    if log['address'] == pool and \
                        len(log['topics']) > 0 and \
                        log['topics'][0].endswith(topic_needed):
                        relevant_logs.append(log)

                if len(relevant_logs) != 1:
                    # probably a token's transfer() method calls swap, ignore
                    if strict:
                        raise TooManyLogs()
                    return

                log = relevant_logs[0]

                zero_for_one = bytes.fromhex(input_token[2:]) < bytes.fromhex(output_token[2:])

                payload = log['data']

                # found the log, find needed amount
                if is_pool_uniswap_v2:
                    amt0_in = int.from_bytes(payload[:32], byteorder='big', signed=False)
                    amt1_in = int.from_bytes(payload[32:64], byteorder='big', signed=False)
                    amt0_out = int.from_bytes(payload[64:96], byteorder='big', signed=False)
                    amt1_out = int.from_bytes(payload[96:128], byteorder='big', signed=False)
                else:
                    amount0 = int.from_bytes(payload[:32], byteorder='big', signed=True)
                    amount1 = int.from_bytes(payload[32:32+32], byteorder='big', signed=True)

                    if amount0 <= 0:
                        assert amount1 >= 0
                        amt0_in = 0
                        amt0_out = -amount0
                        amt1_in = amount1
                        amt1_out = 0
                    else:
                        assert amount1 <= 0
                        amt0_in = amount0
                        amt0_out = 0
                        amt1_in = 0
                        amt1_out = -amount1

                if output_amount is None:
                    if zero_for_one:
                        output_amount = amt1_out
                    else:
                        output_amount = amt0_out
                else:
                    assert input_amount is None
                    if zero_for_one:
                        input_amount = amt0_in
                    else:
                        input_amount = amt1_in

            else:
                # failed txn, do nothing...
                pass

        except web3.exceptions.TransactionNotFound:
            pass

        return UniswapUniversalRouterSwap(
            txn_hash=bytes(txn['hash']),
            block_number=txn['blockNumber'],
            transaction_index=txn['transactionIndex'],
            amount_bound=amount_bound,
            amount_input=input_amount,
            amount_output=output_amount,
            input_token=input_token,
            output_token=output_token,
            exact_input=exact_input,
            failed= receipt['status'] != 0x1 if receipt else False,
            from_address=txn['from'],
            pool=pool,
            to_address=to_address,
            supports_fee_on_transfer=supports_fee_on_transfer,
            is_pool_uniswap_v2=is_pool_uniswap_v2
        )

def process_sushiswap_router(w3: web3.Web3, txn, strict = False) -> typing.Optional[SushiswapRouterSwap]:
    assert txn['to'] == constants.SUSHISWAP_ROUTER
    b_input = txn.input

    try:
        func, args = _uv2_router2().decode_function_input(b_input)
    except:
        print('cannot decode function')
        if strict:
            raise MalformedTxn()
        return

    if 'liquidity' in func.fn_name.lower():
        # boring
        if strict:
            raise UnsupportedCommand()
        return

    receipt = w3.eth.get_transaction_receipt(txn['hash'])

    if func.fn_name == 'swapExactETHForTokens':
        if len(args['path']) > 2:
            # no complex paths please thanks
            if strict:
                raise TooManyPools()
            return

        if len(args['path']) < 2:
            # dumb
            if strict:
                raise MalformedTxn()
            return

        exact_input = True
        input_token = constants.WETH_ADDRESS
        input_amount = txn['value']
        output_token = args['path'][-1]
        output_amount = None
        amount_bound = args['amountOutMin']
        to_address = args['to']
        supports_fee_on_transfer = False

    elif func.fn_name == 'swapExactTokensForETH':
        if len(args['path']) > 2:
            # no complex paths please thanks
            if strict:
                raise TooManyPools()
            return

        if len(args['path']) < 2:
            # dumb
            if strict:
                raise MalformedTxn()
            return

        exact_input = True
        input_token = args['path'][0]
        input_amount = args['amountIn']
        output_token = constants.WETH_ADDRESS
        output_amount = None
        amount_bound = args['amountOutMin']
        to_address = args['to']
        supports_fee_on_transfer = False

    elif func.fn_name == 'swapExactTokensForTokens':
        if len(args['path']) > 2:
            # no complex paths please thanks
            if strict:
                raise TooManyPools()
            return

        exact_input = True
        input_token = args['path'][0]
        input_amount = args['amountIn']
        output_token = args['path'][-1]
        output_amount = None
        amount_bound = args['amountOutMin']
        to_address = args['to']
        supports_fee_on_transfer = False

    elif func.fn_name == 'swapExactTokensForETHSupportingFeeOnTransferTokens':
        if len(args['path']) > 2:
            # no complex paths please thanks
            if strict:
                raise TooManyPools()
            return

        exact_input = True
        input_token = args['path'][0]
        input_amount = args['amountIn']
        output_token = constants.WETH_ADDRESS
        output_amount = None
        amount_bound = args['amountOutMin']
        to_address = args['to']
        supports_fee_on_transfer = True

    elif func.fn_name == 'swapExactETHForTokensSupportingFeeOnTransferTokens':
        if len(args['path']) > 2:
            # no complex paths please thanks
            if strict:
                raise TooManyPools()
            return

        exact_input = True
        input_token = constants.WETH_ADDRESS
        input_amount = txn['value']
        output_token = args['path'][-1]
        output_amount = None
        amount_bound = args['amountOutMin']
        to_address = args['to']
        supports_fee_on_transfer = True

    elif func.fn_name == 'swapExactTokensForTokensSupportingFeeOnTransferTokens':
        if len(args['path']) > 2:
            # no complex paths please thanks
            if strict:
                raise TooManyPools()
            return

        exact_input = True
        input_token = args['path'][0]
        input_amount = args['amountIn']
        output_token = args['path'][-1]
        output_amount = None
        amount_bound = args['amountOutMin']
        to_address = args['to']
        supports_fee_on_transfer = True

    elif func.fn_name == 'swapETHForExactTokens':
        if len(args['path']) > 2:
            if strict:
                raise TooManyPools()
            return

        exact_input = False
        input_token = constants.WETH_ADDRESS
        input_amount = None
        output_token = args['path'][-1]
        output_amount = args['amountOut']
        amount_bound = txn['value']
        to_address = args['to']
        supports_fee_on_transfer = False

    elif func.fn_name == 'swapTokensForExactETH':
        if len(args['path']) > 2:
            if strict:
                raise TooManyPools()
            return

        exact_input = False
        input_token = args['path'][0]
        input_amount = None
        output_token = constants.WETH_ADDRESS
        output_amount = args['amountOut']
        amount_bound = args['amountInMax']
        to_address = args['to']
        supports_fee_on_transfer = False

    elif func.fn_name == 'swapTokensForExactTokens':
        if len(args['path']) > 2:
            if strict:
                raise TooManyPools()
            return

        exact_input = False
        input_token = args['path'][0]
        input_amount = None
        output_token = args['path'][-1]
        output_amount = args['amountOut']
        amount_bound = args['amountInMax']
        to_address = args['to']
        supports_fee_on_transfer = False

    else:
        # unknown function
        print('unknown function', func.fn_name)
        if strict:
            raise UnsupportedCommand()
        return


    # compute pool address
    binput_token, boutput_token = [bytes.fromhex(input_token[2:]), bytes.fromhex(output_token[2:])]
    btoken0, btoken1 = sorted([binput_token, boutput_token])
    zero_for_one = btoken0 == binput_token

    pool= '0x' + sushiswap_pool_address(btoken0, btoken1).hex()
    pool = web3.Web3.to_checksum_address(pool)

    if receipt['status'] == 0x1:
        # success, find output (input) amount

        # where is the swap log? should be just one (we'll check to ensure it's just one)
        relevant_logs = []
        for log in receipt['logs']:
            if log['address'] == pool and \
                len(log['topics']) > 0 and \
                log['topics'][0] == constants.UNISWAP_V2_SWAP_TOPIC():
                relevant_logs.append(log)

        if len(relevant_logs) != 1:
            # probably a token's transfer() method calls swap, ignore
            if strict:
                raise TooManyLogs()
            return

        log = relevant_logs[0]

        # found the log, find needed amount
        payload = log['data']
        amt0_in = int.from_bytes(payload[:32], byteorder='big', signed=False)
        amt1_in = int.from_bytes(payload[32:64], byteorder='big', signed=False)
        amt0_out = int.from_bytes(payload[64:96], byteorder='big', signed=False)
        amt1_out = int.from_bytes(payload[96:128], byteorder='big', signed=False)
        if output_amount is None:
            if zero_for_one:
                output_amount = amt1_out
            else:
                output_amount = amt0_out
        else:
            assert input_amount is None
            if zero_for_one:
                input_amount = amt0_in
            else:
                input_amount = amt1_in
    else:
        # failed txn, do nothing...
        pass

    return SushiswapRouterSwap(
        txn_hash=bytes(txn['hash']),
        block_number=txn['blockNumber'],
        transaction_index=txn['transactionIndex'],
        amount_bound=amount_bound,
        amount_input=input_amount,
        amount_output=output_amount,
        input_token=input_token,
        output_token=output_token,
        exact_input=exact_input,
        failed=receipt['status'] != 0x1,
        from_address=receipt['from'],
        pool=pool,
        to_address=to_address,
        supports_fee_on_transfer=supports_fee_on_transfer
    )

_uv3_router2_cache = None
def _uv3_router2() -> web3.contract.Contract:
    global _uv3_router2_cache
    if _uv3_router2_cache is None:
        with open('abis/UniswapV3Router2.json') as fin:
            _uv3_router2_cache = web3.Web3().eth.contract(
                address=b'\x00'*20,
                abi=ujson.load(fin)
            )
    return _uv3_router2_cache


_uv2_router2_cache = None
def _uv2_router2() -> web3.contract.Contract:
    global _uv2_router2_cache
    if _uv2_router2_cache is None:
        with open('abis/UniswapV2Router2.json') as fin:
            _uv2_router2_cache = web3.Web3().eth.contract(
                address=b'\x00'*20,
                abi=ujson.load(fin)
            )
    return _uv2_router2_cache


_uni_universal_router_cache = None
def _uni_universal_router():
    global _uni_universal_router_cache
    if _uni_universal_router_cache is None:
        with open('abis/UniswapUniversalRouter.json') as fin:
            _uni_universal_router_cache = web3.Web3().eth.contract(
                address=b'\x00'*20,
                abi=ujson.load(fin)
            )
    return _uni_universal_router_cache


if __name__ == '__main__':
    main()
