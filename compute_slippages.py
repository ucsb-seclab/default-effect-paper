import collections
import io
import os
import decimal
import tarfile
import time
import psycopg
import datetime
import typing
import itertools
import multiprocessing
import functools
import argparse
import backoff
import web3.contract
import web3.types
import lzma
import web3
import ujson
import dotenv


from models.uniswap_v2 import UniswapV2Pricer
from models.uniswap_v3 import NotEnoughLiquidityException, UniswapV3Pricer
from scrape_router_swap_txns import uniswap_v3_pool_address

import constants

from db import connect_db
from utils import connect_web3

N_WORKERS = 24

MEMPOOL_OBSERVED_TOO_LATE = 'mempool_observed_too_late'
ARRIVED_BEFORE_WINDOW = 'arrived_before_window'
USES_FEE_ON_TRANSFER = 'fee_on_transfer'
NOT_ENOUGH_LIQUIDITY = 'not_enough_liquidity'


def main():
    dotenv.load_dotenv()
    results = bulk_process()

    results = [result[0] for result in results]

    print('Inserting results....')
    db = connect_db()
    curr = db.cursor()
    with curr.copy('''
        COPY slippage_results
        (
            block_number, transaction_index, transaction_hash,
            mempool_arrival_time, queue_time_seconds, sender,
            recipient, router, pool, token_in, token_out,
            exact_in, amount_in, amount_out, amount_in_usd,
            amount_out_usd, boundary_amount, expected_amount,
            success, fail_reason, slippage, slippage_protector_triggered
        )
        FROM STDIN
        ''') as cpy:
        for row in results:
            cpy.write_row(tuple(row))

    db.commit()
    print('Done')


def bulk_process(strict = False, routers = None):
    """
    Process slippages in bulk, using multiprocessing
    Can be done strictly (reporting failures), and selectively only for certain routers
    """
    results = []
    with multiprocessing.Pool(N_WORKERS, initializer=init_worker) as pool:
        # start by doing a small batch to align the block to a multiple of 1000
        new_start = (constants.START_BLOCK // 1000) * 1000 + 1000
        results.extend(process_batch((constants.START_BLOCK, new_start - constants.START_BLOCK, strict, routers)))
        print('first batch completed')

        # do another batch to align the end block to a multiple of 1000
        new_end = (constants.END_BLOCK // 1000) * 1000
        results.extend(process_batch((new_end, constants.END_BLOCK - new_end + 1, strict, routers)))
        print('last batch completed')

        iresults = pool.imap_unordered(
            process_batch,
            zip(
                range(new_start, new_end, 1_000),
                itertools.repeat(1_000),
                itertools.repeat(strict),
                itertools.repeat(routers)
            )
        )
        n_batches = (new_end - new_start) // 1_000
        measures = collections.deque(maxlen=10)
        measures.append((0, time.time()))
        for i, batch in enumerate(iresults):
            results.extend(batch)

            if i % 10 == 0:
                # status update
                t_now = time.time()
                measures.append((i+1, t_now))
                speed = (measures[-1][0] - measures[0][0]) / (measures[-1][1] - measures[0][1])
                n_remaining = n_batches - i - 1
                eta = n_remaining / speed
                eta_td = datetime.timedelta(seconds=eta)
                print(f'Processed {i+1}/{n_batches} batches ({speed:.2f} batches/s), ETA: {eta_td}')

    print('Done bulk_process')
    return results


def _backoff_reset(*args, **kwargs):
    get_w3.cache_clear()

@backoff.on_exception(
    backoff.expo,
    (TimeoutError,),
    max_tries=10,
    on_backoff=_backoff_reset
)
def process_batch(start_block_nblocks_strict_and_routers: typing.Tuple[int, int, bool, typing.Optional[typing.List[str]]]):
    start_block, n_blocks, strict, routers = start_block_nblocks_strict_and_routers
    db = get_db()
    curr = db.cursor()
    w3 = get_w3()
    results = []

    batch_start = start_block
    batch_end = start_block + n_blocks

    # get the blocks and transactions that we need to pull
    with db.cursor() as curr:
        # build the query
        if routers is None:
            routers = [
                constants.UNISWAP_V2_ROUTER_2,
                constants.UNISWAP_UNIVERSAL_ROUTER,
                constants.UNISWAP_V3_ROUTER_2,
                constants.SUSHISWAP_ROUTER
            ]
        
        query_parts = []
        for router in routers:
            table = {
                constants.UNISWAP_V2_ROUTER_2: 'uniswap_v2_router_2_txns',
                constants.UNISWAP_UNIVERSAL_ROUTER: 'uniswap_universal_router_txns',
                constants.UNISWAP_V3_ROUTER_2: 'uniswap_v3_router_2_txns',
                constants.SUSHISWAP_ROUTER: 'sushiswap_router_txns'
            }[router]
            query_parts.append(
                f'''
                SELECT block_number, txn_hash
                FROM {table} r
                WHERE failed
                    AND NOT EXISTS (
                        SELECT FROM slippage_results sr
                        WHERE sr.block_number = r.block_number AND sr.transaction_index = r.txn_idx
                    )
                '''
            )
        
        query = ' UNION '.join(query_parts)


        curr.execute(
            f'''
            SELECT * FROM (
                {query}
            ) x(bn, txn_hash)
            WHERE %s <= bn AND bn < %s
            ORDER BY bn, idx
            ''',
            (
                batch_start,
                batch_end
            )
        )
        queries = curr.fetchall()

        revert_reasons = {}
        for _, txn_hash in queries:
            # txn_hash should be bytes already
            txn_hash = bytes(txn_hash)
            revert_reason = _get_revert_reason(w3, txn_hash)
            revert_reasons[txn_hash] = revert_reason

        for block_number in range(batch_start, batch_end):
            if constants.UNISWAP_UNIVERSAL_ROUTER in routers:
                results.extend(
                    zip(
                        scrape_uniswap_universal_router(
                            curr, w3, revert_reasons, block_number, strict
                        ),
                        itertools.repeat(constants.UNISWAP_UNIVERSAL_ROUTER)
                    )
                )

            if constants.UNISWAP_V2_ROUTER_2 in routers:
                results.extend(
                    zip(
                        scrape_uv2_router_2(
                            curr, w3, revert_reasons, block_number
                        ),
                        itertools.repeat(constants.UNISWAP_V2_ROUTER_2)
                    )
                )

            if constants.UNISWAP_V3_ROUTER_2 in routers:
                results.extend(
                    zip(
                        scrape_uv3_router_2(
                            curr, w3, revert_reasons, block_number
                        ),
                        itertools.repeat(constants.UNISWAP_V3_ROUTER_2)
                    )
                )

            if constants.SUSHISWAP_ROUTER in routers:
                results.extend(
                    zip(
                        scrape_sushiswap(
                            curr, w3, revert_reasons, block_number, strict
                        ),
                        itertools.repeat(constants.SUSHISWAP_ROUTER)
                    )
                )

    db.rollback() # ensure we are not in a transaction
    return results


class SlippageResult(typing.NamedTuple):
    block_number: int
    transaction_index: int
    transaction_hash: str
    mempool_arrival_time: datetime.datetime
    queue_time_seconds: float
    sender: str
    recipient: str
    router: str
    pool: str
    token_in: str
    token_out: str
    exact_in: bool
    amount_in: int
    amount_out: int
    amount_in_usd: decimal.Decimal
    amount_out_usd: decimal.Decimal
    boundary_amount: int
    expected_amount: int
    success: bool
    fail_reason: str
    slippage: float
    slippage_protector_triggered: bool


def init_worker():
    get_w3.cache_clear()
    get_db.cache_clear()

@functools.cache
def get_w3() -> web3.Web3:
    return connect_web3()

@functools.cache
def get_db() -> psycopg.Connection:
    return connect_db()

def scrape_uv2_router_2(
    curr: psycopg.cursor_async.AsyncCursor,
    w3: web3.Web3,
    revert_reasons: typing.Dict[bytes, bytes],
    block_number: int
) -> typing.List[SlippageResult]:
    ret: typing.List[SlippageResult] = []

    curr.execute(
        '''
        SELECT
            uv2_txn.id,
            uv2_txn.txn_hash,
            (
                SELECT block_timestamps.number
                FROM block_timestamps
                WHERE block_timestamps.timestamp < arr.timestamp
                ORDER BY block_timestamps.timestamp DESC
                LIMIT 1
            ) number,
            (
                SELECT block_timestamps.timestamp
                FROM block_timestamps
                WHERE block_timestamps.number = uv2_txn.block_number
            ) block_ts,
            uv2_txn.block_number,
            uv2_txn.txn_idx,
            arr.timestamp,
            failed,
            exact_input,
            input_token,
            output_token,
            amount_input,
            amount_output,
            amount_bound,
            supports_fee_on_transfer,
            pool,
            from_address,
            to_address
        FROM uniswap_v2_router_2_txns uv2_txn
        JOIN transaction_timestamps arr ON concat('0x', encode(uv2_txn.txn_hash, 'hex')) = arr.transaction_hash
        WHERE %s = block_number
            AND NOT EXISTS (
                SELECT FROM slippage_results sr
                WHERE sr.block_number = uv2_txn.block_number
                    AND sr.transaction_index = uv2_txn.txn_idx
            )
        ''',
        (block_number,)
    )

    if curr.rowcount == 0:
        return []

    rows = curr.fetchall()

    for i, row in enumerate(rows):
        (
            uv2_id,
            txn_hash,
            presumed_gen_number,
            observed_ts,
            observed_number,
            txn_idx,
            mempool_arrival_ts,
            failed,
            exact_input,
            input_token,
            output_token,
            amount_input,
            amount_output,
            amount_bound,
            supports_fee_on_transfer,
            pool,
            from_address,
            to_address
        ) = row
        if mempool_arrival_ts - constants.START_DATETIME < datetime.timedelta(seconds=12):
            # skip the first 12 seconds of the window because we havent
            # seen a block yet
            continue

        if presumed_gen_number >= observed_number:
            continue

        if supports_fee_on_transfer:
            continue

        pool = '0x' + bytes(pool).hex()
        token0, token1 = sorted([input_token, output_token])
        zero_for_one = input_token == token0

        token0 = '0x' + token0.hex()
        token1 = '0x' + token1.hex()
        model = UniswapV2Pricer(w3, pool, token0, token1)

        try:
            slippage, expectation = infer_slippage_and_expectation_from_model(
                model,
                exact_input,
                amount_input if amount_input is not None else amount_output,
                amount_bound,
                zero_for_one,
                presumed_gen_number
            )
        except:
            # print txn hash
            print(f'Failed to infer slippage for txn {txn_hash.hex()}')
            raise

        if failed:
            # why did it fail? go figure out
            revert_reason = revert_reasons.get(txn_hash)
            fail_reason = get_err_msg(revert_reason) or 'UNKNOWN'
            slippage_protector_triggered = 'EXCESSIVE_INPUT_AMOUNT' in fail_reason or 'INSUFFICIENT_OUTPUT_AMOUNT' in fail_reason
        else:
            fail_reason = ''
            slippage_protector_triggered = False

        amount_in_usd = None
        amount_out_usd = None
        # try:
        #     amount_in_usd = None
        #     amount_out_usd = None
        #     # amount_in_usd = quote_token_usd(
        #     #     legacy_w3,
        #     #     web3.Web3.to_checksum_address(input_token),
        #     #     int(amount_input),
        #     #     observed_number,
        #     # ) if amount_input is not None else None
        #     # amount_out_usd = quote_token_usd(
        #     #     legacy_w3,
        #     #     web3.Web3.to_checksum_address(input_token),
        #     #     int(amount_output),
        #     #     observed_number,
        #     # ) if amount_output is not None else None
        # except token_price_oracle.exceptions.NoPriceData:
        #     # ignore
        #     continue

        ret.append(SlippageResult(
            block_number=observed_number,
            transaction_index=txn_idx, 
            transaction_hash='0x' + txn_hash.hex(),
            mempool_arrival_time=mempool_arrival_ts,
            queue_time_seconds=\
                typing.cast(
                    datetime.timedelta,
                    (observed_ts - mempool_arrival_ts)
                ).total_seconds(),
            sender = web3.Web3.to_checksum_address(from_address),
            recipient = web3.Web3.to_checksum_address(to_address),
            router = f'UniswapV2/{constants.UNISWAP_V2_ROUTER_2}',
            pool = web3.Web3.to_checksum_address(pool),
            token_in = web3.Web3.to_checksum_address(input_token),
            token_out = web3.Web3.to_checksum_address(output_token),
            exact_in = exact_input,
            amount_in = amount_input if amount_input is not None else None,
            amount_in_usd = amount_in_usd,
            amount_out_usd = amount_out_usd,
            amount_out = amount_output if amount_output is not None else None,
            boundary_amount = amount_bound,
            expected_amount = expectation,
            fail_reason = fail_reason,
            slippage = slippage,
            success = not failed,
            slippage_protector_triggered = slippage_protector_triggered
        ))

    return ret

def scrape_uv3_router_2(
    curr: psycopg.cursor_async.AsyncCursor,
    w3: web3.Web3,
    revert_reasons: typing.Dict[bytes, bytes],
    block_number: int
) -> typing.List[SlippageResult]:
    ret: typing.List[SlippageResult] = []


    curr.execute(
        '''
        SELECT
            uv3_txn.id,
            uv3_txn.txn_hash,
            (
                SELECT block_timestamps.number
                FROM block_timestamps
                WHERE block_timestamps.timestamp < arr.timestamp
                ORDER BY block_timestamps.timestamp DESC
                LIMIT 1
            ) number,
            (
                SELECT block_timestamps.timestamp
                FROM block_timestamps
                WHERE block_timestamps.number = uv3_txn.block_number
            ) block_ts,
            uv3_txn.block_number,
            uv3_txn.txn_idx,
            arr.timestamp,
            failed,
            exact_input,
            input_token,
            output_token,
            amount_input,
            amount_output,
            amount_bound,
            supports_fee_on_transfer,
            pool,
            pool_is_v2,
            from_address,
            to_address
        FROM uniswap_v3_router_2_txns uv3_txn
        JOIN transaction_timestamps arr ON concat('0x', encode(uv3_txn.txn_hash, 'hex')) = arr.transaction_hash
        WHERE uv3_txn.block_number = %s
            AND NOT EXISTS (
                SELECT FROM slippage_results sr
                WHERE sr.block_number = uv3_txn.block_number
                    AND sr.transaction_index = uv3_txn.txn_idx
            )
        ''',
        (block_number,)
    )

    if curr.rowcount == 0:
        return []

    rows = curr.fetchall()

    for i, row in enumerate(rows):
        (
            uv3_id,
            txn_hash,
            presumed_gen_number,
            observed_ts,
            observed_number,
            txn_idx,
            mempool_arrival_ts,
            failed,
            exact_input,
            input_token,
            output_token,
            amount_input,
            amount_output,
            amount_bound,
            supports_fee_on_transfer,
            pool,
            pool_is_uniswap_v2,
            from_address,
            to_address
        ) = row
        if mempool_arrival_ts - constants.START_DATETIME < datetime.timedelta(seconds=12):
            # skip the first 12 seconds of the window because we havent
            # seen a block yet
            continue

        if presumed_gen_number >= observed_number:
            continue

        if supports_fee_on_transfer:
            continue

        pool = '0x' + bytes(pool).hex()
        token0, token1 = sorted([input_token, output_token])
        zero_for_one = input_token == token0

        token0 = '0x' + token0.hex()
        token1 = '0x' + token1.hex()

        if pool_is_uniswap_v2:
            model = UniswapV2Pricer(w3, pool, token0, token1)
        else:
            # infer the fee
            for fee in [100, 500, 3_000, 10_000]:
                if pool.endswith(uniswap_v3_pool_address(token0, token1, fee).hex()):
                    # found it
                    break
            else:
                print(f'did not find fee in {txn_hash.hex()}')
                continue
            model = UniswapV3Pricer(w3, pool, token0, token1, fee)

        try:
            slippage, expectation = infer_slippage_and_expectation_from_model(
                model,
                exact_input,
                amount_input if amount_input is not None else amount_output,
                amount_bound,
                zero_for_one,
                presumed_gen_number
            )
        except NotEnoughLiquidityException:
            # not sure what is up with this one so discard it
            continue

        if failed:
            # why did it fail? go figure out
            revert_reason = revert_reasons.get(txn_hash)
            fail_reason = get_err_msg(revert_reason) or 'UNKNOWN'
            slippage_protector_triggered = 'Too little received' in fail_reason or 'Too much requested' in fail_reason
        else:
            fail_reason = ''
            slippage_protector_triggered = False

        amount_in_usd = None
        amount_out_usd = None
        # try:
        #     amount_in_usd = None
        #     amount_out_usd = None
        #     # amount_in_usd = quote_token_usd(
        #     #     legacy_w3,
        #     #     web3.Web3.to_checksum_address(input_token),
        #     #     int(amount_input),
        #     #     observed_number,
        #     # ) if amount_input is not None else None
        #     # amount_out_usd = quote_token_usd(
        #     #     legacy_w3,
        #     #     web3.Web3.to_checksum_address(input_token),
        #     #     int(amount_output),
        #     #     observed_number,
        #     # ) if amount_output is not None else None
        # except token_price_oracle.exceptions.NoPriceData:
        #     # ignore
        #     continue

        ret.append(SlippageResult(
            block_number=observed_number,
            transaction_index=txn_idx, 
            transaction_hash='0x' + txn_hash.hex(),
            mempool_arrival_time=mempool_arrival_ts,
            queue_time_seconds=\
                typing.cast(
                    datetime.timedelta,
                    (observed_ts - mempool_arrival_ts)
                ).total_seconds(),
            sender = web3.Web3.to_checksum_address(from_address),
            recipient = web3.Web3.to_checksum_address(to_address),
            router = f'UniswapV3/{constants.UNISWAP_V2_ROUTER_2}',
            pool = web3.Web3.to_checksum_address(pool),
            token_in = web3.Web3.to_checksum_address(input_token),
            token_out = web3.Web3.to_checksum_address(output_token),
            exact_in = exact_input,
            amount_in = amount_input if amount_input is not None else None,
            amount_in_usd = amount_in_usd,
            amount_out_usd = amount_out_usd,
            amount_out = amount_output if amount_output is not None else None,
            boundary_amount = amount_bound,
            expected_amount = expectation,
            fail_reason = fail_reason,
            slippage = slippage,
            success = not failed,
            slippage_protector_triggered = slippage_protector_triggered
        ))

    return ret

def scrape_uniswap_universal_router(
    curr: psycopg.cursor_async.AsyncCursor,
    w3: web3.Web3,
    revert_reasons: typing.Dict[bytes, bytes],
    block_number: int,
    strict: bool
) -> typing.List[SlippageResult]:
    ret: typing.List[SlippageResult] = []

    curr.execute(
        '''
        SELECT
            u_txn.id,
            u_txn.txn_hash,
            (
                SELECT block_timestamps.number
                FROM block_timestamps
                WHERE block_timestamps.timestamp < arr.timestamp
                ORDER BY block_timestamps.timestamp DESC
                LIMIT 1
            ) number,
            (
                SELECT block_timestamps.timestamp
                FROM block_timestamps
                WHERE block_timestamps.number = u_txn.block_number
            ) block_ts,
            u_txn.block_number,
            u_txn.txn_idx,
            arr.timestamp,
            failed,
            exact_input,
            input_token,
            output_token,
            amount_input,
            amount_output,
            amount_bound,
            supports_fee_on_transfer,
            pool,
            pool_is_v2,
            from_address,
            to_address
        FROM uniswap_universal_router_txns u_txn
        JOIN transaction_timestamps arr ON concat('0x', encode(u_txn.txn_hash, 'hex')) = arr.transaction_hash
        WHERE %s = u_txn.block_number
            AND NOT EXISTS (
                SELECT FROM slippage_results sr
                WHERE sr.block_number = u_txn.block_number
                    AND sr.transaction_index = u_txn.txn_idx
            )
        ''',
        (
            block_number,
        )
    )

    rows = curr.fetchall()

    for i, row in enumerate(rows):
        (
            u_id,
            txn_hash,
            presumed_gen_number,
            observed_ts,
            observed_number,
            txn_idx,
            mempool_arrival_ts,
            failed,
            exact_input,
            input_token,
            output_token,
            amount_input,
            amount_output,
            amount_bound,
            supports_fee_on_transfer,
            pool,
            pool_is_uniswap_v2,
            from_address,
            to_address
        ) = row

        if mempool_arrival_ts - constants.START_DATETIME < datetime.timedelta(seconds=12):
            # skip the first 12 seconds of the window because we havent
            # seen a block yet
            if strict:
                ret.append(ARRIVED_BEFORE_WINDOW)
            continue

        if presumed_gen_number >= observed_number:
            if strict:
                ret.append(MEMPOOL_OBSERVED_TOO_LATE)
            continue

        if supports_fee_on_transfer:
            if strict:
                ret.append(USES_FEE_ON_TRANSFER)
            continue

        pool = '0x' + bytes(pool).hex()
        token0, token1 = sorted([input_token, output_token])
        zero_for_one = input_token == token0

        token0 = '0x' + token0.hex()
        token1 = '0x' + token1.hex()

        if pool_is_uniswap_v2:
            model = UniswapV2Pricer(w3, pool, token0, token1)
        else:
            # infer the fee
            for fee in [100, 500, 3_000, 10_000]:
                if pool.endswith(uniswap_v3_pool_address(token0, token1, fee).hex()):
                    # found it
                    break
            else:
                print(f'did not find fee in {txn_hash.hex()}')
                if strict:
                    ret.append('UNKNOWN_FEE')
                continue
            model = UniswapV3Pricer(w3, pool, token0, token1, fee)

        try:
            slippage, expectation = infer_slippage_and_expectation_from_model(
                model,
                exact_input,
                amount_input if amount_input is not None else amount_output,
                amount_bound,
                zero_for_one,
                presumed_gen_number
            )
        except NotEnoughLiquidityException:
            # not sure what is up with this one so discard it
            if strict:
                ret.append(NOT_ENOUGH_LIQUIDITY)
            continue

        if failed:
            # why did it fail? go figure out
            revert_reason = revert_reasons.get(txn_hash)
            fail_reason = get_err_msg(revert_reason) or 'UNKNOWN'
            slippage_protector_triggered = fail_reason in [constants.UNISWAP_UNIVERSAL_ERROR_TOO_LITTLE_RECEIVED, constants.UNISWAP_UNIVERSAL_ERROR_TOO_MUCH_REQUESTED]
        else:
            fail_reason = ''
            slippage_protector_triggered = False

        amount_in_usd = None
        amount_out_usd = None
        # try:
        #     amount_in_usd = None
        #     amount_out_usd = None
        #     # amount_in_usd = quote_token_usd(
        #     #     legacy_w3,
        #     #     web3.Web3.to_checksum_address(input_token),
        #     #     int(amount_input),
        #     #     observed_number,
        #     # ) if amount_input is not None else None
        #     # amount_out_usd = quote_token_usd(
        #     #     legacy_w3,
        #     #     web3.Web3.to_checksum_address(input_token),
        #     #     int(amount_output),
        #     #     observed_number,
        #     # ) if amount_output is not None else None
        # except token_price_oracle.exceptions.NoPriceData:
        #     # ignore
        #     continue

        ret.append(SlippageResult(
            block_number=observed_number,
            transaction_index=txn_idx, 
            transaction_hash='0x' + txn_hash.hex(),
            mempool_arrival_time=mempool_arrival_ts,
            queue_time_seconds=\
                typing.cast(
                    datetime.timedelta,
                    (observed_ts - mempool_arrival_ts)
                ).total_seconds(),
            sender = web3.Web3.to_checksum_address(from_address),
            recipient = web3.Web3.to_checksum_address(to_address),
            router = f'UniswapUniversal/{constants.UNISWAP_UNIVERSAL_ROUTER}',
            pool = web3.Web3.to_checksum_address(pool),
            token_in = web3.Web3.to_checksum_address(input_token),
            token_out = web3.Web3.to_checksum_address(output_token),
            exact_in = exact_input,
            amount_in = amount_input if amount_input is not None else None,
            amount_in_usd = amount_in_usd,
            amount_out_usd = amount_out_usd,
            amount_out = amount_output if amount_output is not None else None,
            boundary_amount = amount_bound,
            expected_amount = expectation,
            fail_reason = fail_reason,
            slippage = slippage,
            success = not failed,
            slippage_protector_triggered = slippage_protector_triggered
        ))

    return ret

def scrape_sushiswap(
    curr: psycopg.cursor_async.AsyncCursor,
    w3: web3.Web3,
    revert_reasons: typing.Dict[bytes, bytes],
    block_number: int,
    strict: bool
) -> typing.List[SlippageResult]:
    ret: typing.List[SlippageResult] = []

    curr.execute(
        '''
        SELECT
            sushi_txn.id,
            sushi_txn.txn_hash,
            (
                SELECT block_timestamps.number
                FROM block_timestamps
                WHERE block_timestamps.timestamp < arr.timestamp
                ORDER BY block_timestamps.timestamp DESC
                LIMIT 1
            ) number,
            (
                SELECT block_timestamps.timestamp
                FROM block_timestamps
                WHERE block_timestamps.number = sushi_txn.block_number
            ) block_ts,
            sushi_txn.block_number,
            sushi_txn.txn_idx,
            arr.timestamp,
            failed,
            exact_input,
            input_token,
            output_token,
            amount_input,
            amount_output,
            amount_bound,
            supports_fee_on_transfer,
            pool,
            from_address,
            to_address
        FROM sushiswap_router_txns sushi_txn
        JOIN transaction_timestamps arr ON concat('0x', encode(sushi_txn.txn_hash, 'hex')) = arr.transaction_hash
        WHERE %s = block_number
            AND NOT EXISTS (
                SELECT FROM slippage_results sr
                WHERE sr.block_number = sushi_txn.block_number
                    AND sr.transaction_index = sushi_txn.txn_idx
            )
        ''',
        (block_number,)
    )

    if curr.rowcount == 0:
        return []

    rows = curr.fetchall()

    for i, row in enumerate(rows):
        (
            sushi_id,
            txn_hash,
            presumed_gen_number,
            observed_ts,
            observed_number,
            txn_idx,
            mempool_arrival_ts,
            failed,
            exact_input,
            input_token,
            output_token,
            amount_input,
            amount_output,
            amount_bound,
            supports_fee_on_transfer,
            pool,
            from_address,
            to_address
        ) = row
        if mempool_arrival_ts - constants.START_DATETIME < datetime.timedelta(seconds=12):
            # skip the first 12 seconds of the window because we havent
            # seen a block yet
            if strict:
                ret.append(ARRIVED_BEFORE_WINDOW)
            continue

        if presumed_gen_number >= observed_number:
            if strict:
                ret.append(MEMPOOL_OBSERVED_TOO_LATE)
            continue

        if supports_fee_on_transfer:
            if strict:
                ret.append(USES_FEE_ON_TRANSFER)
            continue

        pool = '0x' + bytes(pool).hex()
        token0, token1 = sorted([input_token, output_token])
        zero_for_one = input_token == token0

        token0 = '0x' + token0.hex()
        token1 = '0x' + token1.hex()
        model = UniswapV2Pricer(w3, pool, token0, token1)

        slippage, expectation = infer_slippage_and_expectation_from_model(
            model,
            exact_input,
            amount_input if amount_input is not None else amount_output,
            amount_bound,
            zero_for_one,
            presumed_gen_number
        )

        if failed:
            # why did it fail? go figure out
            revert_reason = revert_reasons.get(txn_hash)
            fail_reason = get_err_msg(revert_reason) or 'UNKNOWN'
            slippage_protector_triggered = 'EXCESSIVE_INPUT_AMOUNT' in fail_reason or 'INSUFFICIENT_OUTPUT_AMOUNT' in fail_reason
        else:
            fail_reason = ''
            slippage_protector_triggered = False

        amount_in_usd = None
        amount_out_usd = None
        # try:
        #     amount_in_usd = None
        #     amount_out_usd = None
        #     # amount_in_usd = quote_token_usd(
        #     #     legacy_w3,
        #     #     web3.Web3.to_checksum_address(input_token),
        #     #     int(amount_input),
        #     #     observed_number,
        #     # ) if amount_input is not None else None
        #     # amount_out_usd = quote_token_usd(
        #     #     legacy_w3,
        #     #     web3.Web3.to_checksum_address(input_token),
        #     #     int(amount_output),
        #     #     observed_number,
        #     # ) if amount_output is not None else None
        # except token_price_oracle.exceptions.NoPriceData:
        #     # ignore
        #     continue

        ret.append(SlippageResult(
            block_number=observed_number,
            transaction_index=txn_idx, 
            transaction_hash='0x' + txn_hash.hex(),
            mempool_arrival_time=mempool_arrival_ts,
            queue_time_seconds=\
                typing.cast(
                    datetime.timedelta,
                    (observed_ts - mempool_arrival_ts)
                ).total_seconds(),
            sender = web3.Web3.to_checksum_address(from_address),
            recipient = web3.Web3.to_checksum_address(to_address),
            router = f'Sushiswap/{constants.SUSHISWAP_ROUTER}',
            pool = web3.Web3.to_checksum_address(pool),
            token_in = web3.Web3.to_checksum_address(input_token),
            token_out = web3.Web3.to_checksum_address(output_token),
            exact_in = exact_input,
            amount_in = amount_input if amount_input is not None else None,
            amount_in_usd = amount_in_usd,
            amount_out_usd = amount_out_usd,
            amount_out = amount_output if amount_output is not None else None,
            boundary_amount = amount_bound,
            expected_amount = expectation,
            fail_reason = fail_reason,
            slippage = slippage,
            success = not failed,
            slippage_protector_triggered = slippage_protector_triggered
        ))

    return ret

def infer_slippage_from_model(
        model: typing.Union[UniswapV2Pricer, UniswapV3Pricer],
        exact_input: bool,
        amount_specified: int,
        amount_bound: int,
        zero_for_one: bool,
        block_number: int
):
    return infer_slippage_and_expectation_from_model(
        model,
        exact_input,
        amount_specified,
        amount_bound,
        zero_for_one,
        block_number
    )[0]


def infer_slippage_and_expectation_from_model(
        model: typing.Union[UniswapV2Pricer, UniswapV3Pricer],
        exact_input: bool,
        amount_specified: int,
        amount_bound: int,
        zero_for_one: bool,
        block_number: int
) -> typing.Tuple[float, int]:
    # NOTE TO SELF: this function is deceptive! amount_input and amount_output are not necessarily
    # as they are named.
    if exact_input:
        amount_input = int(amount_specified)
        if zero_for_one:
            amount_output = model.exact_token0_to_token1(amount_input, block_number)
        else:
            amount_output = model.exact_token1_to_token0(amount_input, block_number)
    else:
        amount_output = int(amount_specified)
        if zero_for_one:
            amount_input = model.token0_in_for_exact_token1_out(amount_output, block_number)
        else:
            amount_input = model.token1_in_for_exact_token0_out(amount_output, block_number)


    slippage = infer_slippage_from_amounts(
        exact_input,
        amount_input,
        amount_output,
        amount_bound
    )

    expectation = amount_output if exact_input else amount_input

    return slippage, expectation


def infer_slippage_from_amounts(
        exact_input: bool,
        amount_input: int,
        amount_output: int,
        amount_bound: int,
) -> float:
    assert amount_bound >= 0

    if exact_input:
        if amount_output == 0:
            slippage = float('nan')
        else:
            # Bound bound to 100% slippage (uniswap interface limit is 50)
            # in that situation:
            # amount_bound = amount_out / 2
            # if the bound is less than that, just set it to 100%
            if amount_bound < amount_output / 2:
                slippage = 100.0
            else:
                slippage = ((float(amount_output) / float(amount_bound)) - 1) * 100
            # print(f'(1 - (float({amount_bound}) / float({amount_out}))) * 100')
            # slippage = (1 - (float(amount_bound) / float(amount_out))) * 100
    else:
        if amount_output == 0:
            slippage = float('nan')
        else:
            slippage = ((float(amount_bound) / float(amount_output)) - 1) * 100

    return slippage


def get_err_msg(returndata) -> typing.Optional[str]:
    if returndata is None:
        return None

    if isinstance(returndata, str):
        breturndata = bytes.fromhex(returndata)
    else:
        breturndata = bytes(returndata)
    if len(breturndata) == 4:
        return '0x' + breturndata.hex()

    if len(breturndata) < (4 + 32 + 32):
        return None

    offset = int.from_bytes(breturndata[4:32+4], byteorder='big', signed=False)
    length = int.from_bytes(breturndata[32+4:32+32+4], byteorder='big', signed=False)

    if len(breturndata) < offset+4+32+length:
        return None

    reason = breturndata[offset+4+32:offset+4+32+length].decode('utf8')
    return reason


def quote_token_usd(
        w3: web3.Web3,
        token: str,
        qty: int,
        block_number: int
    ) -> decimal.Decimal:
    price = token_price_oracle.price_token_dollars(
        w3, token, block_identifier=block_number
    )
    decimals = get_token_decimals(w3, token)
    spot = price / pow(10, 18 - decimals)
    return qty * spot

_decimal_cache = {}
def get_token_decimals(w3: web3.Web3, token: str):
    if token not in _decimal_cache:
        with open('abis/erc20.abi.json') as fin:
            abi = ujson.load(fin)
        c = w3.eth.contract(
            address=web3.Web3.to_checksum_address(token),
            abi = abi
        )
        d = c.functions.decimals().call()
        _decimal_cache[token] = d
    return _decimal_cache[token]


def _get_revert_reason(w3: web3.Web3, txn_hash: bytes) -> typing.Optional[bytes]:
    trace = w3.provider.make_request('trace_transaction', ['0x' + bytes(txn_hash).hex()])
    if 'error' in trace:
        return None
    
    if 'result' not in trace:
        return None
    
    result = trace['result']

    if len(result) < 1:
        return None
    
    if 'result' not in result[0]:
        return None
    
    result = result[0]['result']

    if 'output' not in result:
        return None

    output = result['output']
    
    # parse and return as bytes
    return bytes.fromhex(output[2:])


def query_traces(dir: str, queries: typing.Iterable[typing.Tuple[int, int]]) -> typing.List[typing.Tuple[int, int, typing.Dict]]:
    """
    Extract several traces of the given (block number, index) pairs from the storage data directory.

    raises KeyError when trace is not found
    """

    queries_remaining = collections.deque(sorted(queries))
    ret = []

    while len(queries_remaining) > 0:
        # print(f'Have {len(queries_remaining)} queries remaining')
        starting_block_number, _ = queries_remaining[0]

        for level in [1, 10, 100, 1000]:
            bin_ = starting_block_number // level * level
            target_fname = os.path.join(dir, str(level), str(bin_) + '.tar.xz')
            if os.path.exists(target_fname):
                break
        else:
            raise KeyError(f'Could not find block {starting_block_number:,}')

        print(f'Inspecting {target_fname}')

        target_bin = bin_
        needed_blocks = collections.defaultdict(list)
        while len(queries_remaining) > 0:
            if queries_remaining[0][0] // level * level == target_bin:
                bn, idx = queries_remaining.popleft()
                needed_blocks[bn].append(idx)
            else:
                break

        t_start = time.time()
        with open(target_fname, mode='rb') as fin:
            decomp_tarball = lzma.decompress(fin.read())

        tin = tarfile.TarFile(fileobj=io.BytesIO(decomp_tarball))

        block_to_member: typing.Dict[int, tarfile.TarInfo] = {}
        for member in tin.getmembers():
            bn = int(member.name.split('.')[0])
            if bn in needed_blocks:
                block_to_member[bn] = member

        for i, block_number in enumerate(sorted(needed_blocks.keys())):
            assert len(ret) < len(queries)

            member = block_to_member[block_number]
            cpy = decomp_tarball[member.offset_data:member.offset_data + member.size]
            blob = ujson.loads(cpy)

            needed_txns = set(needed_blocks[block_number])
            for txn in blob:
                txn_idx = txn['i']
                assert isinstance(txn_idx, int)
                if txn_idx in needed_txns:
                    ret.append((block_number, txn_idx, txn))
                    needed_txns.remove(txn_idx)
            assert len(needed_txns) == 0

    return ret

if __name__ == '__main__':
    main()
