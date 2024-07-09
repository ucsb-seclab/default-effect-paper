import dotenv
import psycopg
import decimal
import typing
import datetime
import functools
import multiprocessing
import web3
import web3.types
import token_price_oracle
import collections

import constants
from db import connect_db
import utils

N_WORKERS = 24
BATCH_SIZE_BLOCKS = 10

def main():
    dotenv.load_dotenv()

    db = get_db()
    cur = db.cursor()

    all_swaps = []

    print('starting scrape...')
    with multiprocessing.Pool(N_WORKERS, initializer=init_worker) as pool:
        results = pool.imap_unordered(get_swaps, range(constants.START_BLOCK, constants.END_BLOCK, BATCH_SIZE_BLOCKS))
        n_batches = (constants.END_BLOCK - constants.START_BLOCK) // BATCH_SIZE_BLOCKS
        marks = collections.deque(maxlen=N_WORKERS * 5)
        marks.append((datetime.datetime.now(), 0))
        for i, result in enumerate(results):
            marks.append((datetime.datetime.now(), i+1))
            speed = (marks[-1][1] - marks[0][1]) / (marks[-1][0] - marks[0][0]).total_seconds()
            eta = (n_batches - i - 1) / speed
            eta_str = str(datetime.timedelta(seconds=eta))
            print(f'ETA: {eta_str}')
            print(f'Batch {i+1}/{n_batches}')
            all_swaps.extend(result)
    
    print('committing to database...')

    # we end up with duplicates, I think I'm off-by-one in the scrape block range .. that's okay, just remove the dupes (this is safe)
    seen_swaps = set()

    with cur.copy(
        '''
        COPY all_swaps (
            exchange_type, timestamp, timestamp_unixtime,
            block_number, txn_idx, txn_hash, block_log_index,
            seen_in_mempool, pool, sender, recipient, origin,
            token0_symbol, token1_symbol, token0_address, token1_address,
            amount0, amount1, amount_usd
        ) FROM STDIN
        '''
    ) as cpy:
        for swap in all_swaps:
            k = (swap.block_number, swap.block_log_index)
            if k in seen_swaps:
                continue
            seen_swaps.add(k)
            cpy.write_row((
                swap.exchange_type,
                swap.timestamp,
                swap.timestamp_unixtime,
                swap.block_number,
                swap.txn_idx,
                swap.txn_hash,
                swap.block_log_index,
                swap.seen_in_mempool,
                swap.pool,
                swap.sender,
                swap.recipient,
                swap.origin,
                swap.token0_symbol,
                swap.token1_symbol,
                swap.token0_address,
                swap.token1_address,
                swap.amount0,
                swap.amount1,
                swap.amount_usd
            ))
    
    db.commit()

    print('Inserted swap rows')

    # sometimes we have multiple swaps with the same txn_hash and router swap action,
    # this can occur, for example, when the token's transfer() method also calls uniswap
    # See: 
    # 0xd512484075b9f523cc91a4c4dc7cebb9f1a688aaa17a60727b2a7984c34a7a5c
    # 0xfed059a1f42ff615fab253fe71a69e75456d6e7e9b7bad0b440dec3d9df3784e
    # 0x79fa7c5e8c7eeaf0c3b576263acf377cc9c18584121f9d44de3847d20c274597
    # 0xc11e054155a53727417027c7a78c78bf40abb36ae77b49b732ae399f190857ad
    # 0x5077d7d6582ad2c3e1273e04acc1f7deca0367b7cf321d4e5cabdced9672922d
    #
    # To avoid this, we will first find out all the transactions with more than one swap,
    # then we will ignore those for filling slippage
    cur.execute(
        '''
        CREATE TEMP TABLE tmp_dupe_txns AS
            SELECT txn_hash
            FROM (
                SELECT txn_hash, count(*) cnt
                FROM all_swaps
                JOIN slippage_results ON slippage_results.transaction_hash = all_swaps.txn_hash
                GROUP BY txn_hash
            ) x
            WHERE cnt > 1;

        CREATE INDEX tmp_dupe_txns_txn_hash ON tmp_dupe_txns (txn_hash);
        '''
    )
    
    cur.execute('SELECT COUNT(*) FROM tmp_dupe_txns;')
    n_dupe_txns = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM slippage_results;')
    tot_slippage_inferred = cur.fetchone()[0]
    print(f'Have {n_dupe_txns:,} ({n_dupe_txns / tot_slippage_inferred:.2%}) slippage-inferred transactions with multiple swaps')

    # now we can fill in the slippage for the rest
    cur.execute(
        '''
        UPDATE all_swaps als
        SET
            slippage = sr.slippage,
            slippage_protector_triggered = sr.slippage_protector_triggered,
            success = sr.success,
            fail_reason = sr.fail_reason
        FROM slippage_results sr
        WHERE sr.transaction_hash NOT IN (
            SELECT txn_hash
            FROM tmp_dupe_txns
        ) AND sr.transaction_hash = als.txn_hash;
        '''
    )
    print(f'Filled in slippage for {cur.rowcount:,} transactions')

    db.commit()

    # Fill in gas used for all router transactions

    # get all transaction hashes we need to examine
    cur.execute(
        '''
        SELECT transaction_hash
        FROM slippage_results
        '''
    )
    txn_hashes = [row[0] for row in cur.fetchall()]
    
    print(f'Have {len(txn_hashes):,} transaction hashes to examine')

    print(f'Filling gas usage...')
    gas_used_records = []
    time_marks = collections.deque(maxlen=100)
    time_marks.append((datetime.datetime.now(), 0))
    with multiprocessing.Pool(N_WORKERS) as pool:
        results = pool.imap_unordered(get_gas_used, txn_hashes)
        for i, result in enumerate(results):
            time_marks.append((datetime.datetime.now(), i+1))
            gas_used_records.append(result)
            if i % 1000 == 0:
                speed = (time_marks[-1][1] - time_marks[0][1]) / (time_marks[-1][0] - time_marks[0][0]).total_seconds()
                eta = (len(txn_hashes) - i - 1) / speed
                eta_str = str(datetime.timedelta(seconds=eta))
                print(f'Processed {i:,} / {len(txn_hashes):,} ({i / len(txn_hashes):.2%}) - ETA: {eta_str}')
    
    print(f'Have {len(gas_used_records):,} gas used records')
    # fill in the database
    # do this by first filling a temporary table, then copying into the real table
    cur.execute(
        'CREATE TEMP TABLE tmp_gas_used (txn_hash TEXT, gas_used_eth DECIMAL, gas_used_usd DECIMAL);'
    )
    with cur.copy(
        '''
        COPY tmp_gas_used (txn_hash, gas_used_eth, gas_used_usd) FROM STDIN
        '''
    ) as cpy:
        for record in gas_used_records:
            cpy.write_row(record)
    
    cur.execute(
        '''
        UPDATE slippage_results sr
        SET
            gas_used_eth = tgu.gas_used_eth,
            gas_used_usd = tgu.gas_used_usd
        FROM tmp_gas_used tgu
        WHERE sr.transaction_hash = tgu.txn_hash;
        '''
    )
    print(f'Filled in gas used for {cur.rowcount:,} transactions in slippage_results')

    cur.execute(
        '''
        UPDATE all_swaps als
        SET
            gas_used_eth = sr.gas_used_eth,
            gas_used_usd = sr.gas_used_usd
        FROM slippage_results sr
        WHERE als.txn_hash = sr.transaction_hash;
        '''
    )
    print(f'Filled in gas used for {cur.rowcount:,} transactions')
    db.commit()

    # preload the token prices (this is slow)
    cached_token_prices_fname = '.cache/token_prices.pickle'
    token_prices_by_block: typing.Dict[typing.Tuple[str, int], decimal.Decimal] = {}

    try:
        import pickle
        with open(cached_token_prices_fname, 'rb') as fin:
            token_prices_by_block = pickle.load(fin)
            print(f'Loaded {len(token_prices_by_block):,} token prices from {cached_token_prices_fname}')
    except FileNotFoundError:
        pass

    # check if any token prices need to be filled in
    cur.execute(
        '''
        SELECT token_in, token_out, exact_in, block_number
        FROM slippage_results
        WHERE success = FALSE
        '''
    )
    prices_needed = set()
    n_rows = cur.rowcount
    for i, (t_in, t_out, exact_in, block_number) in enumerate(cur.fetchall()):
        if i % 100 == 0:
            print(f'Processed {i:,} / {n_rows:,} ({i / n_rows:.2%})')
        try:
            token_in = get_token_by_address(cur, t_in)
            token_out = get_token_by_address(cur, t_out)
        except ValueError as e:
            if 'not found' in str(e):
                continue

        token = token_in if exact_in else token_out
        if (token.address, block_number * 100 // 100) not in token_prices_by_block:
            prices_needed.add((token, block_number * 100 // 100))

    if len(prices_needed) > 0:
        print(f'Need {len(prices_needed):,} token prices')
        token_prices_by_block: typing.Dict[typing.Tuple[str, int], decimal.Decimal] = {}
        time_marks = collections.deque(maxlen=500)
        time_marks.append((datetime.datetime.now(), 0))
        with multiprocessing.Pool(N_WORKERS) as pool:
            results = pool.imap_unordered(_get_token_price_usd, prices_needed)
            for i, result in enumerate(results):
                token, block, price = result
                token_prices_by_block[(token.address, block)] = price
                time_marks.append((datetime.datetime.now(), i+1))
                if i % 100 == 0:
                    speed = (time_marks[-1][1] - time_marks[0][1]) / (time_marks[-1][0] - time_marks[0][0]).total_seconds()
                    eta = (len(prices_needed) - i - 1) / speed
                    eta_str = str(datetime.timedelta(seconds=eta))
                    print(f'Processed {i:,} / {len(prices_needed):,} ({i / len(prices_needed):.2%}) token prices - ETA: {eta_str}')

        print(f'Dumping token prices to {cached_token_prices_fname}...')
        import pickle
        with open(cached_token_prices_fname, 'wb') as fout:
            pickle.dump(token_prices_by_block, fout)

    print('importing failed transactions into all_swaps table...')

    # remove all failed transactions from all_swaps table
    cur.execute(
        '''
        DELETE FROM all_swaps
        USING slippage_results
        WHERE all_swaps.txn_hash = slippage_results.transaction_hash
            AND slippage_results.success = FALSE
        '''
    )
    print(f'Removed {cur.rowcount:,} /failed transactions/ from all_swaps table')

    # we need the receipts for all failed transactions so we can get the gas used;
    # do this in bulk to save time
    cur.execute(
        '''
        SELECT transaction_hash
        FROM slippage_results
        WHERE success = FALSE
        '''
    )
    txns_needed = set(txn_hash for (txn_hash,) in cur.fetchall())
    print(f'Need {len(txns_needed):,} transaction receipts')
    txn_receipts = {
        '0x' + bytes(txn_hash).hex(): receipt
        for txn_hash, receipt in _bulk_get_transaction_receipt(list(txns_needed)).items()
    }

    # need to add all failed transactions to the slippage_results table
    cur.execute(
        '''
        SELECT
            block_number, transaction_index, transaction_hash,
            sender, recipient, router, pool, token_in, token_out,
            exact_in, amount_in, amount_out, fail_reason, slippage_protector_triggered,
            slippage
        FROM slippage_results
        WHERE success = FALSE
        '''
    )
    n_rows = cur.rowcount
    for i, row in enumerate(cur.fetchall()):
        if i % 100 == 0:
            print(f'Processing {i:,} / {n_rows:,} ({i / n_rows:.2%}) - filling in failed transactions')
        (
            block_number, transaction_index, transaction_hash,
            sender, recipient, router, pool, token_in, token_out,
            exact_in, amount_in, amount_out, fail_reason, slippage_protector_triggered,
            slippage
        ) = row

        token0, token1 = sorted([token_in, token_out], key=lambda t: bytes.fromhex(t[2:]))
        try:
            token0 = get_token_by_address(cur, token0)
            token1 = get_token_by_address(cur, token1)
        except ValueError as e:
            if 'not found' in str(e):
                continue

        zero_for_one = token0.address == token_in

        amount0 = amount_in if zero_for_one else (-amount_out if amount_out is not None else None)
        amount1 = (-amount_out if amount_out is not None else None) if zero_for_one else amount_in

        block_to_use_for_pricing = block_number // 100 * 100
        token_to_use_for_pricing = token0 if amount0 is not None else token1
        amount_to_use_for_pricing = amount0 if amount0 is not None else amount1
        price = token_prices_by_block.get((token_to_use_for_pricing.address, block_to_use_for_pricing), None)
        if price is None:
            amount_usd = None
        else:
            amount_usd = price * amount_to_use_for_pricing
        
        block = get_block(block_number)
        timestamp = datetime.datetime.fromtimestamp(block['timestamp'])
        timestamp_unixtime = block['timestamp']

        if 'uniswap' in router.lower():
            # attempt to infer whether this pool is uniswap v2 or v3
            if utils.is_uniswap_v2(token0.address, token1.address, pool):
                exchange_type = 'UniswapV2'
            else:
                assert utils.is_uniswap_v3(token0.address, token1.address, pool), f'Unknown exchange type for pool {pool}'
                exchange_type = 'UniswapV3'
        elif 'sushi' in router.lower():
            exchange_type = 'SushiSwap'

        gas_used = txn_receipts[transaction_hash]['gasUsed']
        effective_gas_price = txn_receipts[transaction_hash]['effectiveGasPrice']

        gas_used_eth = gas_used * effective_gas_price
        gas_used_usd = decimal.Decimal(gas_used_eth) / (10**18) * get_eth_price_usd(block_number // 100 * 100)

        cur.execute(
            '''
            INSERT INTO all_swaps (
                exchange_type, timestamp, timestamp_unixtime,
                block_number, txn_idx, txn_hash, block_log_index,
                seen_in_mempool, pool, sender, recipient, origin,
                token0_symbol, token1_symbol, token0_address, token1_address,
                slippage, slippage_protector_triggered, success,
                amount0, amount1, amount_usd, gas_used_eth, gas_used_usd
            ) VALUES (
                %(exchange_type)s, %(timestamp)s, %(timestamp_unixtime)s,
                %(block_number)s, %(txn_idx)s, %(txn_hash)s, %(block_log_index)s,
                %(seen_in_mempool)s, %(pool)s, %(sender)s, %(recipient)s, %(origin)s,
                %(token0_symbol)s, %(token1_symbol)s, %(token0_address)s, %(token1_address)s,
                %(slippage)s, %(slippage_protector_triggered)s, FALSE,
                %(amount0)s, %(amount1)s, %(amount_usd)s, %(gas_used_eth)s, %(gas_used_usd)s
            )
            ''',
            {
                'exchange_type': exchange_type,
                'timestamp': timestamp,
                'timestamp_unixtime': timestamp_unixtime,
                'block_number': block_number,
                'txn_idx': transaction_index,
                'txn_hash': transaction_hash,
                'block_log_index': -1,
                'seen_in_mempool': True,
                'pool': pool,
                'sender': sender,
                'recipient': recipient,
                'origin': sender,
                'token0_symbol': token0.symbol,
                'token1_symbol': token1.symbol,
                'token0_address': token0.address,
                'token1_address': token1.address,
                'slippage': slippage,
                'slippage_protector_triggered': slippage_protector_triggered,
                'amount0': amount0,
                'amount1': amount1,
                'amount_usd': amount_usd,
                'gas_used_eth': gas_used_eth,
                'gas_used_usd': gas_used_usd,
            }
        )

    db.commit()

    print('done')


class Swap(typing.NamedTuple):
    exchange_type: str
    timestamp: datetime.datetime
    timestamp_unixtime: int
    block_number: int
    txn_idx: int
    txn_hash: str
    block_log_index: int
    seen_in_mempool: bool
    pool: str
    sender: str
    recipient: str
    origin: str
    token0_symbol: str
    token1_symbol: str
    token0_address: str
    token1_address: str
    amount0: decimal.Decimal
    amount1: decimal.Decimal
    amount_usd: decimal.Decimal

def get_swaps(start_block_number: int, n_blocks: int = BATCH_SIZE_BLOCKS) -> typing.List[Swap]:
    topics = [
        '0x' + constants.UNISWAP_V2_SWAP_TOPIC().hex(),
        '0x' + constants.UNISWAP_V3_SWAP_TOPIC().hex(),
    ]

    db = get_db()
    cur = db.cursor()

    w3 = get_w3()
    logs: typing.List[web3.types.LogReceipt] = w3.eth.get_logs({
        'fromBlock': start_block_number,
        'toBlock': min(constants.END_BLOCK, start_block_number + n_blocks),
        'topics': [topics],
    })
    
    result = []


    for log in logs:
        exc = None
        exc_type = None
        if log['topics'][0] == constants.UNISWAP_V2_SWAP_TOPIC():
            print('v2')
            exc = get_uniswap_v2_exchange(cur, log['address'])
            if exc is not None:
                exc_type = 'UniswapV2'
            else:
                exc = get_sushiswap_exchange(cur, log['address'])
                if exc is not None:
                    exc_type = 'SushiSwap'
            
            if exc_type is None:
                # unknown exchange, skip
                continue

            # parse log
            parsedLog = constants._univ2().events.Swap().process_log(log)
            
            # determine zero-for-one
            if parsedLog['args']['amount0In'] == 0:
                if parsedLog['args']['amount1In'] == 0:
                    # zero-swap, skip
                    continue
                zero_for_one = False
            else:
                if parsedLog['args']['amount1In'] == 0:
                    zero_for_one = True
                else:
                    # both non-zero, skip
                    print('both nonzero')
                    continue

            # determine amount0 and amount1
            if zero_for_one:
                amount0 = parsedLog['args']['amount0In']
                amount1 = -parsedLog['args']['amount1Out']
            else:
                amount0 = -parsedLog['args']['amount0Out']
                amount1 = parsedLog['args']['amount1In']

            sender = parsedLog['args']['sender']
            recipient = parsedLog['args']['to']

        elif log['topics'][0] == constants.UNISWAP_V3_SWAP_TOPIC():
            exc = get_uniswap_v3_exchange(cur, log['address'])
            if exc is None:
                # unknown exchange, skip
                continue
            exc_type = 'UniswapV3'
            
            # parse log
            parsedLog = constants._univ3().events.Swap().process_log(log)

            # determine zero-for-one
            if parsedLog['args']['amount0'] > 0:
                if parsedLog['args']['amount1'] > 0:
                    # both non-zero, skip
                    continue
                zero_for_one = True
            else:
                if parsedLog['args']['amount1'] > 0:
                    zero_for_one = False
                else:
                    # zero-swap, skip
                    continue
            
            # determine amount0 and amount1
            amount0 = parsedLog['args']['amount0']
            amount1 = parsedLog['args']['amount1']

            sender = parsedLog['args']['sender']
            recipient = parsedLog['args']['recipient']

        decimal_amount0 = decimal.Decimal(amount0) / (10**exc[1].decimals)
        decimal_amount1 = decimal.Decimal(amount1) / (10**exc[2].decimals)

        block_to_use_for_pricing = parsedLog['blockNumber'] // 100 * 100
        try:
            amount0_usd = get_token_price_usd(exc[1], block_to_use_for_pricing) * decimal_amount0
        except token_price_oracle.exceptions.NoPriceData:
            amount0_usd = None
        try:
            amount1_usd = get_token_price_usd(exc[2], block_to_use_for_pricing) * decimal_amount1
        except token_price_oracle.exceptions.NoPriceData:
            amount1_usd = None

        block = get_block(log['blockNumber'])
        txn = get_transaction(log['transactionHash'].hex())

        # did we see this in the mempool?
        cur.execute(
            '''
            SELECT EXISTS(
                SELECT 1
                FROM transaction_timestamps
                WHERE transaction_hash = %s
            )
            ''',
            (log['transactionHash'].hex(),)
        )
        seen_in_mempool = cur.fetchone()[0]

        swap = Swap(
            exchange_type = exc_type,
            timestamp = datetime.datetime.fromtimestamp(block['timestamp']),
            timestamp_unixtime = block['timestamp'],
            block_number = log['blockNumber'],
            txn_idx = log['transactionIndex'],
            txn_hash = log['transactionHash'].hex(),
            block_log_index = log['logIndex'],
            seen_in_mempool = seen_in_mempool,
            pool = exc[0],
            sender = sender,
            recipient = recipient,
            origin = txn['from'],
            token0_symbol = exc[1].symbol,
            token1_symbol = exc[2].symbol,
            token0_address = exc[1].address,
            token1_address = exc[2].address,
            amount0 = decimal_amount0,
            amount1 = decimal_amount1,
            amount_usd = amount0_usd if zero_for_one else amount1_usd,
        )

        result.append(swap)

        # # print summary
        # print(f'Swap {exc_type}: {exc[0]}')
        # print(f'  block: {parsedLog["blockNumber"]:,}')
        # print(f'  txn: {parsedLog["transactionIndex"]:,}')
        # print(f'  txn_hash: {parsedLog["transactionHash"].hex()}')
        # print(f'  amount0: {decimal_amount0}')
        # print(f'  amount1: {decimal_amount1}')
        # print(f'  amount0_usd: {amount0_usd}')
        # print(f'  amount1_usd: {amount1_usd}')
        # print(f'  sender: {sender}')
        # print(f'  recipient: {recipient}')

    return result

def _bulk_get_transaction_receipt(
    txn_hashes: typing.List[bytes],
) -> typing.Dict[bytes, web3.types.TxReceipt]:
    N_CPUS = min(multiprocessing.cpu_count(), len(txn_hashes) // 100 + 1)

    print(f"getting receipts using {N_CPUS} CPUs")
    with multiprocessing.Pool(N_CPUS, initializer=_init_worker) as pool:
        receipts_results = pool.imap_unordered(
            _get_transaction_receipt, txn_hashes, chunksize=100
        )

        receipts = {}
        for i, receipt in enumerate(receipts_results):
            if i % 100 == 0:
                print(
                    f"Got receipt {i:,} / {len(txn_hashes):,} ({i / len(txn_hashes) * 100:.2f}%)"
                )
            receipts[receipt["transactionHash"]] = receipt

    print(f"Got {len(receipts):,} receipts")
    return receipts

def _init_worker():
    web3.WebsocketProvider._loop = None
    get_w3.cache_clear()

def _get_transaction_receipt(tx_hash: bytes) -> web3.types.TxReceipt:
    w3 = get_w3()
    return w3.eth.get_transaction_receipt(tx_hash)

def get_gas_used(txn_hash: str) -> typing.Tuple[str, decimal.Decimal, decimal.Decimal]:
    w3 = get_w3()
    receipt = w3.eth.get_transaction_receipt(txn_hash)
    gas_used = receipt['gasUsed']
    gas_used_eth = decimal.Decimal(gas_used * receipt['effectiveGasPrice']) / (10**18)
    gas_used_usd = gas_used_eth * get_eth_price_usd(receipt['blockNumber'] // 100 * 100)

    return txn_hash, gas_used_eth, gas_used_usd

_uv2_exchange_cache = {}
def get_uniswap_v2_exchange(cur: psycopg.Cursor, address: str) -> typing.Optional[typing.Tuple[str, 'Token', 'Token']]:
    if address not in _uv2_exchange_cache:
        cur.execute(
            '''
            SELECT token0, token1
            FROM uniswap_v2_exchanges
            WHERE address = %s
            ''',
            (bytes.fromhex(address[2:]),)
        )
        row = cur.fetchone()
        if row is None:
            return None
        token0_id = row[0]
        token1_id = row[1]
        token0 = get_token_by_id(cur, token0_id)
        token1 = get_token_by_id(cur, token1_id)
        _uv2_exchange_cache[address] = (address, token0, token1)
    return _uv2_exchange_cache[address]

_uv3_exchange_cache = {}
def get_uniswap_v3_exchange(cur: psycopg.Cursor, address: str) -> typing.Optional[typing.Tuple[str, 'Token', 'Token']]:
    if address not in _uv3_exchange_cache:
        cur.execute(
            '''
            SELECT token0, token1
            FROM uniswap_v3_exchanges
            WHERE address = %s
            ''',
            (bytes.fromhex(address[2:]),)
        )
        assert cur.rowcount <= 1
        row = cur.fetchone()
        if row is None:
            return None
        token0_id = row[0]
        token1_id = row[1]
        token0 = get_token_by_id(cur, token0_id)
        token1 = get_token_by_id(cur, token1_id)
        _uv3_exchange_cache[address] = (address, token0, token1)
    return _uv3_exchange_cache[address]

_sushi_exchange_cache = {}
def get_sushiswap_exchange(cur: psycopg.Cursor, address: str) -> typing.Optional[typing.Tuple[str, 'Token', 'Token']]:
    if address not in _sushi_exchange_cache:
        cur.execute(
            '''
            SELECT token0, token1
            FROM sushiswap_exchanges
            WHERE address = %s
            ''',
            (bytes.fromhex(address[2:]),)
        )
        row = cur.fetchone()
        if row is None:
            return None
        token0_id = row[0]
        token1_id = row[1]
        token0 = get_token_by_id(cur, token0_id)
        token1 = get_token_by_id(cur, token1_id)
        _sushi_exchange_cache[address] = (address, token0, token1)
    return _sushi_exchange_cache[address]

class Token(typing.NamedTuple):
    id: int
    address: str
    symbol: str
    name: str
    decimals: int


_token_cache = {}
def get_token_by_id(cur: psycopg.Cursor, id_: int) -> Token:
    if id_ not in _token_cache:
        cur.execute(
            '''
            SELECT address, symbol, name, decimals
            FROM tokens
            WHERE id = %s
            ''',
            (id_,)
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError(f'Token {id_} not found')
        try:
            _token_cache[id_] = Token(
                id_,
                web3.Web3.to_checksum_address(row[0]),
                row[1],
                row[2],
                row[3]
            )
        except:
            print(row)
            raise
    return _token_cache[id_]

_token_cache = {}
def get_token_by_address(cur: psycopg.Cursor, address: str) -> Token:
    if address not in _token_cache:
        cur.execute(
            '''
            SELECT id, symbol, name, decimals
            FROM tokens
            WHERE address = %s
            ''',
            (bytes.fromhex(address[2:]),)
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError(f'Token {address} not found')
        _token_cache[address] = Token(
            row[0],
            web3.Web3.to_checksum_address(address),
            row[1],
            row[2],
            row[3]
        )
    
    return _token_cache[address]

@functools.lru_cache(maxsize=1024*10)
def get_transaction(txn_hash: str) -> web3.types.TxData:
    return get_w3().eth.get_transaction(txn_hash)

@functools.lru_cache(maxsize=1024*1024)
def get_eth_price_usd(block_number: int) -> decimal.Decimal:
    price = token_price_oracle.price_eth_dollars(
        get_w3(),
        block_number
    )
    return price

# used for imap_unordered
def _get_token_price_usd(arg: typing.Tuple[Token, int]) -> typing.Tuple[Token, int, decimal.Decimal]:
    try:
        return arg[0], arg[1], get_token_price_usd(arg[0], arg[1])
    except token_price_oracle.exceptions.NoPriceData:
        return arg[0], arg[1], None

def get_token_price_usd(token: Token, block_number: int) -> decimal.Decimal:
    price = token_price_oracle.price_token_dollars(
        get_w3(),
        token.address,
        block_number
    )
    price = price / (pow(decimal.Decimal(10), (18 - token.decimals)))
    return price

@functools.lru_cache(maxsize=1024)
def get_block(block_number: int) -> web3.types.BlockData:
    return get_w3().eth.get_block(block_number)

@functools.cache
def get_db() -> psycopg.Connection:
    return connect_db()

@functools.cache
def get_w3() -> web3.Web3:
    return utils.connect_web3()

def init_worker():
    get_w3.cache_clear()
    get_db.cache_clear()

if __name__ == '__main__':
    main()
