"""
Emits stats about the dataset
"""

import pickle
import db
import dotenv
import utils
import datetime
import tabulate
import scipy.stats
import numpy as np
import collections
import utils
import web3
import functools
import multiprocessing
import os
import time

import constants

dotenv.load_dotenv()
print('Getting stats...')

conn = db.connect_db()
cur = conn.cursor()

if False:
    # get count of all transactions in range
    @functools.cache
    def get_w3() -> web3.Web3:
        return utils.connect_web3()

    def get_txn_count_in_block(block_number: int) -> int:
        w3 = get_w3()
        return len(w3.eth.get_block(block_number).transactions)

    tot_n_results = 0

    with multiprocessing.Pool(20) as pool:
        results = pool.imap_unordered(get_txn_count_in_block, range(constants.START_BLOCK, constants.END_BLOCK + 1))
        n_blocks = constants.END_BLOCK - constants.START_BLOCK + 1
        for i, result in enumerate(results):
            if i % 100 == 0:
                print(f'{i:,} / {n_blocks:,} ({i / n_blocks * 100:.2f}%)')
            
            tot_n_results += result
    
    print(f'Total number of transactions in range: {tot_n_results:,}')
    
    exit()

fname_count_mempool_txns = '.cache/process_data_count_mempool_txns.pickle'
try:
    with open(fname_count_mempool_txns, 'rb') as f:
        num_mempool_txns = pickle.load(f)
except FileNotFoundError:
    # get the number of transactions in the study period that we saw in the mempool
    cur.execute(
        '''
        SELECT COUNT(*)
        FROM transaction_timestamps
        WHERE timestamp >= %s
            AND timestamp < %s
        ''',
        (
            datetime.datetime(
                year=2023,
                month=3,
                day=1,
            ),
            datetime.datetime(
                year=2023,
                month=4,
                day=1,
            ),
        )
    )
    num_mempool_txns = cur.fetchone()[0]

    with open(fname_count_mempool_txns, 'wb') as f:
        pickle.dump(num_mempool_txns, f)

print(f'Number of mempool transactions in study period: {num_mempool_txns:,}')
    # exit()

# count how many transactions went to uniswap universal router or sushiswap router in the study period

cache_fname_txns_to_routers = '.cache/process_data_txns_to_routers.pickle'
try:
    with open(cache_fname_txns_to_routers, 'rb') as f:
        txns_to_universal_router, txns_to_sushiswap_router, blocks_seen = pickle.load(f)
except FileNotFoundError:
    print('Loading txns to routers...')
    from bulk import _bulk_get_blocks

    # see if we can fast-forward
    try:
        with open(cache_fname_txns_to_routers + '.tmp', 'rb') as f:
            txns_to_universal_router, txns_to_sushiswap_router, blocks_seen = pickle.load(f)
        print('fast-forwarding from prior run')
    except FileNotFoundError:
        print('no cache file found')
        txns_to_universal_router = set()
        txns_to_sushiswap_router = set()
        blocks_seen = set()

    for block_number in range(constants.START_BLOCK, constants.END_BLOCK + 1):
        if block_number not in blocks_seen:
            start_block = block_number
            break

    blocks = _bulk_get_blocks(start_block, constants.END_BLOCK + 1)

    txns_to_universal_router = set()
    txns_to_sushiswap_router = set()
    blocks_seen = set()

    for i, block in enumerate(blocks):
        if block in blocks_seen:
            continue
        blocks_seen.add(block.number)

        for txn in block.transactions:
            if txn.to is None:
                continue
            if txn.to.lower() == constants.UNISWAP_UNIVERSAL_ROUTER.lower():
                txns_to_universal_router.add(txn.hash)
            if txn.to.lower() == constants.SUSHISWAP_ROUTER.lower():
                txns_to_sushiswap_router.add(txn.hash)

        # if i % 1_000 == 0:
        #     with open(cache_fname_txns_to_routers + '.tmp', 'wb') as f:
        #         pickle.dump((txns_to_universal_router, txns_to_sushiswap_router, blocks_seen), f)
        #     print(f'Found {len(txns_to_universal_router):,} txns to universal router and {len(txns_to_sushiswap_router):,} txns to sushiswap router in {len(blocks_seen):,} blocks')

    with open(cache_fname_txns_to_routers, 'wb') as f:
        pickle.dump((txns_to_universal_router, txns_to_sushiswap_router, blocks_seen), f)

# count how many were seen in the mempool
cache_fname_txns_to_routers_in_mempool = '.cache/process_data_txns_to_routers_in_mempool.pickle'
try:
    with open(cache_fname_txns_to_routers_in_mempool, 'rb') as f:
        num_txns_to_universal_router_in_mempool, num_txns_to_sushiswap_router_in_mempool = pickle.load(f)
except FileNotFoundError:
    print('Loading txns to routers in mempool...')


    cur.execute(
        '''
        SELECT transaction_hash
        FROM transaction_timestamps
        WHERE timestamp >= %s
            AND timestamp < %s
        ''',
        (
            constants.START_DATETIME,
            constants.END_DATETIME,
        )
    )
    txns_in_mempool = set(
        bytes.fromhex(x[2:]) for (x,) in cur.fetchall()
    )

    assert len(txns_in_mempool) > 0
    first_txn_in_mempool = next(iter(txns_in_mempool))
    assert isinstance(first_txn_in_mempool, bytes)
    first_txn_to_universal_router = next(iter(txns_to_universal_router))
    assert isinstance(first_txn_to_universal_router, bytes)
    first_txn_to_sushiswap_router = next(iter(txns_to_sushiswap_router))
    assert isinstance(first_txn_to_sushiswap_router, bytes)
    # ensure that we are comparing the same types....

    num_txns_to_universal_router_in_mempool = len(txns_to_universal_router.intersection(txns_in_mempool))
    num_txns_to_sushiswap_router_in_mempool = len(txns_to_sushiswap_router.intersection(txns_in_mempool))

    assert num_txns_to_universal_router_in_mempool > 0
    assert num_txns_to_sushiswap_router_in_mempool > 0

    with open(cache_fname_txns_to_routers_in_mempool, 'wb') as f:
        pickle.dump((num_txns_to_universal_router_in_mempool, num_txns_to_sushiswap_router_in_mempool), f)

all_txns = txns_to_universal_router.union(txns_to_sushiswap_router)

# get the parse failure reasons
cache_fname_uniswap_n_too_many_pools = '.cache/process_data_uniswap_n_too_many_pools-2.pickle'
try:
    with open(cache_fname_uniswap_n_too_many_pools, 'rb') as f:
        uniswap_results_counts, sushiswap_results_counts = pickle.load(f)
except FileNotFoundError:
    from bulk import _bulk_get_transaction_receipt, _bulk_load_transactions
    from scrape_router_swap_txns import process_uni_universal_router, process_sushiswap_router, TooManyPools, TooManyLogs, UnsupportedCommand, MalformedTxn

    # import random
    # all_txns = random.sample(list(all_txns), 10_000)
    # get transactions
    transactions = _bulk_load_transactions(list(all_txns))

    # need to do the work, do it in parallel
    @functools.cache
    def get_w3() -> web3.Web3:
        web3.WebsocketProvider._loop = None
        return utils.connect_web3()

    def _process_universal_router(txn):
        w3 = get_w3()
        try:
            returned = process_uni_universal_router(w3, txn, strict=True)
            if returned is None:
                return 'other'
            else:
                return 'success'
        except TooManyPools:
            return 'too_many_pools'
        except TooManyLogs:
            return 'too_many_logs'
        except UnsupportedCommand:
            return 'unsupported_command'
        except MalformedTxn:
            return 'malformed_txn'

    def _process_sushiswap_router(txn):
        w3 = get_w3()
        try:
            returned = process_sushiswap_router(w3, txn, strict=True)
            if returned is None:
                return 'other'
            else:
                return 'success'
        except TooManyPools:
            return 'too_many_pools'
        except TooManyLogs:
            return 'too_many_logs'
        except UnsupportedCommand:
            return 'unsupported_command'
        except MalformedTxn:
            return 'malformed_txn'
    
    def _process_txn(txn):
        if txn['to'] == constants.UNISWAP_UNIVERSAL_ROUTER:
            return 'uniswap', _process_universal_router(txn)
        elif txn['to'] == constants.SUSHISWAP_ROUTER:
            return 'sushiswap', _process_sushiswap_router(txn)
        else:
            raise Exception(f'Unknown router: {txn["to"]}')

    uniswap_results_counts = collections.Counter()
    sushiswap_results_counts = collections.Counter()
    with multiprocessing.Pool(24) as pool:
        last_print = time.time()
        observations = collections.deque([(0, time.time())])

        results = pool.imap_unordered(_process_txn, list(transactions))
        for i, (protocol, result) in enumerate(results):
            observations.append((i+1, time.time()))

            while observations[-1][1] - observations[0][1] > 5 * 10:
                observations.popleft()
            
            if time.time() - last_print > 5:
                last_print = time.time()
                n_processed = observations[-1][0] - observations[0][0]
                elapsed = observations[-1][1] - observations[0][1]
                speed = n_processed / elapsed
                remaining = len(transactions) - (i+1)
                eta_s = remaining / speed
                eta = datetime.timedelta(seconds=eta_s)
                print(f'Parsing swaps - {i+1:,} / {len(transactions):,} ({(i+1) / len(transactions) * 100:.2f}%) - {speed:.2f} txns/s - ETA {eta}')
            
            if protocol == 'uniswap':
                uniswap_results_counts[result] += 1
            elif protocol == 'sushiswap':
                sushiswap_results_counts[result] += 1
            else:
                raise Exception(f'Unknown protocol: {protocol}')
        

        with open(cache_fname_uniswap_n_too_many_pools, 'wb') as f:
            pickle.dump((dict(uniswap_results_counts), dict(sushiswap_results_counts)), f)
    
    print('done')

# print the results
print('Uniswap parse results:')
print(tabulate.tabulate(
    sorted(
        [
            ('Success', uniswap_results_counts.get('success', 0)),
            ('Too many pools', uniswap_results_counts.get('too_many_pools', 0)),
            ('Too many logs', uniswap_results_counts.get('too_many_logs', 0)),
            ('Unsupported command', uniswap_results_counts.get('unsupported_command', 0)),
            ('Malformed txn', uniswap_results_counts.get('malformed_txn', 0)),
            ('Other', uniswap_results_counts.get('other', 0)),
        ],
        key=lambda x: x[1],
        reverse=True
    ),
    headers=['Result', 'Count'],
))
print()

print('Sushiswap parse results:')
print(tabulate.tabulate(
    sorted(
        [
            ('Success', sushiswap_results_counts.get('success', 0)),
            ('Too many pools', sushiswap_results_counts.get('too_many_pools', 0)),
            ('Too many logs', sushiswap_results_counts.get('too_many_logs', 0)),
            ('Unsupported command', sushiswap_results_counts.get('unsupported_command', 0)),
            ('Malformed txn', sushiswap_results_counts.get('malformed_txn', 0)),
            ('Other', sushiswap_results_counts.get('other', 0)),
        ],
        key=lambda x: x[1],
        reverse=True
    ),
    headers=['Result', 'Count'],
))
print()

# ensure that the number of successful parses squares up, in two ways:
# 1. with the number of router transactions recorded here
# 2. with the number of swaps parsed in the database

# pt 1
num_uniswap_successful_parses = uniswap_results_counts.get('success', 0)
num_sushiswap_successful_parses = sushiswap_results_counts.get('success', 0)
assert sum(uniswap_results_counts.values()) == len(txns_to_universal_router)
assert sum(sushiswap_results_counts.values()) == len(txns_to_sushiswap_router)

# pt 2
cur.execute(
    '''
    SELECT COUNT(DISTINCT txn_hash)
    FROM uniswap_universal_router_txns
    '''
)
num_uniswap_successful_parses_db = cur.fetchone()[0]
assert num_uniswap_successful_parses == num_uniswap_successful_parses_db

cur.execute(
    '''
    SELECT COUNT(DISTINCT txn_hash)
    FROM sushiswap_router_txns
    '''
)
num_sushiswap_successful_parses_db = cur.fetchone()[0]
assert num_sushiswap_successful_parses == num_sushiswap_successful_parses_db

cache_fname_infer_slippage_counts = '.cache/process_data_infer_slippage_counts.pickle'
try:
    with open(cache_fname_infer_slippage_counts, 'rb') as f:
        uniswap_slippage_result_counts, sushiswap_slippage_result_counts = pickle.load(f)
except FileNotFoundError:
    print('Bulk-processing slippages to report on success rate and failure causes...')
    
    conn.rollback() # for safety, don't want to accidentally write something to the database
    # we need to rotate out the actual slippages table
    cur.execute(
        '''
        ALTER TABLE slippage_results RENAME TO slippage_results_old_process_data
        '''
    )
    conn.commit()

    print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    print('ROTATED OUT SLIPPAGE RESULTS TABLE -- DO NOT FORGET TO ROTATE IT BACK IN IF THIS SCRIPT FAILS')
    print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    
    import time; time.sleep(5)

    # we need to re-create the slippage results table
    db.create_schema(cur)
    conn.commit()

    try:
        # re-process the transactions, with strict mode enabled
        from compute_slippages import bulk_process, SlippageResult
        results = bulk_process(strict=True, routers=[constants.UNISWAP_UNIVERSAL_ROUTER, constants.SUSHISWAP_ROUTER])
    finally:
        print('rotating table back in...')
        # rotate the table back in
        cur.execute(
            '''
            DROP TABLE slippage_results
            '''
        )
        cur.execute(
            '''
            ALTER TABLE slippage_results_old_process_data RENAME TO slippage_results
            '''
        )
        conn.commit()

    uniswap_slippage_result_counts = collections.Counter()
    sushiswap_slippage_result_counts = collections.Counter()

    for result, router in results:
        if router == constants.UNISWAP_UNIVERSAL_ROUTER:
            if isinstance(result, SlippageResult):
                uniswap_slippage_result_counts['success'] += 1
            elif isinstance(result, str):
                uniswap_slippage_result_counts[result] += 1
            else:
                raise Exception(f'Unknown result type: {result}')
        elif router == constants.SUSHISWAP_ROUTER:
            if isinstance(result, SlippageResult):
                sushiswap_slippage_result_counts['success'] += 1
            elif isinstance(result, str):
                sushiswap_slippage_result_counts[result] += 1
            else:
                raise Exception(f'Unknown result type: {result}')

    with open(cache_fname_infer_slippage_counts, 'wb') as f:
        pickle.dump((dict(uniswap_slippage_result_counts), dict(sushiswap_slippage_result_counts)), f)

# count how many were not in the mempool
cur.execute(
    '''
    SELECT COUNT(*)
    FROM uniswap_universal_router_txns
    WHERE NOT EXISTS (
        SELECT FROM transaction_timestamps WHERE transaction_timestamps.transaction_hash = concat('0x', encode(uniswap_universal_router_txns.txn_hash, 'hex'))
    )
    '''
)
n_uniswap_universal_not_seen_in_mempool = cur.fetchone()[0]
uniswap_slippage_result_counts['not_in_mempool'] = n_uniswap_universal_not_seen_in_mempool

cur.execute(
    '''
    SELECT COUNT(*)
    FROM sushiswap_router_txns
    WHERE NOT EXISTS (
        SELECT FROM transaction_timestamps WHERE transaction_timestamps.transaction_hash = concat('0x', encode(sushiswap_router_txns.txn_hash, 'hex'))
    )
    '''
)
n_sushiswap_not_seen_in_mempool = cur.fetchone()[0]
sushiswap_slippage_result_counts['not_in_mempool'] = n_sushiswap_not_seen_in_mempool

# print the results
print('Uniswap slippage inference results:')
print(tabulate.tabulate(uniswap_slippage_result_counts.items(), headers=['Result', 'Count']))
print()


print('Sushiswap slippage inference results:')
print(tabulate.tabulate(sushiswap_slippage_result_counts.items(), headers=['Result', 'Count']))
print()

# ensure that the numbers add up
assert sum(uniswap_slippage_result_counts.values()) == num_uniswap_successful_parses, f'{sum(uniswap_slippage_result_counts.values())} != {num_uniswap_successful_parses}'
assert sum(sushiswap_slippage_result_counts.values()) == num_sushiswap_successful_parses, f'{sum(sushiswap_slippage_result_counts.values())} != {num_sushiswap_successful_parses}'


# get success rate of these transactions
cache_fname_txns_to_routers_success = '.cache/process_data_txns_to_routers_success.pickle'
try:
    with open(cache_fname_txns_to_routers_success, 'rb') as f:
        num_txns_to_universal_router_success, num_txns_to_sushiswap_router_success = pickle.load(f)
except FileNotFoundError:
    print('Loading txns to routers success...')
    num_txns_to_universal_router_success = 1_484_884
    num_txns_to_sushiswap_router_success = 30_091

    # # need to load receipts for this
    # from load_bot_identification import _bulk_get_transaction_receipt, _bulk_load_transactions
    # from scrape_router_swap_txns import process_uni_universal_router, TooManyPools, TooManyLogs

    # receipts = _bulk_get_transaction_receipt(list(all_txns))

    # num_txns_to_universal_router_success = 0
    # num_txns_to_sushiswap_router_success = 0

    # for txn_hash, receipt in receipts.items():
    #     if receipt.status == 1:
    #         if receipt.transactionHash in txns_to_universal_router:
    #             num_txns_to_universal_router_success += 1
    #         if receipt.transactionHash in txns_to_sushiswap_router:
    #             num_txns_to_sushiswap_router_success += 1
    
    with open(cache_fname_txns_to_routers_success, 'wb') as f:
        pickle.dump((num_txns_to_universal_router_success, num_txns_to_sushiswap_router_success), f)

# how many have amount bound = 0?
cur.execute(
    '''
    SELECT COUNT(*)
    FROM uniswap_universal_router_txns
    WHERE amount_bound = 0 and exact_input = true
    '''
)
n_uniswap_amount_bound_zero = cur.fetchone()[0]

cur.execute(
    '''
    SELECT COUNT(*)
    FROM sushiswap_router_txns
    WHERE amount_bound = 0 and exact_input = true
    '''
)

n_sushiswap_amount_bound_zero = cur.fetchone()[0]

pct_txns_to_universal_router_seen_in_mempool = num_txns_to_universal_router_in_mempool / len(txns_to_universal_router) * 100
pct_txns_to_sushiswap_router_seen_in_mempool = num_txns_to_sushiswap_router_in_mempool / len(txns_to_sushiswap_router) * 100
pct_all_txns_seen_in_mempool = (num_txns_to_universal_router_in_mempool + num_txns_to_sushiswap_router_in_mempool) / (len(txns_to_universal_router) + len(txns_to_sushiswap_router)) * 100
pct_txns_to_universal_router_success = num_txns_to_universal_router_success / len(txns_to_universal_router) * 100
pct_txns_to_sushiswap_router_success = num_txns_to_sushiswap_router_success / len(txns_to_sushiswap_router) * 100
pct_all_txns_success = (num_txns_to_universal_router_success + num_txns_to_sushiswap_router_success) / (len(txns_to_universal_router) + len(txns_to_sushiswap_router)) * 100


# find the deploy block
cur.execute(
    '''
    SELECT number
    FROM block_timestamps
    WHERE timestamp < %s
    ORDER BY number DESC
    LIMIT 1
    ''',
    (
        datetime.datetime(
            year=2023,
            month=3,
            day=16,
        ),
    )
)
deploy_block = cur.fetchone()[0]
print(f'Using deploy block: {deploy_block:,}')

# find the post-deploy block
cur.execute(
    '''
    SELECT number
    FROM block_timestamps
    WHERE timestamp < %s
    ORDER BY number DESC
    LIMIT 1
    ''',
    (
        datetime.datetime(
            year=2023,
            month=3,
            day=21,
        ),
    )
)
postdeploy_block = cur.fetchone()[0]
print(f'Using postdeploy block: {postdeploy_block:,}')

cur.execute(
    '''
    SELECT count(*)
    from uniswap_universal_router_txns
    where exists (
        select from transaction_timestamps where transaction_timestamps.transaction_hash = concat('0x', encode(uniswap_universal_router_txns.txn_hash, 'hex'))
    )
    '''
)
n_parsed_uniswap_txns = cur.fetchone()[0]

cur.execute(
    '''
    SELECT count(*) from sushiswap_router_txns
    where exists (
        select from transaction_timestamps where transaction_timestamps.transaction_hash = concat('0x', encode(sushiswap_router_txns.txn_hash, 'hex'))
    )
    '''
)
n_parsed_sushiswap_txns = cur.fetchone()[0]

cur.execute(
    '''
    select count(*)
    from slippage_results
    where router like 'UniswapUniversal%' and slippage != 'nan'::float;
    '''
)
num_uniswap_txns = cur.fetchone()[0]
print(f'Number of slippages inferred on Uniswap: {num_uniswap_txns:,}')

cur.execute(
    '''
    select count(*)
    from slippage_results
    where router like 'Sushiswap%' and slippage != 'nan'::float;
    '''
)
num_sushiswap_txns = cur.fetchone()[0]
print(f'Number of slippages inferred on Sushiswap: {num_sushiswap_txns:,}')

# measure negative slippages
cur.execute(
    '''
    SELECT
        COUNT(*),
        COUNT(*) FILTER (WHERE success = true)
    FROM slippage_results
    WHERE router like 'UniswapUniversal%%'
        AND slippage < 0
    '''
)
num_negative_slippages_uni, num_successful_negative_slippages_uni = cur.fetchone()
print(f'Number of negative slippages on Uniswap: {num_negative_slippages_uni:,} (success rate {num_successful_negative_slippages_uni / num_negative_slippages_uni * 100:.2f}%)')

cur.execute(
    '''
    SELECT
        COUNT(*),
        COUNT(*) FILTER (WHERE success = true)
    FROM slippage_results
    WHERE router like 'Sushiswap%%'
        AND slippage < 0
    '''
)
num_negative_slippages_sushi, num_successful_negative_slippages_sushi = cur.fetchone()
print(f'Number of negative slippages on Sushiswap: {num_negative_slippages_sushi:,} (success rate {num_successful_negative_slippages_sushi / num_negative_slippages_sushi * 100:.2f}%)')

# measure slippages over 50
cur.execute(
    '''
    SELECT COUNT(*)
    FROM slippage_results
    WHERE router like 'UniswapUniversal%%'
        AND slippage > 50
    '''
)
num_slippages_over_50_uni = cur.fetchone()[0]
print(f'Number of slippages over 50% on Uniswap: {num_slippages_over_50_uni:,}')

cur.execute(
    '''
    SELECT COUNT(*)
    FROM slippage_results
    WHERE router like 'Sushiswap%%'
        AND slippage > 50
    '''
)
num_slippages_over_50_sushi = cur.fetchone()[0]
print(f'Number of slippages over 50% on Sushiswap: {num_slippages_over_50_sushi:,}')

# measure number of slippages that are nan
cur.execute(
    '''
    SELECT COUNT(*)
    FROM slippage_results
    WHERE router like 'UniswapUniversal%%'
        AND slippage = 'nan'::float
    '''
)
num_nan_slippages_uni = cur.fetchone()[0]
print(f'Number of slippages that are nan on Uniswap: {num_nan_slippages_uni:,}')

cur.execute(
    '''
    SELECT COUNT(*)
    FROM slippage_results
    WHERE router like 'Sushiswap%%'
        AND slippage = 'nan'::float
    '''
)
num_nan_slippages_sushi = cur.fetchone()[0]
print(f'Number of slippages that are nan on Sushiswap: {num_nan_slippages_sushi:,}')

# the number of Delta x and Delta y swaps (ie, exact in / exact out)
cur.execute(
    '''
    SELECT
        COUNT(*) FILTER (WHERE exact_input = TRUE)
    FROM uniswap_universal_router_txns
    '''
)
(n_exact_in_uniswap,) = cur.fetchone()

pct_exact_in_uniswap = n_exact_in_uniswap / n_parsed_uniswap_txns

cur.execute(
    '''
    SELECT
        COUNT(*) FILTER (WHERE exact_input = TRUE)
    FROM sushiswap_router_txns
    '''
)
(n_exact_in_sushiswap,) = cur.fetchone()

pct_exact_in_sushiswap = n_exact_in_sushiswap / n_parsed_sushiswap_txns

print('printing to output file')
with open('latex_stats.tex', 'w') as f:
    print('% ------------------------------', file=f)
    print('% AUTOGENERATED BY process_data.py part 1 -- DO NOT EDIT', file=f)


    print(r'\newcommand{\numUniswapAmountBoundZero}{%s\xspace}' % f'{n_uniswap_amount_bound_zero:,}', file=f)
    print(r'\newcommand{\numSushiswapAmountBoundZero}{%s\xspace}' % f'{n_sushiswap_amount_bound_zero:,}', file=f)
    print(file=f)
    print(r'\newcommand{\nUniswapTxnsExactIn}{%s\xspace}' % f'{n_exact_in_uniswap:,}', file=f)
    print(r'\newcommand{\pctUniswapTxnsExactIn}{%s\%%\xspace}' % f'{pct_exact_in_uniswap * 100:.1f}', file=f)
    print(r'\newcommand{\nSushiswapTxnsExactIn}{%s\xspace}' % f'{n_exact_in_sushiswap:,}', file=f)
    print(r'\newcommand{\pctSushiswapTxnsExactIn}{%s\%%\xspace}' % f'{pct_exact_in_sushiswap * 100:.1f}', file=f)
    print(file=f)
    print(r'\newcommand{\numUniswapParsedTxns}{%s\xspace}' % f'{n_parsed_uniswap_txns:,}', file=f)
    print(r'\newcommand{\numSushiswapParsedTxns}{%s\xspace}' % f'{n_parsed_sushiswap_txns:,}', file=f)
    print(file=f)
    print(r'\newcommand{\numUniswapSlippageInferred}{%s\xspace}' % f'{num_uniswap_txns:,}', file=f)
    print(r'\newcommand{\pctUniswapSlippageInferred}{%s\%%\xspace}' % f'{num_uniswap_txns / n_parsed_uniswap_txns * 100:.1f}', file=f)
    print(r'\newcommand{\numSushiswapSlippageInferred}{%s\xspace}' % f'{num_sushiswap_txns:,}', file=f)
    print(r'\newcommand{\pctSushiswapSlippageInferred}{%s\%%\xspace}' % f'{num_sushiswap_txns / n_parsed_sushiswap_txns * 100:.1f}', file=f)
    print(file=f)
    print(r'\newcommand{\numUniswapTooManyLogs}{%s\xspace}' % f'{uniswap_results_counts["too_many_logs"]:,}', file=f)
    print(file=f)
    print(r'\newcommand{\numUniswapTooManyPools}{%s\xspace}' % f'{uniswap_results_counts["too_many_pools"]:,}', file=f)
    print(r'\newcommand{\numSushiswapTooManyPools}{%s\xspace}' % f'{sushiswap_results_counts["too_many_pools"]:,}', file=f)
    print(r'\newcommand{\pctUniswapSinglePool}{%s\%%\xspace}' % f'{(1 - uniswap_results_counts["too_many_pools"] / len(txns_to_universal_router)) * 100:.1f}', file=f)
    print(r'\newcommand{\pctSushiswapSinglePool}{%s\%%\xspace}' % f'{(1 - sushiswap_results_counts["too_many_pools"] / len(txns_to_sushiswap_router)) * 100:.1f}', file=f)
    print(r'\newcommand{\numMempoolTxns}{%s\xspace}' % f'{num_mempool_txns:,}', file=f)
    print(file=f)
    print(r'\newcommand{\numTxnsToUniversalRouter}{%s\xspace}' % f'{len(txns_to_universal_router):,}', file=f)
    print(r'\newcommand{\numTxnsToSushiswapRouter}{%s\xspace}' % f'{len(txns_to_sushiswap_router):,}', file=f)
    print(r'\newcommand{\numTxnsTotal}{%s\xspace}' % f'{len(txns_to_universal_router.union(txns_to_sushiswap_router)):,}', file=f)
    print(file=f)
    print(r'\newcommand{\numNegativeSlippagesUniswap}{%s\xspace}' % f'{num_negative_slippages_uni:,}', file=f)
    print(r'\newcommand{\pctNegativeSlippagesUniswap}{%s\%%\xspace}' % f'{num_negative_slippages_uni / num_uniswap_txns * 100:.1f}', file=f)
    print(r'\newcommand{\pctSuccessfulNegativeSlippagesUniswap}{%s\%%\xspace}' % f'{num_successful_negative_slippages_uni / num_negative_slippages_uni * 100:.1f}', file=f)
    print(r'\newcommand{\numNegativeSlippagesSushiswap}{%s\xspace}' % f'{num_negative_slippages_sushi:,}', file=f)
    print(r'\newcommand{\pctNegativeSlippagesSushiswap}{%s\%%\xspace}' % f'{num_negative_slippages_sushi / num_sushiswap_txns * 100:.1f}', file=f)
    print(r'\newcommand{\pctSuccessfulNegativeSlippagesSushiswap}{%s\%%\xspace}' % f'{num_successful_negative_slippages_sushi / num_negative_slippages_sushi * 100:.1f}', file=f)
    print(file=f)
    print(r'\newcommand{\pctSlippagesOverFiftyUniswap}{%s\%%\xspace}' % f'{num_slippages_over_50_uni / num_uniswap_txns * 100:.1f}', file=f)
    print(r'\newcommand{\pctSlippagesOverFiftySushiswap}{%s\%%\xspace}' % f'{num_slippages_over_50_sushi / num_sushiswap_txns * 100:.1f}', file=f)
    print(file=f)
    print(r'\newcommand{\numTxnsToUniversalRouterInMempool}{%s\xspace}' % f'{num_txns_to_universal_router_in_mempool:,}', file=f)
    print(r'\newcommand{\pctTxnsToUniversalRouterInMempool}{%s\%%\xspace}' % f'{pct_txns_to_universal_router_seen_in_mempool:.1f}', file=f)
    print(r'\newcommand{\numTxnsToSushiswapRouterInMempool}{%s\xspace}' % f'{num_txns_to_sushiswap_router_in_mempool:,}', file=f)
    print(r'\newcommand{\pctTxnsToSushiswapRouterInMempool}{%s\%%\xspace}' % f'{pct_txns_to_sushiswap_router_seen_in_mempool:.1f}', file=f)
    print(r'\newcommand{\numTxnsTotalInMempool}{%s\xspace}' % f'{num_txns_to_universal_router_in_mempool + num_txns_to_sushiswap_router_in_mempool:,}', file=f)
    print(r'\newcommand{\pctTxnsTotalInMempool}{%s\%%\xspace}' % f'{pct_all_txns_seen_in_mempool:.1f}', file=f)
    print(file=f)
    print(r'\newcommand{\numTxnsToUniversalRouterSuccess}{%s\xspace}' % f'{num_txns_to_universal_router_success:,}', file=f)
    print(r'\newcommand{\pctTxnsToUniversalRouterSuccess}{%s\%%\xspace}' % f'{pct_txns_to_universal_router_success:.1f}', file=f)
    print(r'\newcommand{\numTxnsToSushiswapRouterSuccess}{%s\xspace}' % f'{num_txns_to_sushiswap_router_success:,}', file=f)
    print(r'\newcommand{\pctTxnsToSushiswapRouterSuccess}{%s\%%\xspace}' % f'{pct_txns_to_sushiswap_router_success:.1f}', file=f)
    print(r'\newcommand{\numTxnsTotalSuccess}{%s\xspace}' % f'{num_txns_to_universal_router_success + num_txns_to_sushiswap_router_success:,}', file=f)
    print(r'\newcommand{\pctTxnsTotalSuccess}{%s\%%\xspace}' % f'{pct_all_txns_success:.1f}', file=f)

    # stupid -- emit a table using booktabs
    table_sz = r'''\begin{table}[]
    \begin{center}
    \begin{tabular}{@{}lrrr@{}}
    \toprule
    & All & Seen in mempool & Succeeded \\ \midrule
    \textbf{Uniswap} & \numTxnsToUniversalRouter &
        \numTxnsToUniversalRouterInMempool (\pctTxnsToUniversalRouterInMempool) &
        \numTxnsToUniversalRouterSuccess (\pctTxnsToUniversalRouterSuccess) \\
    \textbf{Sushiswap} & \numTxnsToSushiswapRouter &
        \numTxnsToSushiswapRouterInMempool (\pctTxnsToSushiswapRouterInMempool) &
        \numTxnsToSushiswapRouterSuccess (\pctTxnsToSushiswapRouterSuccess) \\ \midrule
    \textbf{Total} & \numTxnsTotal &
        \numTxnsTotalInMempool (\pctTxnsTotalInMempool) &
        \numTxnsTotalSuccess (\pctTxnsTotalSuccess) \\ \bottomrule
    \end{tabular}
    \caption{Router-based swap transactions.}
    \label{tab:router-txns}
    \end{center}
    \end{table}
    '''

    print(r'\newcommand{\txnsTable}{%s}' % table_sz, file=f)

    print('% ------------------------------', file=f)

# number of transactions that seem to have arrived after the confirmation block
cur.execute(
    '''
    SELECT
        u_txn.id,
        (
            SELECT block_timestamps.number
            FROM block_timestamps
            WHERE block_timestamps.timestamp < arr.timestamp
            ORDER BY block_timestamps.timestamp DESC
            LIMIT 1
        ) number,
        u_txn.block_number,
        arr.timestamp
    FROM uniswap_universal_router_txns u_txn
    JOIN transaction_timestamps arr ON concat('0x', encode(u_txn.txn_hash, 'hex')) = arr.transaction_hash
    '''
)
n_too_late = 0
n_too_early = 0
for id_, presumed_gen_number, block_number, arr_ts in cur:
    if arr_ts - constants.START_DATETIME < datetime.timedelta(seconds=12):
        n_too_early += 1
        continue
    if presumed_gen_number > block_number :
        n_too_late += 1
print(f'Number of Uniswap transactions that arrived outside window: {n_too_early:,}')
print(f'Number of Uniswap transactions that seem to have arrived after the confirmation block: {n_too_late:,}')

# number of transactions that seem to have arrived after the confirmation block
cur.execute(
    '''
    SELECT
        u_txn.id,
        (
            SELECT block_timestamps.number
            FROM block_timestamps
            WHERE block_timestamps.timestamp < arr.timestamp
            ORDER BY block_timestamps.timestamp DESC
            LIMIT 1
        ) number,
        u_txn.block_number,
        arr.timestamp
    FROM sushiswap_router_txns u_txn
    JOIN transaction_timestamps arr ON concat('0x', encode(u_txn.txn_hash, 'hex')) = arr.transaction_hash
    '''
)
n_too_late = 0
n_too_early = 0
for id_, presumed_gen_number, block_number, arr_ts in cur:
    if arr_ts - constants.START_DATETIME < datetime.timedelta(seconds=12):
        n_too_early += 1
        continue
    if presumed_gen_number > block_number :
        n_too_late += 1
print(f'Number of Sushiswap transactions that arrived outside window: {n_too_early:,}')
print(f'Number of Sushiswap transactions that seem to have arrived after the confirmation block: {n_too_late:,}')

if False:
    # independent interest: do some traders use a strategy skewing their mean queue time?
    cur.execute(
        '''
        SELECT sender, queue_time_seconds
        FROM slippage_results
        WHERE queue_time_seconds IS NOT NULL
        AND queue_time_seconds < 12
        AND (router like 'UniswapUniversal%%' OR router ilike 'Sushiswap%%')
        '''
    )
    queue_times_by_sender = collections.defaultdict(list)
    for sender, queue_time in cur.fetchall():
        queue_times_by_sender[sender].append(queue_time)

    # get top 10 senders by number of swaps
    senders = sorted(queue_times_by_sender.keys(), key=lambda sender: len(queue_times_by_sender[sender]), reverse=True)[:30]
    tab = []
    for sender in senders:
        tab.append((
            sender,
            len(queue_times_by_sender[sender]),
            np.mean(queue_times_by_sender[sender]),
            scipy.stats.ttest_1samp(queue_times_by_sender[sender], 6.0).pvalue,
        ))
    
    print('Top senders by number of swaps')
    print(tabulate.tabulate(tab, headers=['Sender', 'Number of swaps', 'Mean queue time (seconds)', 'p-value']))
    exit()


# table we want to draw

def pct_router_txns_in_mempool():    
    cur.execute(
        '''
        SELECT count(*)
        FROM uniswap_universal_router_txns r
        WHERE EXISTS(
            SELECT FROM transaction_timestamps tt
            WHERE concat('0x', encode(r.txn_hash, 'hex')) = tt.transaction_hash
        )
        '''
    )
    n_uniswap_in_mempool = cur.fetchone()[0]
    cur.execute(
        '''
        SELECT count(*)
        FROM uniswap_universal_router_txns
        '''
    )
    n_uniswap = cur.fetchone()[0]
    cur.execute(
        '''
        SELECT count(*)
        FROM uniswap_universal_router_txns
        WHERE failed = False
        '''
    )
    n_uniswap_success = cur.fetchone()[0]
    print(f'Saw {n_uniswap:,} UniswapUniversal router transactions')
    print(f'Saw {n_uniswap_in_mempool:,} UniswapUniversal router transactions in the mempool')
    print(f'Saw {n_uniswap_success:,} UniswapUniversal router transactions that succeeded')
    n_total = n_uniswap
    n_in_mempool = n_uniswap_in_mempool

    cur.execute(
        '''
        SELECT count(*)
        FROM sushiswap_router_txns r
        WHERE EXISTS(
            SELECT FROM transaction_timestamps tt
            WHERE concat('0x', encode(r.txn_hash, 'hex')) = tt.transaction_hash
        )
        '''
    )
    n_sushiswap_in_mempool = cur.fetchone()[0]
    cur.execute(
        '''
        SELECT count(*)
        FROM sushiswap_router_txns
        '''
    )
    n_sushiswap = cur.fetchone()[0]
    cur.execute(
        '''
        SELECT count(*)
        FROM sushiswap_router_txns
        WHERE failed = False
        '''
    )
    n_sushiswap_success = cur.fetchone()[0]
    print(f'Saw {n_sushiswap:,} Sushiswap router transactions')
    print(f'Saw {n_sushiswap_in_mempool:,} Sushiswap router transactions in the mempool')
    print(f'Saw {n_sushiswap_success:,} Sushiswap router transactions that succeeded')
    n_total += n_sushiswap
    n_in_mempool += n_sushiswap_in_mempool

    return n_in_mempool / n_total * 100

pct_router_txns_in_mempool()

cur.execute(
    '''
    SELECT queue_time_seconds
    FROM slippage_results
    WHERE router like 'UniswapUniversal%%' OR router ilike 'Sushiswap%%'
    '''
)
queue_times = [x[0] for x in cur.fetchall()]
pts = [25, 50, 75]
percentiles = np.percentile(queue_times, pts)
for pt, pct in zip(pts, percentiles):
    print(f'{pt}% of swaps had a queue time of {pct:.2f} seconds or less')

# Get the percentage of swaps that used 0.5% slippage before the deploy block
TOLERANCE_PCT = 10
pts = [10, 5, 2, 1, 0.5, 0.1]
marks = []
for pt in pts:
    lo = pt * (1 - TOLERANCE_PCT / 100)
    hi = pt * (1 + TOLERANCE_PCT / 100)
    cur.execute(
        '''
        SELECT
            COUNT(*) FILTER (WHERE %s <= slippage AND slippage <= %s),
            -- COUNT(*) FILTER (WHERE slippage IS NOT NULL AND slippage <= 60 AND slippage > 0)
            COUNT(*) FILTER (WHERE slippage IS NOT NULL)
        FROM slippage_results
        WHERE block_number < %s AND router like 'UniswapUniversal%%'
        ''',
        (
            lo,
            hi,
            deploy_block,
        )
    )
    in_range, tot = cur.fetchone()
    pct = in_range / tot * 100
    marks.append((pt, pct))

print('Before deploy block:')
print(tabulate.tabulate(marks, headers=['Slippage (%)', 'Percentage of swaps within tolerance']))

# Get the percentage of swaps that used 0.5% slippage before the deploy block
TOLERANCE_PCT = 10
pts = [10, 5, 2, 1, 0.5, 0.1]
marks = []
for pt in pts:
    lo = pt * (1 - TOLERANCE_PCT / 100)
    hi = pt * (1 + TOLERANCE_PCT / 100)
    cur.execute(
        '''
        SELECT
            COUNT(*) FILTER (WHERE %s <= slippage AND slippage <= %s),
            -- COUNT(*) FILTER (WHERE slippage IS NOT NULL AND slippage <= 60 AND slippage > 0)
            COUNT(*) FILTER (WHERE slippage IS NOT NULL)
        FROM slippage_results
        WHERE router ilike 'Sushiswap%%'
        ''',
        (
            lo,
            hi,
        )
    )
    in_range, tot = cur.fetchone()
    pct = in_range / tot * 100
    marks.append((pt, pct))

print('(sushiswap)')
print(tabulate.tabulate(marks, headers=['Slippage (%)', 'Percentage of swaps within tolerance']))

TOLERANCE_PCT = 10
pts = [10, 5, 2, 1, 0.5, 0.1]
marks = []
for pt in pts:
    lo = pt * (1 - TOLERANCE_PCT / 100)
    hi = pt * (1 + TOLERANCE_PCT / 100)
    cur.execute(
        '''
        SELECT
            COUNT(*) FILTER (WHERE %s <= slippage AND slippage <= %s),
            -- COUNT(*) FILTER (WHERE slippage IS NOT NULL AND slippage <= 60 AND slippage > 0)
            COUNT(*) FILTER (WHERE slippage IS NOT NULL)
        FROM slippage_results
        WHERE block_number >= %s AND router like 'UniswapUniversal%%'
        ''',
        (
            lo,
            hi,
            postdeploy_block,
        )
    )
    in_range, tot = cur.fetchone()
    pct = in_range / tot * 100
    marks.append((pt, pct))

print('Post deploy:')
print(tabulate.tabulate(marks, headers=['Slippage (%)', 'Percentage of swaps within tolerance']))
