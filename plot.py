import math
import matplotlib.pyplot as plt
import numpy as np
import dotenv
import functools
import typing
import datetime
import pickle
import os
from matplotlib.colors import LogNorm
import matplotlib.dates as dates

from db import connect_db
from utils import connect_web3
import constants

dotenv.load_dotenv()

db = connect_db()
cur = db.cursor()

from compute_slippages import infer_slippage_from_amounts


# change matplotlib font to serif
plt.rcParams['font.family'] = 'serif'

# increase font size
plt.rcParams.update({'font.size': 14})

# a decorator that caches the result of a function to the filesystem
def cache(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        filename = f'.cache/plot_{f.__name__}.pickle'
        try:
            with open(filename, 'rb') as fin:
                return pickle.load(fin)
        except FileNotFoundError:
            pass
        result = f(*args, **kwargs)
        with open(filename, 'wb') as fout:
            pickle.dump(result, fout)
        return result
    return wrapper

w3 = connect_web3()

#
# Get block numbers over time
#
@cache
def get_block_timestamps() -> typing.Tuple[np.ndarray, np.ndarray]:
    print('getting block timestamps')
    marks = np.linspace(constants.START_BLOCK, constants.END_BLOCK, 1000, dtype=int)
    timestamps = []
    for i, mark in enumerate(marks):
        print(f'Getting block {i}...')
        block = w3.eth.get_block(int(mark), full_transactions=False)
        timestamps.append(block.timestamp)
    print('got block timestamps')
    return marks, np.array(timestamps)

block_ts_blocks, block_ts_timestamps = get_block_timestamps()


#
# Plot failure % over slippage
#
# @cache
def get_cdf_by_realized_slippage() -> typing.Tuple[np.ndarray, np.ndarray]:
    print('getting cdf of realized slippage')
    default_slippages, step = np.linspace(0, 0.1, 100, retstep=True)
    cnt_realized_slippage_bins = np.zeros_like(default_slippages)
    total_cnt = 0

    cur.execute(
        '''
        SELECT transaction_hash, amount_in, amount_out, exact_in, boundary_amount, expected_amount, slippage
        FROM slippage_results
        JOIN consistent_pools cp ON slippage_results.pool = cp.address
        WHERE router LIKE 'UniswapUniversal%' and success = TRUE
        '''
    )

    # count of how many slippages somehow exceed the expected slippage setting
    # (NOTE: this happens when tokens misreport transfer amounts in the logs)
    n_weird_slippage = 0

    # TODO do this before + after the change deployment to see if it changes between the two
    for txn_hash, amount_in, amount_out, exact_in, amount_bound, expected_amount, est_slippage in cur:
        # compute the actual slippage

        if not (0 <= est_slippage <= 60):
            continue

        realized_amount = amount_out if exact_in else amount_in
        if realized_amount == 0:
            continue

        # I DO NOT LIKE THIS
        slippage = infer_slippage_from_amounts(
            exact_in,
            amount_in if exact_in else expected_amount,
            expected_amount if exact_in else amount_out,
            realized_amount
        )
        if slippage is None:
            continue

        # bound slippage to 0 (negative slippage = price movement in favor)
        slippage = max(slippage, 0.0)

        total_cnt += 1

        if slippage > (est_slippage + 0.01):
            print(f'slippage should be less than estimated slippage but got {slippage} >= {est_slippage} for exact_in={exact_in}, amount_in={amount_in}, amount_out={amount_out}, expected_amount={expected_amount}, txn_hash={txn_hash}, amount_bound={amount_bound}')
            n_weird_slippage += 1
            # import pdb; pdb.set_trace()

        # increment the corresponding bin
        bin = math.floor(slippage / step)
        if not (0 <= bin < len(cnt_realized_slippage_bins)):
            continue

        cnt_realized_slippage_bins[bin] += 1
    
    print(f'got {n_weird_slippage} weird slippages')

    # do a cumulative sum
    cnt_realized_slippage_bins = np.cumsum(cnt_realized_slippage_bins)

    assert total_cnt > 0, 'no successes found'

    # normalize to percentage of total
    cnt_realized_slippage_bins = cnt_realized_slippage_bins / total_cnt

    print('got cdf of realized slippage')

    return default_slippages, cnt_realized_slippage_bins

default_slippages, cnt_realized_slippage_bins = get_cdf_by_realized_slippage()

print('realized slippage pct at 0.01%:', cnt_realized_slippage_bins[-1] * 100)
print('realized slippage at 0', cnt_realized_slippage_bins[0] * 100)

# dump the plot data
if not os.path.exists('plot_data/fig4'):
    os.mkdir('plot_data/fig4')

with open('plot_data/fig4/realized_slippage_cdf.csv', mode='w') as fout:
    fout.write('slippage (percent),cdf\n')
    for slippage, cdf in zip(default_slippages, cnt_realized_slippage_bins):
        fout.write(f'{slippage},{cdf}\n')

plt.plot(default_slippages, cnt_realized_slippage_bins, color='black')
# remove the top and right spines
plt.gca().spines['right'].set_visible(False)
plt.gca().spines['top'].set_visible(False)
plt.xlabel('Realized Slippage (%)')
plt.ylabel('CDF')
plt.ylim(0, 1)
plt.grid(True, linestyle='--', alpha=0.5)
plt.title('CDF, Realized Slippages, Uniswap (2)')
plt.tight_layout()
plt.savefig('plots/realized_slippage_cdf.png', dpi=300)
plt.savefig('plots/realized_slippage_cdf.pdf')
plt.cla()
plt.clf()
# exit()

#
# Plot mempool transaction rate over time
#

@cache
def get_bucketed_timestamps() -> typing.Tuple[np.ndarray, np.ndarray]:
    print('getting bucketed timestamps')
    min_timestamp = datetime.datetime(year=2023, month=3, day=1).timestamp()
    max_timestamp = datetime.datetime(year=2023, month=4, day=1).timestamp()
    buckets, step = np.linspace(min_timestamp, max_timestamp, 1000, retstep=True)
    bucket_cnts = np.zeros_like(buckets)
    cur.execute('SELECT timestamp FROM transaction_timestamps')
    for (timestamp,) in cur:
        bucket = int((timestamp.timestamp() - min_timestamp) / step)
        if not (0 <= bucket < len(bucket_cnts)):
            continue
        bucket_cnts[bucket] += 1
    print('got bucketed timestamps')
    return buckets, bucket_cnts, step


bucketed_timestamps, bucketed_cnts, bucket_step = get_bucketed_timestamps()

# normalize the bucketed counts to count per hour
bucketed_cnts = bucketed_cnts / bucket_step * 60 * 60

if not os.path.exists('plots'):
    os.mkdir('plots')

# plot the bucketed counts
xs_dts = [datetime.datetime.utcfromtimestamp(x) for x in bucketed_timestamps]
plt.plot(xs_dts, bucketed_cnts, color='black', linewidth=1)
plt.xlabel('Time')
plt.ylabel('Transactions per hour')
# convert y-ticks to thousands
plt.gca().yaxis.set_major_formatter(lambda x, pos: f'{x/1000:.0f}k')
# remove top and right spines
plt.gca().spines['top'].set_visible(False)
plt.gca().spines['right'].set_visible(False)
# show the grid
plt.grid(True, axis='y')
plt.title('Mempool Transaction Arrival Rate')
# rotate and shift the x axis labels
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig('plots/mempool_txn_rate.png', dpi=300)
print('Saved plots/mempool_txn_rate.png')
plt.cla()


# plot the uniswap routers over time
@cache
def get_router_txns() -> typing.Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    print('getting router txn counts...')
    min_timestamp = datetime.datetime(year=2023, month=3, day=1).timestamp()
    max_timestamp = datetime.datetime(year=2023, month=4, day=1).timestamp()
    buckets, step = np.linspace(min_timestamp, max_timestamp, 1000, retstep=True)
    bucket_router_v2_cnts = np.zeros_like(buckets)
    bucket_router_v3_cnts = np.zeros_like(buckets)
    bucket_router_universal_cnts = np.zeros_like(buckets)
    bucket_sushiswap_cnts = np.zeros_like(buckets)

    cur.execute('SELECT block_number FROM uniswap_v2_router_2_txns')
    for (block_number,) in cur:
        block_ts = np.interp(block_number, block_ts_blocks, block_ts_timestamps)
        bucket = int((block_ts - min_timestamp) / step)
        if not (0 <= bucket < len(bucket_router_v2_cnts)):
            continue
        bucket_router_v2_cnts[bucket] += 1
    
    cur.execute('SELECT block_number FROM uniswap_v3_router_2_txns')
    for (block_number,) in cur:
        block_ts = np.interp(block_number, block_ts_blocks, block_ts_timestamps)
        bucket = int((block_ts - min_timestamp) / step)
        if not (0 <= bucket < len(bucket_router_v3_cnts)):
            continue
        bucket_router_v3_cnts[bucket] += 1
    
    cur.execute('SELECT block_number FROM uniswap_universal_router_txns')
    for (block_number,) in cur:
        block_ts = np.interp(block_number, block_ts_blocks, block_ts_timestamps)
        bucket = int((block_ts - min_timestamp) / step)
        if not (0 <= bucket < len(bucket_router_universal_cnts)):
            continue
        bucket_router_universal_cnts[bucket] += 1
    
    cur.execute('SELECT block_number FROM sushiswap_router_txns')
    for (block_number,) in cur:
        block_ts = np.interp(block_number, block_ts_blocks, block_ts_timestamps)
        bucket = int((block_ts - min_timestamp) / step)
        if not (0 <= bucket < len(bucket_sushiswap_cnts)):
            continue
        bucket_sushiswap_cnts[bucket] += 1

    return buckets, bucket_router_v2_cnts, bucket_router_v3_cnts, bucket_router_universal_cnts, bucket_sushiswap_cnts

old_xs_dts = xs_dts
router_bucket_ts, router_v2_cnts, router_v3_cnts, router_universal_cnts, sushiswap_cnts = get_router_txns()
tot_cnts = router_v2_cnts + router_v3_cnts + router_universal_cnts + sushiswap_cnts

# normalize the bucketed counts to count per hour
router_v2_cnts = router_v2_cnts / bucket_step * 60 * 60
router_v3_cnts = router_v3_cnts / bucket_step * 60 * 60
router_universal_cnts = router_universal_cnts / bucket_step * 60 * 60
sushiswap_cnts = sushiswap_cnts / bucket_step * 60 * 60

# plot the bucketed counts
xs_dts = [datetime.datetime.utcfromtimestamp(x) for x in router_bucket_ts]
plt.plot(xs_dts, router_v2_cnts, label='Uniswap V2 Router 2')
plt.plot(xs_dts, router_v3_cnts, label='Uniswap V3 Router 2')
plt.plot(xs_dts, router_universal_cnts, label='Uniswap Universal Router')
plt.plot(xs_dts, sushiswap_cnts, label='Sushiswap Router')
plt.yscale('log')
plt.xlabel('Time')
plt.ylabel('Transactions per hour')
plt.title('Unique router transaction arrival rate')
plt.legend()
# rotate the x axis labels
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('plots/router_txn_rate.png')
print('Saved plots/router_txn_rate.png')
plt.cla()
plt.clf()

# plot all rates within one plot
xs_dts = [datetime.datetime.utcfromtimestamp(x) for x in bucketed_timestamps]
plt.plot(xs_dts, bucketed_cnts, color='black', linewidth=1, label='All Mempool')
plt.plot(old_xs_dts[:-5], router_universal_cnts[:-5], label='Uniswap Router', linewidth=1)
plt.plot(old_xs_dts[:-5], sushiswap_cnts[:-5], label='Sushiswap Router', linewidth=1)

# dump the plot data
if not os.path.exists('plot_data/fig1'):
    os.mkdir('plot_data/fig1')
with open('plot_data/fig1/all_mempool.csv', mode='w') as fout:
    for ts, pt in zip(xs_dts, bucketed_cnts):
        fout.write(f'{ts.isoformat()},{pt}\n')
with open('plot_data/fig1/uniswap_router.csv', mode='w') as fout:
    for ts, pt in zip(xs_dts[:-5], router_universal_cnts[:-5]):
        fout.write(f'{ts.isoformat()},{pt}\n')
with open('plot_data/fig1/sushiswap_router.csv', mode='w') as fout:
    for ts, pt in zip(xs_dts[:-5], sushiswap_cnts[:-5]):
        fout.write(f'{ts.isoformat()},{pt}\n')

plt.xlabel('Time')
plt.ylabel('Transactions per hour')
# convert y-ticks to thousands
# plt.gca().yaxis.set_major_formatter(lambda x, pos: f'{x/1000:.0f}k')
# log scale
plt.yscale('log')
# remove top and right spines
plt.gca().spines['top'].set_visible(False)
plt.gca().spines['right'].set_visible(False)
# show the grid with dashes
plt.grid(True, axis='y', linestyle='--')
plt.title('Transaction Arrival Rate')
# format x-axis as month-date
plt.gca().xaxis.set_major_formatter(dates.DateFormatter('%m-%d'))
# rotate and shift the x axis labels
plt.xticks(rotation=45, ha='right')
plt.legend(loc='lower center', ncol=3, prop={'size': 10})
plt.tight_layout()
# push the title up
plt.savefig('plots/all_txn_rate.png', dpi=300)
print('Saved plots/all_txn_rate.png')
plt.savefig('plots/all_txn_rate.pdf')
plt.cla()
plt.clf()


@cache
def get_cnt_captured_in_mempool_by_router() -> typing.Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    min_timestamp = datetime.datetime(year=2023, month=3, day=1).timestamp()
    max_timestamp = datetime.datetime(year=2023, month=4, day=1).timestamp()
    buckets, step = np.linspace(min_timestamp, max_timestamp, 1000, retstep=True)
    bucket_router_v2_cnts = np.zeros_like(buckets)
    bucket_router_v3_cnts = np.zeros_like(buckets)
    bucket_router_universal_cnts = np.zeros_like(buckets)
    bucket_sushiswap_cnts = np.zeros_like(buckets)

    cur.execute(
        '''
        SELECT block_number
        FROM uniswap_v2_router_2_txns r
        WHERE EXISTS(
            SELECT FROM transaction_timestamps tt
            WHERE concat('0x', encode(r.txn_hash, 'hex')) = tt.transaction_hash
        )
        '''
    )
    for (block_number,) in cur:
        block_ts = np.interp(block_number, block_ts_blocks, block_ts_timestamps)
        bucket = int((block_ts - min_timestamp) / step)
        if not (0 <= bucket < len(bucket_router_v2_cnts)):
            continue
        bucket_router_v2_cnts[bucket] += 1

    cur.execute(
        '''
        SELECT block_number
        FROM uniswap_v3_router_2_txns r
        WHERE EXISTS(
            SELECT FROM transaction_timestamps tt
            WHERE concat('0x', encode(r.txn_hash, 'hex')) = tt.transaction_hash
        )
        '''
    )
    for (block_number,) in cur:
        block_ts = np.interp(block_number, block_ts_blocks, block_ts_timestamps)
        bucket = int((block_ts - min_timestamp) / step)
        if not (0 <= bucket < len(bucket_router_v3_cnts)):
            continue
        bucket_router_v3_cnts[bucket] += 1
    
    cur.execute(
        '''
        SELECT block_number
        FROM uniswap_universal_router_txns r
        WHERE EXISTS(
            SELECT FROM transaction_timestamps tt
            WHERE concat('0x', encode(r.txn_hash, 'hex')) = tt.transaction_hash
        )
        '''
    )
    for (block_number,) in cur:
        block_ts = np.interp(block_number, block_ts_blocks, block_ts_timestamps)
        bucket = int((block_ts - min_timestamp) / step)
        if not (0 <= bucket < len(bucket_router_universal_cnts)):
            continue
        bucket_router_universal_cnts[bucket] += 1
    
    cur.execute(
        '''
        SELECT block_number
        FROM sushiswap_router_txns r
        WHERE EXISTS(
            SELECT FROM transaction_timestamps tt
            WHERE concat('0x', encode(r.txn_hash, 'hex')) = tt.transaction_hash
        )
        '''
    )
    for (block_number,) in cur:
        block_ts = np.interp(block_number, block_ts_blocks, block_ts_timestamps)
        bucket = int((block_ts - min_timestamp) / step)
        if not (0 <= bucket < len(bucket_sushiswap_cnts)):
            continue
        bucket_sushiswap_cnts[bucket] += 1

    return buckets, bucket_router_v2_cnts, bucket_router_v3_cnts, bucket_router_universal_cnts, bucket_sushiswap_cnts

router_bucket_ts, router_v2_mempool_cnts, router_v3_mempool_cnts, router_universal_mempool_cnts, sushiswap_mempool_cnts = \
    get_cnt_captured_in_mempool_by_router()

# compute percentages
tot_mempool_cnts = router_v2_mempool_cnts + router_v3_mempool_cnts + router_universal_mempool_cnts + sushiswap_mempool_cnts
pct_all_mempool_cnts = np.divide(tot_mempool_cnts, tot_cnts, dtype=float, where=tot_cnts != 0, out=np.zeros_like(tot_mempool_cnts)) * 100


# pct_router_v2_mempool_cnts = np.divide(router_v2_mempool_cnts, router_v2_cnts, dtype=float, where=router_v2_cnts != 0, out=np.zeros_like(router_v2_mempool_cnts)) * 100
# pct_router_v3_mempool_cnts = np.divide(router_v3_mempool_cnts, router_v3_cnts, dtype=float, where=router_v3_cnts != 0, out=np.zeros_like(router_v3_mempool_cnts)) * 100
# pct_router_universal_mempool_cnts = np.divide(router_universal_mempool_cnts, router_universal_cnts, dtype=float, where=router_universal_cnts != 0, out=np.zeros_like(router_universal_mempool_cnts)) * 100
# pct_sushiswap_mempool_cnts = np.divide(sushiswap_mempool_cnts, sushiswap_cnts, dtype=float, where=sushiswap_cnts != 0, out=np.zeros_like(sushiswap_mempool_cnts)) * 100

# plt.plot(xs_dts, pct_router_v2_mempool_cnts, label='Uniswap V2 Router 2')
# plt.plot(xs_dts, pct_router_v3_mempool_cnts, label='Uniswap V3 Router 2')
# plt.plot(xs_dts, pct_router_universal_mempool_cnts, label='Uniswap Universal Router')
# plt.plot(xs_dts, pct_sushiswap_mempool_cnts, label='Sushiswap Router')
plt.plot(xs_dts, pct_all_mempool_cnts, label='All Routers')
plt.xlabel('Time')
plt.ylabel('Transactions captured in mempool (%)')
plt.title('Percentage of router transactions captured in mempool')
plt.legend()
# rotate the x axis labels
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('plots/router_txn_pct_mempool.png')
print('Saved plots/router_txn_pct_mempool.png')
plt.cla()


# plot the failure rates over time
@cache
def get_swap_failures():
    print('getting failure rates')
    min_timestamp = datetime.datetime(year=2023, month=3, day=1).timestamp()
    max_timestamp = datetime.datetime(year=2023, month=4, day=1).timestamp()
    buckets, step = np.linspace(min_timestamp, max_timestamp, 1000, retstep=True)
    bucket_router_v2_failure_cnts = np.zeros_like(buckets)
    bucket_router_v3_failure_cnts = np.zeros_like(buckets)
    bucket_router_universal_failure_cnts = np.zeros_like(buckets)
    bucket_sushiswap_failure_cnts = np.zeros_like(buckets)

    cur.execute(
        '''
        SELECT block_number
        FROM uniswap_v2_router_2_txns r
        WHERE failed = TRUE
        '''
    )
    for (block_number,) in cur:
        block_ts = np.interp(block_number, block_ts_blocks, block_ts_timestamps)
        bucket = int((block_ts - min_timestamp) / step)
        if not (0 <= bucket < len(bucket_router_v2_failure_cnts)):
            continue
        bucket_router_v2_failure_cnts[bucket] += 1
    
    cur.execute(
        '''
        SELECT block_number
        FROM uniswap_v3_router_2_txns r
        WHERE failed = TRUE
        '''
    )
    for (block_number,) in cur:
        block_ts = np.interp(block_number, block_ts_blocks, block_ts_timestamps)
        bucket = int((block_ts - min_timestamp) / step)
        if not (0 <= bucket < len(bucket_router_v3_failure_cnts)):
            continue
        bucket_router_v3_failure_cnts[bucket] += 1
    
    cur.execute(
        '''
        SELECT block_number
        FROM uniswap_universal_router_txns r
        WHERE failed = TRUE
        '''
    )
    for (block_number,) in cur:
        block_ts = np.interp(block_number, block_ts_blocks, block_ts_timestamps)
        bucket = int((block_ts - min_timestamp) / step)
        if not (0 <= bucket < len(bucket_router_universal_failure_cnts)):
            continue
        bucket_router_universal_failure_cnts[bucket] += 1
    
    cur.execute(
        '''
        SELECT block_number
        FROM sushiswap_router_txns r
        WHERE failed = TRUE
        '''
    )
    for (block_number,) in cur:
        block_ts = np.interp(block_number, block_ts_blocks, block_ts_timestamps)
        bucket = int((block_ts - min_timestamp) / step)
        if not (0 <= bucket < len(bucket_sushiswap_failure_cnts)):
            continue
        bucket_sushiswap_failure_cnts[bucket] += 1
    
    return buckets, bucket_router_v2_failure_cnts, bucket_router_v3_failure_cnts, bucket_router_universal_failure_cnts, bucket_sushiswap_failure_cnts

failure_bucket_ts, router_v2_failure_cnts, router_v3_failure_cnts, router_universal_failure_cnts, sushiswap_failure_cnts = get_swap_failures()

# compute percentage failure rate
router_v2_failure_pct = np.divide(router_v2_failure_cnts, router_v2_cnts, dtype=float, where=router_v2_cnts != 0, out=np.zeros_like(router_v2_failure_cnts)) * 100
router_v3_failure_pct = np.divide(router_v3_failure_cnts, router_v3_cnts, dtype=float, where=router_v3_cnts != 0, out=np.zeros_like(router_v3_failure_cnts)) * 100
router_universal_failure_pct = np.divide(router_universal_failure_cnts, router_universal_cnts, dtype=float, where=router_universal_cnts != 0, out=np.zeros_like(router_universal_failure_cnts)) * 100
sushiswap_failure_pct = np.divide(sushiswap_failure_cnts, sushiswap_cnts, dtype=float, where=sushiswap_cnts != 0, out=np.zeros_like(sushiswap_failure_cnts)) * 100

uniswap_failure_rate = np.divide(
    router_v2_failure_cnts + router_v3_failure_cnts + router_universal_failure_cnts,
    router_v2_cnts + router_v3_cnts + router_universal_cnts,
    dtype=float,
    where=router_v2_cnts + router_v3_cnts + router_universal_cnts != 0,
    out=np.zeros_like(router_v2_failure_cnts)
) * 100
sushiswap_failure_rate = np.divide(
    sushiswap_failure_cnts,
    sushiswap_cnts,
    dtype=float,
    where=sushiswap_cnts != 0,
    out=np.zeros_like(sushiswap_failure_cnts)
) * 100

# plot the bucketed counts
xs_dts = [datetime.datetime.utcfromtimestamp(x) for x in failure_bucket_ts]

# sample every 10th point
xs_dts = xs_dts[::10]
uniswap_failure_rate = uniswap_failure_rate[::10]
sushiswap_failure_rate = sushiswap_failure_rate[::10]

# plt.plot(xs_dts, router_v2_failure_pct, label='Uniswap V2 Router 2')
# plt.plot(xs_dts, router_v3_failure_pct, label='Uniswap V3 Router 2')
# plt.plot(xs_dts, router_universal_failure_pct, label='Uniswap Universal Router')
# plt.plot(xs_dts, sushiswap_failure_pct, label='Sushiswap Router')
plt.plot(xs_dts, uniswap_failure_rate, label='Uniswap Routers')
plt.plot(xs_dts, sushiswap_failure_rate, label='Sushiswap Router')
plt.xlabel('Time')
plt.ylabel('Failure rate (%)')
plt.title('Percentage of router transactions that failed')
plt.legend()
# rotate the x axis labels
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('plots/router_txn_failure_rate.png')
print('Saved plots/router_txn_failure_rate.png')
plt.cla()
plt.clf()


# plot the slippage histograms over time
# @cache
def get_slippage_histograms():
    min_timestamp = datetime.datetime(year=2023, month=3, day=1).timestamp()
    max_timestamp = datetime.datetime(year=2023, month=4, day=1).timestamp()
    time_buckets, t_step = np.linspace(min_timestamp, max_timestamp, 100, retstep=True)

    percentage_buckets, pct_step = np.linspace(-0.5, 5, 110, retstep=True)

    uniswap_universal_router_slippage_histogram = np.zeros((len(percentage_buckets), len(time_buckets)))
    sushiswap_router_slippage_histogram = np.zeros((len(percentage_buckets), len(time_buckets)))

    cur.execute(
        '''
        SELECT block_number, slippage, router
        FROM slippage_results r
        WHERE slippage IS NOT NULL
        '''
    )
    for (block_number, slippage, router) in cur:
        block_ts = np.interp(block_number, block_ts_blocks, block_ts_timestamps)
        time_bucket = int((block_ts - min_timestamp) / t_step)
        slippage = float(slippage)
        if np.isnan(slippage):
            continue
        if not (0 <= time_bucket < len(time_buckets)):
            continue
        pct_bucket = int((slippage - percentage_buckets[0]) / pct_step)
        if not (0 <= pct_bucket < len(percentage_buckets)):
            continue

        if router.startswith('UniswapUniversal'):
            uniswap_universal_router_slippage_histogram[pct_bucket, time_bucket] += 1
        elif router.startswith('Sushiswap'):
            sushiswap_router_slippage_histogram[pct_bucket, time_bucket] += 1

    return (time_buckets, percentage_buckets), \
        uniswap_universal_router_slippage_histogram, \
        sushiswap_router_slippage_histogram

(slippage_buckets, percentage_buckets), \
    uniswap_universal_router_slippage_histogram, \
    sushiswap_router_slippage_histogram = get_slippage_histograms()

if not os.path.exists('plot_data/fig5'):
    os.mkdir('plot_data/fig5')


# change font size
plt.rcParams.update({'font.size': 16})

for hist, name in zip(
    [uniswap_universal_router_slippage_histogram, sushiswap_router_slippage_histogram],
    ['Uniswap Router', 'Sushiswap Router']
    ):

    # dump the plot data
    if not os.path.exists(f'plot_data/fig5/{name.replace(" ", "_").lower()}'):
        os.mkdir(f'plot_data/fig5/{name.replace(" ", "_").lower()}')
    with open(f'plot_data/fig5/{name.replace(" ", "_").lower()}/slippage_hist.csv', mode='w') as fout:
        fout.write('slippage (percent),time,count\n')
        for i, j in np.ndindex(hist.shape):
            # need to translate the indices to the actual values
            ts = slippage_buckets[j]
            # convert to datetime
            dt = datetime.datetime.utcfromtimestamp(ts)
            fout.write(f'{percentage_buckets[i]},{dt.isoformat()},{hist[i, j]}\n')

    # crop the histogram to the time range we care about
    hist = hist[:, :-1]
    im = plt.imshow(
        hist,
        origin='lower',
        norm=LogNorm(clip=True),
        interpolation='nearest', aspect='auto',
        cmap='inferno'
    )
    cb = plt.colorbar(im)
    # reduce font size on colorbar
    cb.ax.tick_params(labelsize=14)
    n_xticks = 8
    plt.xticks(
        np.linspace(0, len(hist[0]), n_xticks),
        [datetime.datetime.utcfromtimestamp(x).strftime('%m-%d') for x in np.linspace(slippage_buckets[0], slippage_buckets[-1], n_xticks)],
        rotation=45,
        ha='right',
        size=16,
    )
    n_yticks = int((5 - (-0.5)) / 0.5) + 1
    plt.yticks(
        np.linspace(0, len(percentage_buckets), n_yticks),
        [f'{x:.1f}' for x in np.linspace(percentage_buckets[0], percentage_buckets[-1], n_yticks)],
        size=16
    )

    plt.xlabel('Time')
    plt.ylabel('Slippage (%)')
    plt.title(f'{name} Slippages')
    # remove all splines
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)
    plt.gca().spines['bottom'].set_visible(False)
    plt.gca().spines['left'].set_visible(False)
    plt.tight_layout()
    plt.savefig(f'plots/{name.replace(" ", "_").lower()}_slippage_hist.png', dpi=300)
    plt.savefig(f'plots/{name.replace(" ", "_").lower()}_slippage_hist.pdf')
    plt.savefig(f'plots/{name.replace(" ", "_").lower()}_slippage_hist.eps')
    print(f'Saved plots/{name.replace(" ", "_").lower()}_slippage_hist.png')
    plt.cla()
    plt.clf()

# clear
plt.clf()

@cache
def get_slippage_protector_triggers():
    min_timestamp = datetime.datetime(year=2023, month=3, day=1).timestamp()
    max_timestamp = datetime.datetime(year=2023, month=4, day=1).timestamp()
    buckets, step = np.linspace(min_timestamp, max_timestamp, 100, retstep=True)

    uniswap_v2_router_2_slippage_protector_cnts = np.zeros_like(buckets)
    uniswap_v3_router_2_slippage_protector_cnts = np.zeros_like(buckets)
    uniswap_universal_router_slippage_protector_cnts = np.zeros_like(buckets)
    sushiswap_router_slippage_protector_cnts = np.zeros_like(buckets)

    uniswap_v2_router_2_cnts = np.zeros_like(buckets)
    uniswap_v3_router_2_cnts = np.zeros_like(buckets)
    uniswap_universal_router_cnts = np.zeros_like(buckets)
    sushiswap_router_cnts = np.zeros_like(buckets)

    cur.execute(
        '''
        SELECT block_number, slippage_protector_triggered, router
        FROM slippage_results
        '''
    )

    for (block_number, slippage_protector_triggered, router) in cur:
        block_ts = np.interp(block_number, block_ts_blocks, block_ts_timestamps)
        bucket = int((block_ts - min_timestamp) / step)
        if not (0 <= bucket < len(buckets)):
            continue

        if router.startswith('UniswapV2'):
            uniswap_v2_router_2_cnts[bucket] += 1
            if slippage_protector_triggered:
                uniswap_v2_router_2_slippage_protector_cnts[bucket] += 1
        elif router.startswith('UniswapV3'):
            uniswap_v3_router_2_cnts[bucket] += 1
            if slippage_protector_triggered:
                uniswap_v3_router_2_slippage_protector_cnts[bucket] += 1
        elif router.startswith('UniswapUniversal'):
            uniswap_universal_router_cnts[bucket] += 1
            if slippage_protector_triggered:
                uniswap_universal_router_slippage_protector_cnts[bucket] += 1
        elif router.startswith('Sushiswap'):
            sushiswap_router_cnts[bucket] += 1
            if slippage_protector_triggered:
                sushiswap_router_slippage_protector_cnts[bucket] += 1

    uniswap_v2_slippage_protector_rate = np.divide(
        uniswap_v2_router_2_slippage_protector_cnts,
        uniswap_v2_router_2_cnts,
        dtype=float,
        where=uniswap_v2_router_2_cnts != 0,
        out=np.zeros_like(uniswap_v2_router_2_slippage_protector_cnts)
    ) * 100
    uniswap_v3_slippage_protector_rate = np.divide(
        uniswap_v3_router_2_slippage_protector_cnts,
        uniswap_v3_router_2_cnts,
        dtype=float,
        where=uniswap_v3_router_2_cnts != 0,
        out=np.zeros_like(uniswap_v3_router_2_slippage_protector_cnts)
    ) * 100
    uniswap_universal_slippage_protector_rate = np.divide(
        uniswap_universal_router_slippage_protector_cnts,
        uniswap_universal_router_cnts,
        dtype=float,
        where=uniswap_universal_router_cnts != 0,
        out=np.zeros_like(uniswap_universal_router_slippage_protector_cnts)
    ) * 100
    sushiswap_slippage_protector_rate = np.divide(
        sushiswap_router_slippage_protector_cnts,
        sushiswap_router_cnts,
        dtype=float,
        where=sushiswap_router_cnts != 0,
        out=np.zeros_like(sushiswap_router_slippage_protector_cnts)
    ) * 100

    return buckets, uniswap_v2_slippage_protector_rate, uniswap_v3_slippage_protector_rate, uniswap_universal_slippage_protector_rate, sushiswap_slippage_protector_rate

result = get_slippage_protector_triggers()
slippage_protector_buckets = result[0]
xs_dts = [datetime.datetime.utcfromtimestamp(x) for x in slippage_protector_buckets]

try:
    os.mkdir('plots/slippage_protector_rates')
except FileExistsError:
    pass

for rate, name in zip(result[1:], ['Uniswap Universal Router', 'Sushiswap Router']):
    plt.plot(xs_dts, rate, label=name)
    plt.xlabel('Time')
    plt.ylabel('Slippage protector trigger rate (%)')
    plt.title(f'Percentage of {name} transactions\nthat triggered the slippage protector')
    # rotate the x axis labels
    plt.xticks(rotation=45)
    # plt.tight_layout()
    plt.savefig(f'plots/slippage_protector_rates/{name.replace(" ", "_").lower()}_protector_rate.png')
    print(f'Saved plots/slippage_protector_rates/{name.replace(" ", "_").lower()}_protector_rate.png')
    plt.cla()
    plt.clf()

# uniswap_universal_router_slippage_protector_pct = np.divide(
#     uniswap_universal_router_slippage_protector_cnts,
#     uniswap_universal_router_cnts,
#     dtype=float,
#     where=uniswap_universal_router_cnts != 0,
#     out=np.zeros_like(uniswap_universal_router_slippage_protector_cnts)
# ) * 100

# xs_dts = [datetime.datetime.utcfromtimestamp(x) for x in slippage_protector_bucket_ts]
# plt.plot(xs_dts, uniswap_universal_router_slippage_protector_pct)
# plt.xlabel('Time')
# plt.ylabel('Slippage protector trigger rate (%)')
# plt.title('Percentage of Uniswap Universal Router transactions\nthat triggered the slippage protector')
# # rotate the x axis labels
# plt.xticks(rotation=45)
# plt.tight_layout()
# plt.savefig('plots/uniswap_universal_router_slippage_protector_trigger_rate.png')

# find the amount paid due to slippage protector triggers
@cache
def get_usd_lost_to_slippage_protector():
    min_timestamp = datetime.datetime(year=2023, month=3, day=1).timestamp()
    max_timestamp = datetime.datetime(year=2023, month=4, day=1).timestamp()
    buckets, step = np.linspace(min_timestamp, max_timestamp, 100, retstep=True)

    usd_lost = np.zeros_like(buckets)

    cur.execute(
        '''
        SELECT block_number, gas_used_usd
        FROM all_swaps
        WHERE exchange_type ilike 'uniswap%' and slippage_protector_triggered = TRUE
        '''
    )

    for (block_number, gas_used_usd) in cur:
        block_ts = np.interp(block_number, block_ts_blocks, block_ts_timestamps)
        bucket = int((block_ts - min_timestamp) / step)
        if not (0 <= bucket < len(buckets)):
            continue

        usd_lost[bucket] += float(gas_used_usd)
    
    return buckets, step, usd_lost

buckets, step, usd_lost = get_usd_lost_to_slippage_protector()

usd_lost_per_hour = usd_lost / step * 60 * 60

xs_dts = [datetime.datetime.utcfromtimestamp(x) for x in buckets]



plt.plot(xs_dts, usd_lost_per_hour)
plt.xlabel('Time')
plt.ylabel('USD lost')
plt.title('Uniswap, gas fees (USD) lost to slippage protector per hour')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('plots/uniswap_usd_lost_to_slippage_protector.png')
plt.cla()
