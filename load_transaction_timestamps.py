from db import connect_db
import dotenv
import time
import datetime
import decimal
import gzip
import argparse

import sqlite3

from bloom_filter2 import BloomFilter

def main():
    dotenv.load_dotenv()
    parser = argparse.ArgumentParser()

    parser.add_argument('filename', type=str, help='The filename to load the transaction timestamps from')

    args = parser.parse_args()

    conn = connect_db()
    cur = conn.cursor()

    # safety check: did we already load the transaction timestamps?
    cur.execute('SELECT FROM transaction_timestamps LIMIT 1')
    if cur.fetchone() is not None:
        print('Transaction timestamps already loaded!')
        input('Continue anyway? (press enter to continue, ctrl+c to exit) ')

    print('Loading transaction timestamps...')
    time.sleep(3)

    # keep a rotating list of 4 buffers to avoid loading all transactions
    # rotate every 15 minutes
    dedupe_buffers = [set() for _ in range(4)]
    last_rotate = None

    txn_bloom = BloomFilter(max_elements=100000000, error_rate=0.01)

    insert_buffer = []
    insert_buffer_txns = set()

    print('starting read...')

    # iterate over the transactions
    with open(args.filename, 'r', buffering=1024 * 1024 * 100) as f:
        for i, line in enumerate(f):
            line = line.strip()
            if not line:
                continue

            txn_hash, timestamp = line.split(',')
            timestamp = float(timestamp)

            if i % 1000 == 0:
                print(f'Loaded {i} transactions ({datetime.datetime.utcfromtimestamp(timestamp)})')

            # check if we've seen this transaction before
            if any(txn_hash in buf for buf in dedupe_buffers):
                dedupe_buffers[-1].add(txn_hash)
                continue

            # add the transaction to the buffer
            dedupe_buffers[-1].add(txn_hash)

            # check if we need to rotate the buffers
            if last_rotate is None or timestamp - last_rotate > (15 * 60):
                last_rotate = timestamp
                dedupe_buffers = dedupe_buffers[1:] + [set()]

            #  check the insert buffer
            if txn_hash in insert_buffer_txns:
                continue

            # check the database for the transaction
            if txn_hash in txn_bloom:
                # we may have seen this transaction before
                cur.execute('SELECT FROM transaction_timestamps WHERE transaction_hash = %s', (txn_hash,))
                if cur.fetchone() is not None:
                    continue

            # add the transaction to the bloom filter
            txn_bloom.add(txn_hash)

            # insert the transaction into the database
            insert_buffer.append((txn_hash, timestamp))
            insert_buffer_txns.add(txn_hash)

            
            if len(insert_buffer) >= 10000:
                cur.executemany(
                    'INSERT INTO transaction_timestamps (transaction_hash, timestamp) VALUES (%s, %s)',
                    [
                        (txn_hash, datetime.datetime.utcfromtimestamp(timestamp))
                        for txn_hash, timestamp in insert_buffer
                    ]
                )
                insert_buffer.clear()
                insert_buffer_txns.clear()
                conn.commit()


    cur.executemany(
        'INSERT INTO transaction_timestamps (transaction_hash, timestamp) VALUES (?, ?)',
        (
            (txn_hash, datetime.datetime.utcfromtimestamp(timestamp))
            for txn_hash, timestamp in insert_buffer
        )
    )

    insert_buffer = []

    conn.commit()


if __name__ == '__main__':
    main()
