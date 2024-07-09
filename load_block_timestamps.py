# loads the block that we presume the mempool txn was generated in

import functools
import multiprocessing
import time
from db import connect_db
from utils import connect_web3
import constants
import datetime
import dotenv

def main():
    dotenv.load_dotenv()
    db = connect_db()
    cur = db.cursor()

    cur.execute('SELECT FROM block_timestamps')
    if cur.fetchone() is None:
        print('Filling database with block timestamps...')
        n_blocks = constants.END_BLOCK - constants.START_BLOCK + 1
        t_start = time.time()
        to_commit = []
        with multiprocessing.Pool(70) as pool:
            for item in pool.imap_unordered(get_timestamp, range(constants.START_BLOCK, constants.END_BLOCK + 1)):
                to_commit.append(item)
                if len(to_commit) % 100 == 0:
                    # status update
                    speed = len(to_commit) / (time.time() - t_start)
                    print(f'Processed {len(to_commit):,} / {n_blocks:,} blocks - {len(to_commit) / n_blocks * 100:.2f}% ({speed:.2f} blocks / sec)')

        # commit
        with cur.copy('COPY block_timestamps (number, timestamp) FROM STDIN') as copy:
            for item in to_commit:
                copy.write_row(item)
        
        db.commit()
        print('Done filling database with block timestamps.')
    else:
        print('Block timestamps already loaded, skipping.')

def get_timestamp(block_number):
    return block_number, datetime.datetime.utcfromtimestamp(get_w3().eth.get_block(block_number)['timestamp'])


@functools.cache
def get_w3():
    return connect_web3()

if __name__ == '__main__':
    main()
