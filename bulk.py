import typing
import multiprocessing
import web3
import web3.types
import collections
import time
import datetime

from utils import connect_web3
import constants

def _bulk_get_blocks(
    start_block: int, end_block: int
) -> typing.Iterable[web3.types.BlockData]:
    n_cpus = multiprocessing.cpu_count()
    progress_marks = collections.deque([(time.time(), 0)])
    last_progress_log = time.time()
    with multiprocessing.Pool(n_cpus, initializer=_init_worker) as pool:
        results = pool.imap_unordered(
            _get_block, range(start_block, end_block), chunksize=10
        )

        for i, result in enumerate(results):
            progress_marks.append((time.time(), i + 1))

            while (
                len(progress_marks) > 2 and progress_marks[1][0] < time.time() - 5 * 60
            ):
                progress_marks.popleft()

            if (
                progress_marks[-1][0] > last_progress_log + 10
                and len(progress_marks) > 2
            ):
                # log progress
                elapsed = progress_marks[-1][0] - progress_marks[0][0]
                speed = (progress_marks[-1][1] - progress_marks[0][1]) / elapsed
                n_remaining = (constants.END_BLOCK - constants.START_BLOCK) - progress_marks[-1][1]
                eta_s = n_remaining / speed
                eta = datetime.timedelta(seconds=eta_s)
                print(
                    f"Loaded {progress_marks[-1][1]:,} / {constants.END_BLOCK - constants.START_BLOCK:,} blocks - {progress_marks[-1][1] / (constants.END_BLOCK - constants.START_BLOCK) * 100:.2f}% - {speed:.2f} blocks / sec - ETA {eta}"
                )
                last_progress_log = progress_marks[-1][0]

            yield result


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


def _bulk_load_transactions(
    txn_hashes: typing.List[bytes],
) -> typing.List[web3.types.TxData]:
    n_cpus = multiprocessing.cpu_count()
    progress_marks = collections.deque([(time.time(), 0)])
    last_progress_log = time.time()

    ret = []

    with multiprocessing.Pool(n_cpus, initializer=_init_worker) as pool:
        results = pool.imap_unordered(_get_transaction, txn_hashes, chunksize=100)

        for i, result in enumerate(results):
            progress_marks.append((time.time(), i + 1))

            while (
                len(progress_marks) > 2 and progress_marks[1][0] < time.time() - 5 * 60
            ):
                progress_marks.popleft()

            if (
                progress_marks[-1][0] > last_progress_log + 10
                and len(progress_marks) > 2
            ):
                # log progress
                elapsed = progress_marks[-1][0] - progress_marks[0][0]
                speed = (progress_marks[-1][1] - progress_marks[0][1]) / elapsed
                n_remaining = len(txn_hashes) - progress_marks[-1][1]
                eta_s = n_remaining / speed
                eta = datetime.timedelta(seconds=eta_s)
                print(
                    f"Loaded {progress_marks[-1][1]:,} / {len(txn_hashes):,} txns - {progress_marks[-1][1] / len(txn_hashes) * 100:.2f}% - {speed:.2f} txns / sec - ETA {eta}"
                )
                last_progress_log = progress_marks[-1][0]

            ret.append(result)

    return ret

def _get_block(block_number: int) -> web3.types.BlockData:
    w3 = _get_w3()
    return w3.eth.get_block(block_number, full_transactions=True)


def _get_transaction_receipt(tx_hash: bytes) -> web3.types.TxReceipt:
    w3 = _get_w3()
    return w3.eth.get_transaction_receipt(tx_hash)


def _get_transaction(txn_hash: bytes) -> web3.types.TxData:
    w3 = _get_w3()
    return w3.eth.get_transaction(txn_hash)

_w3: web3.Web3 = None
def _init_worker(web3_client_q: multiprocessing.Queue = None):
    global _w3
    web3.WebsocketProvider._loop = None
    if web3_client_q is None:
        _w3 = connect_web3()
    else:
        _w3 = connect_web3(web3_client_q.get())


def _get_w3() -> web3.Web3:
    return _w3
