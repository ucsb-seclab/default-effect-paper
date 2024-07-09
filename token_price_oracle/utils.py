import datetime
import decimal
import typing
import web3

def get_block_timestamp(w3: web3.Web3, block_identifier: typing.Any) -> datetime.datetime:
    block = w3.eth.get_block(block_identifier, full_transactions=False)
    dt = datetime.datetime.fromtimestamp(block['timestamp']).replace(tzinfo=datetime.timezone.utc)
    return dt

def weighted_median(pts: typing.List[typing.Tuple[int, decimal.Decimal]]) -> decimal.Decimal:
    """
    Compute a weighted median.
    
    If the weighted median lies between points (unlikely!) then we pick the higher value of the two.

    Arguments:
        pts: a List of (weight, value) tuples
    """
    assert len(pts) > 0, 'must have more than 1 point to take median, got 0'

    pts_sorted = sorted(pts, key=lambda x: x[1])

    tot_weight = sum(x for x, _ in pts_sorted)
    
    middle = tot_weight / 2

    cumsum = 0
    for weight, pt in pts_sorted:
        if cumsum + weight >= middle:
            # we went too far, this point is the median!
            return pt

        cumsum += weight

    print('!!!!!!!!!!!!!!!')
    print('pts_sorted', pts_sorted)
    raise Exception('should be unreachable')
