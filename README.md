## Prerequisites

Run a postgres server. Store the credentials in `.env` file.
Also place your web3 connection url into `.env` (only support websocket as-is, can be changed with some small code updates).

```
DB_HOST=...
DB_PORT=...
DB_USER=...
DB_PASSWORD=...
DB_DATABASE=...

WEB3_HOST=ws://..
```

Run using Python 3.11.4 on Ubuntu 22.04

Install python requirements:

```bash
pip install -r requirements.txt
```

## Collecting mempool timestamps

```bash
cd eth-mempool-listener-go;
while true; do
    NODE_ENDPOINT=ws://your_web3_server:8546 go run main.go;
    sleep 1;
done | while read p; do
    echo "$p,$(date +%s.%N)";
done | pv -i 10 > ../mempool_arrivals.csv
```

## Create the database

```bash
python3 db.py
```

## Load the mempool timestamps into the DB

```bash
python3 load_transaction_timestamps.py mempool_arrivals.csv
```

## Load block timestamps

NOTE: this is hard-coded to load timestamps within the study period.
Check `constants.py`.

```bash
python3 load_block_timestamps.py
```

## Scrape the router-based swap transactions

The router handles slippage, so this helps us determine the tolerance level set.

```
python3 scrape_router_swap_txns.py
```

## Compute slippages

```bash
python3 compute_slippages.py
```

## Fetch all other non-router swaps

assembles the `all_swaps` table

```bash
python3 load_all_swap_txns.py
```

## Dump all_swaps table to csv for further analysis

```bash
COPY (SELECT * FROM all_swaps) TO '/tmp/all_swaps.csv' WITH CSV DELIMITER ',' HEADER;
```

## Dump some statistics and plots

```bash
python3 process_data.py
python3 plot.py
```
