import os
import psycopg
import dotenv

def connect_db() -> psycopg.Connection:
    host=os.environ.get('DB_HOST')
    user=os.environ.get('DB_USER')
    password=os.environ.get('DB_PASSWORD')
    database=os.environ.get('DB_DATABASE')
    port=int(os.environ.get('DB_PORT', '5432'))

    conn = psycopg.connect(
        host=host,
        user=user,
        password=password,
        dbname=database,
        port=port
    )
    conn.autocommit = False
    return conn


def main():
    dotenv.load_dotenv()
    print('Loading db schema...')
    conn = connect_db()
    curr = conn.cursor()

    create_schema(curr)

    conn.commit()

    print('Done!')



def create_schema(curr: psycopg.cursor):
    curr.execute(
        '''
        CREATE TABLE IF NOT EXISTS all_swaps (
            id SERIAL PRIMARY KEY,
            exchange_type TEXT NOT NULL,
            timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            timestamp_unixtime INTEGER NOT NULL,
            block_number INTEGER NOT NULL,
            txn_idx INTEGER NOT NULL,
            txn_hash TEXT NOT NULL,
            block_log_index INTEGER NOT NULL,
            seen_in_mempool BOOLEAN NOT NULL,
            pool TEXT NOT NULL,
            sender TEXT NOT NULL,
            recipient TEXT NOT NULL,
            origin TEXT NOT NULL,
            token0_symbol TEXT NOT NULL,
            token1_symbol TEXT NOT NULL,
            token0_address TEXT NOT NULL,
            token1_address TEXT NOT NULL,
            amount0 DECIMAL,
            amount1 DECIMAL,
            amount_usd DECIMAL,
            slippage DECIMAL,
            slippage_protector_triggered BOOLEAN,
            success BOOLEAN,
            fail_reason TEXT,
            gas_used_eth DECIMAL,
            gas_used_usd DECIMAL
        );

        CREATE INDEX IF NOT EXISTS idx_all_swaps_bn ON all_swaps (block_number);
        CREATE INDEX IF NOT EXISTS idx_all_swaps_exchange_type ON all_swaps (exchange_type);
        CREATE INDEX IF NOT EXISTS idx_all_swaps_txn_hash ON all_swaps (txn_hash);
        CREATE INDEX IF NOT EXISTS idx_all_swaps_ts ON all_swaps (timestamp);

        CREATE TABLE IF NOT EXISTS likely_bots (
            id SERIAL PRIMARY KEY,
            address TEXT NOT NULL,
            explanation TEXT DEFAULT ''
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_likely_bots_address ON likely_bots (address);

        CREATE TABLE IF NOT EXISTS transaction_timestamps (
            id SERIAL PRIMARY KEY,
            transaction_hash TEXT NOT NULL,
            timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL
        );

        CREATE INDEX IF NOT EXISTS tts_transaction_hash
            ON transaction_timestamps (transaction_hash);

        CREATE TABLE IF NOT EXISTS uniswap_v2_router_2_txns (
            id                       SERIAL NOT NULL,
            txn_hash                 BYTEA NOT NULL,
            block_number             INTEGER NOT NULL,
            failed BOOLEAN           NOT NULL,
            txn_idx INTEGER          NOT NULL,
            from_address             BYTEA NOT NULL,
            to_address               BYTEA NOT NULL,
            exact_input              BOOLEAN NOT NULL,
            input_token              BYTEA NOT NULL,
            output_token             BYTEA NOT NULL,
            amount_input             DECIMAL,
            amount_output            DECIMAL,
            amount_bound             DECIMAL,
            supports_fee_on_transfer BOOLEAN NOT NULL,
            pool                     BYTEA NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_uv2_r2_txns_bn ON uniswap_v2_router_2_txns (block_number);

        CREATE TABLE IF NOT EXISTS uniswap_v3_router_2_txns (
            id                       SERIAL NOT NULL,
            txn_hash                 BYTEA NOT NULL,
            block_number             INTEGER NOT NULL,
            failed BOOLEAN           NOT NULL,
            txn_idx INTEGER          NOT NULL,
            from_address             BYTEA NOT NULL,
            to_address               BYTEA NOT NULL,
            exact_input              BOOLEAN NOT NULL,
            input_token              BYTEA NOT NULL,
            output_token             BYTEA NOT NULL,
            amount_input             DECIMAL,
            amount_output            DECIMAL,
            amount_bound             DECIMAL,
            supports_fee_on_transfer BOOLEAN NOT NULL,
            pool                     BYTEA NOT NULL,
            pool_is_v2               BOOLEAN NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_uv3_r2_txns_bn ON uniswap_v3_router_2_txns (block_number);

        CREATE TABLE IF NOT EXISTS uniswap_universal_router_txns (
            id                       SERIAL NOT NULL,
            txn_hash                 BYTEA NOT NULL,
            block_number             INTEGER NOT NULL,
            failed BOOLEAN           NOT NULL,
            txn_idx INTEGER          NOT NULL,
            from_address             BYTEA NOT NULL,
            to_address               BYTEA NOT NULL,
            exact_input              BOOLEAN NOT NULL,
            input_token              BYTEA NOT NULL,
            output_token             BYTEA NOT NULL,
            amount_input             DECIMAL,
            amount_output            DECIMAL,
            amount_bound             DECIMAL,
            supports_fee_on_transfer BOOLEAN NOT NULL,
            pool                     BYTEA NOT NULL,
            pool_is_v2               BOOLEAN NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_uni_u_txns_bn ON uniswap_universal_router_txns (block_number);

        CREATE TABLE IF NOT EXISTS sushiswap_router_txns (
            id                       SERIAL NOT NULL,
            txn_hash                 BYTEA NOT NULL,
            block_number             INTEGER NOT NULL,
            failed BOOLEAN           NOT NULL,
            txn_idx INTEGER          NOT NULL,
            from_address             BYTEA NOT NULL,
            to_address               BYTEA NOT NULL,
            exact_input              BOOLEAN NOT NULL,
            input_token              BYTEA NOT NULL,
            output_token             BYTEA NOT NULL,
            amount_input             DECIMAL,
            amount_output            DECIMAL,
            amount_bound             DECIMAL,
            supports_fee_on_transfer BOOLEAN NOT NULL,
            pool                     BYTEA NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_sushi_txns_bn ON sushiswap_router_txns (block_number);

        CREATE TABLE IF NOT EXISTS slippage_results (
            id                           SERIAL NOT NULL PRIMARY KEY,
            block_number                 INTEGER NOT NULL,
            transaction_index            INTEGER NOT NULL,
            transaction_hash             TEXT NOT NULL,
            mempool_arrival_time         TIMESTAMP NOT NULL,
            queue_time_seconds           FLOAT NOT NULL,
            sender                       TEXT NOT NULL,
            recipient                    TEXT NOT NULL,
            router                       TEXT NOT NULL,
            pool                         TEXT NOT NULL,
            token_in                     TEXT NOT NULL,
            token_out                    TEXT NOT NULL,
            exact_in                     BOOLEAN NOT NULL,
            amount_in                    DECIMAL,
            amount_out                   DECIMAL,
            amount_in_usd                DECIMAL,
            amount_out_usd               DECIMAL,
            boundary_amount              DECIMAL NOT NULL,
            expected_amount              DECIMAL,
            success                      BOOLEAN NOT NULL,
            fail_reason                  TEXT DEFAULT '',
            slippage                     DOUBLE PRECISION NOT NULL,
            slippage_protector_triggered BOOLEAN NOT NULL,
            gas_used_eth                 DECIMAL,
            gas_used_usd                 DECIMAL
        );

        CREATE INDEX IF NOT EXISTS idx_slippage_results_bn ON slippage_results (block_number);

        CREATE TABLE IF NOT EXISTS block_timestamps (
            number INTEGER PRIMARY KEY,
            timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL
        );

        CREATE INDEX IF NOT EXISTS bt_timestamp ON block_timestamps (timestamp);

        CREATE TABLE IF NOT EXISTS selected_tokens (
            id SERIAL NOT NULL PRIMARY KEY,
            address BYTEA NOT NULL,
            tvl_rank INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS st_address ON selected_tokens USING HASH (address);

        CREATE TABLE IF NOT EXISTS consistent_pools (
            id SERIAL NOT NULL PRIMARY KEY,
            address TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS cp_address ON consistent_pools USING HASH (address);

        '''
    )

if __name__ == '__main__':
    main()
