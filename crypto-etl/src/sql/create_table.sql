CREATE TABLE IF NOT EXISTS crypto(
    id SERIAL,
    coin_id TEXT,
    price_usd NUMERIC(20, 2),
    market_cap NUMERIC(20, 2),
    timestamp TEXT,
    PRIMARY KEY (coin_id, timestamp)
);