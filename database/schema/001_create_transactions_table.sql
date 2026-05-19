DROP TABLE IF EXISTS transactions;

CREATE TABLE transactions (

    id BIGSERIAL PRIMARY KEY,
    trans_date_trans_time TIMESTAMP,
    cc_num BIGINT,
    merchant VARCHAR(255),
    category VARCHAR(100),
    amt NUMERIC(12,2),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    gender VARCHAR(10),
    street VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(10),
    zip VARCHAR(20),
    lat NUMERIC(10,6),
    long NUMERIC(10,6),
    city_pop INTEGER,
    job VARCHAR(255),
    dob DATE,
    trans_num VARCHAR(100),
    unix_time BIGINT,
    merch_lat NUMERIC(10,6),
    merch_long NUMERIC(10,6),
    is_fraud INTEGER,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);