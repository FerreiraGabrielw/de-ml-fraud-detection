DROP TABLE IF EXISTS fraud_predictions;

CREATE TABLE fraud_predictions (

    prediction_id BIGSERIAL PRIMARY KEY,

    amt DOUBLE PRECISION,
    city_pop INTEGER,

    lat DOUBLE PRECISION,
    long DOUBLE PRECISION,

    merch_lat DOUBLE PRECISION,
    merch_long DOUBLE PRECISION,

    unix_time BIGINT,

    transaction_hour INTEGER,
    is_weekend INTEGER,

    customer_age INTEGER,

    high_amount_flag INTEGER,
    night_transaction_flag INTEGER,

    customer_merchant_distance DOUBLE PRECISION,

    fraud_prediction INTEGER,
    fraud_probability DOUBLE PRECISION,

    inference_timestamp TIMESTAMP

);