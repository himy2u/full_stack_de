BEGIN;

DROP TABLE IF EXISTS work.wrk_d_payment;
CREATE TABLE work.wrk_d_payment (
    payment_sk           INTEGER
    ,payment_id          INTEGER
    ,name        VARCHAR(40)
    ,created_date        TIMESTAMP
    ,updated_date        TIMESTAMP
);

DROP TABLE IF EXISTS dwh.d_payment;
CREATE TABLE dwh.d_payment (
    payment_sk           SERIAL         PRIMARY KEY
    ,payment_id          INTEGER        NOT NULL
    ,name        VARCHAR(40)    NOT NULL
    ,created_date        TIMESTAMP
    ,updated_date        TIMESTAMP
    ,etl_created_date    TIMESTAMP      DEFAULT NOW()
    ,etl_updated_date    TIMESTAMP      DEFAULT NOW()
);

INSERT INTO dwh.d_payment (payment_sk,payment_id,name,created_date,updated_date)
VALUES (-1, -1, 'UNKNOWN', NULL, NULL);

COMMIT;