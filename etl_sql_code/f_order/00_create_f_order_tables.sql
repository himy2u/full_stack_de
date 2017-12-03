BEGIN;

DROP TABLE IF EXISTS work.wrk_f_order;
CREATE TABLE work.wrk_f_order (
    order_id               INTEGER
    ,address_sk            INTEGER
    ,customer_sk           INTEGER
    ,payment_sk            INTEGER
    ,status_sk             INTEGER
    ,place_date            DATE
    ,paid_date             DATE
    ,total_order_price     NUMERIC(9,2)
    ,created_date          TIMESTAMP
    ,updated_date          TIMESTAMP
);


DROP TABLE IF EXISTS dwh.f_order CASCADE;
CREATE TABLE dwh.f_order (
    order_id               INTEGER       NOT NULL UNIQUE
    ,address_sk            INTEGER       REFERENCES dwh.d_address  (address_sk)
    ,customer_sk           INTEGER       REFERENCES dwh.d_customer (customer_sk)
    ,payment_sk            INTEGER       REFERENCES dwh.d_payment  (payment_sk)
    ,status_sk             INTEGER       REFERENCES dwh.d_status   (status_sk)
    ,place_date            DATE          NOT NULL
    ,paid_date             DATE
    ,total_order_price     NUMERIC(9,2)
    ,created_date          TIMESTAMP
    ,updated_date          TIMESTAMP
    ,etl_created_date      TIMESTAMP     DEFAULT NOW()
    ,etl_updated_date      TIMESTAMP     DEFAULT NOW()
);

COMMIT;