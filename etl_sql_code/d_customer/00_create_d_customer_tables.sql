/*
create tables in wrk and dwh schemas for d_customer. It should contain all future alter statements etc, 
so it is possible to recreate table from scratch if necessery.
*/

BEGIN;

DROP TABLE IF EXISTS work.wrk_d_customer;
CREATE TABLE work.wrk_d_customer (
    customer_sk        INTEGER
    ,customer_id       INTEGER
    ,first_name        VARCHAR(50)
    ,last_name         VARCHAR(50)
    ,mobile            VARCHAR(20)
    ,email             VARCHAR(50)
    ,date_of_birth     DATE
    ,registration_ts   TIMESTAMP
    ,created_date      TIMESTAMP
    ,updated_date      TIMESTAMP
);

DROP TABLE IF EXISTS dwh.d_customer;
CREATE TABLE dwh.d_customer (
    customer_sk        SERIAL       PRIMARY KEY
    ,customer_id       INTEGER      NOT NULL
    ,first_name        VARCHAR(50)  NOT NULL
    ,last_name         VARCHAR(50)  NOT NULL
    ,mobile            VARCHAR(20)
    ,email             VARCHAR(50)
    ,date_of_birth     DATE
    ,registration_ts   TIMESTAMP
    ,created_date      TIMESTAMP
    ,updated_date      TIMESTAMP
    ,etl_created_date  TIMESTAMP    DEFAULT NOW()
    ,etl_updated_date  TIMESTAMP    DEFAULT NOW()
);

-- insert special surogate keys: -1 (UNKNOWN)
INSERT INTO dwh.d_customer (customer_sk,customer_id,first_name,last_name,mobile,email,date_of_birth,registration_ts,created_date,updated_date)
VALUES (-1, -1, 'UNKNOWN','UNKNOWN',NULL,NULL,NULL,NULL,NULL,NULL);

COMMIT;
