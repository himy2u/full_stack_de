BEGIN;

DROP TABLE IF EXISTS work.wrk_d_supplier;
CREATE TABLE work.wrk_d_supplier (
    supplier_sk            INTEGER
    ,supplier_id           INTEGER
    ,name                  VARCHAR(40)
    ,phone                 VARCHAR(20)
    ,email                 VARCHAR(50)
    ,created_date          TIMESTAMP
    ,updated_date          TIMESTAMP
);

DROP TABLE IF EXISTS dwh.d_supplier;
CREATE TABLE dwh.d_supplier (
    supplier_sk            SERIAL         PRIMARY KEY
    ,supplier_id           INTEGER        NOT NULL
    ,name                  VARCHAR(40)    NOT NULL
    ,phone                 VARCHAR(20)
    ,email                 VARCHAR(50)
    ,created_date          TIMESTAMP
    ,updated_date          TIMESTAMP
    ,etl_created_date      TIMESTAMP      DEFAULT NOW()
    ,etl_updated_date      TIMESTAMP      DEFAULT NOW()
);

INSERT INTO dwh.d_supplier (supplier_sk,supplier_id,name,phone,email,created_date,updated_date)
VALUES (-1, -1, 'UNKNOWN', NULL, NULL, NULL, NULL);

COMMIT;