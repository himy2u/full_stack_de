BEGIN;

DROP TABLE IF EXISTS work.wrk_d_product;
CREATE TABLE work.wrk_d_product (
    product_sk             INTEGER
    ,product_id            INTEGER
    ,name                  VARCHAR(100)
    ,type                  VARCHAR(40)
    ,subtype               VARCHAR(40)
    ,price                 NUMERIC(9,2)
    ,valid_from            TIMESTAMP
    ,valid_to              TIMESTAMP
);

DROP TABLE IF EXISTS dwh.d_product;
CREATE TABLE dwh.d_product (
    product_sk             SERIAL         PRIMARY KEY
    ,product_id            INTEGER        NOT NULL
    ,name                  VARCHAR(100)   NOT NULL
    ,type                  VARCHAR(40)    NOT NULL
    ,subtype               VARCHAR(40)    NOT NULL
    ,price                 NUMERIC(9,2)   NOT NULL
    ,valid_from            TIMESTAMP      DEFAULT '1900-01-01'
    ,valid_to              TIMESTAMP      DEFAULT '9999-01-01'
    ,etl_created_date      TIMESTAMP      DEFAULT NOW()
    ,etl_updated_date      TIMESTAMP      DEFAULT NOW()
);

INSERT INTO dwh.d_product (product_sk,product_id,name,type,subtype,price)
VALUES (-1, -1, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 0.0);

COMMIT;