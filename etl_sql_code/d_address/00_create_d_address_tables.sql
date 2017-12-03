BEGIN;

DROP TABLE IF EXISTS work.wrk_d_address;
CREATE TABLE work.wrk_d_address (
    address_sk           INTEGER
    ,address_id          INTEGER
    ,address_line1       VARCHAR(100)
    ,address_line2       VARCHAR(100)
    ,city                VARCHAR(50)
    ,zip                 VARCHAR(20)
    ,country             VARCHAR(60)
    ,created_date        TIMESTAMP
    ,updated_date        TIMESTAMP
);

DROP TABLE IF EXISTS dwh.d_address CASCADE;
CREATE TABLE dwh.d_address (
    address_sk           SERIAL         PRIMARY KEY
    ,address_id          INTEGER        NOT NULL
    ,address_line1       VARCHAR(100)   NOT NULL
    ,address_line2       VARCHAR(100)
    ,city                VARCHAR(50)    NOT NULL
    ,zip                 VARCHAR(20)    NOT NULL
    ,country             VARCHAR(60)    NOT NULL
    ,created_date        TIMESTAMP
    ,updated_date        TIMESTAMP
    ,etl_created_date    TIMESTAMP      DEFAULT NOW()
    ,etl_updated_date    TIMESTAMP      DEFAULT NOW()
);

INSERT INTO dwh.d_address (address_sk,address_id,address_line1,address_line2,city,zip,country,created_date,updated_date)
VALUES (-1, -1, 'UNKNOWN', NULL, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', NULL, NULL);

COMMIT;
