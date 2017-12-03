BEGIN;

DROP TABLE IF EXISTS work.wrk_d_status;
CREATE TABLE work.wrk_d_status (
    status_sk            INTEGER
    ,status_id           INTEGER
    ,name                VARCHAR(40)
    ,created_date        TIMESTAMP
    ,updated_date        TIMESTAMP
);

DROP TABLE IF EXISTS dwh.d_status CASCADE;
CREATE TABLE dwh.d_status (
    status_sk            SERIAL         PRIMARY KEY
    ,status_id           INTEGER        NOT NULL
    ,name                VARCHAR(40)    NOT NULL
    ,created_date        TIMESTAMP
    ,updated_date        TIMESTAMP
    ,etl_created_date    TIMESTAMP      DEFAULT NOW()
    ,etl_updated_date    TIMESTAMP      DEFAULT NOW()
);

INSERT INTO dwh.d_status (status_sk,status_id,name,created_date,updated_date)
VALUES (-1, -1, 'UNKNOWN', NULL, NULL);

COMMIT;