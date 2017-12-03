DROP TABLE IF EXISTS etl_control.reproc_queue;
CREATE TABLE etl_control.reproc_queue (
    table_name          VARCHAR
    ,table_id_col       INTEGER
    ,created_date       TIMESTAMP
);