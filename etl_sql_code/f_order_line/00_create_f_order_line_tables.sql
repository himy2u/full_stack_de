BEGIN;

DROP TABLE IF EXISTS work.wrk_f_order_line;
CREATE TABLE work.wrk_f_order_line (
    order_id               INTEGER
    ,product_sk            INTEGER
    ,supplier_sk           INTEGER
    ,quantity              INTEGER
    ,updated_date          TIMESTAMP
);

/*
Will add surogate key in table to have proper primary key. Otherwise, if for example
sth went wrong with loading dims, -1 will be assigned to skeys, therefore violation of 
composed primary key can occure.
*/
DROP TABLE IF EXISTS dwh.f_order_line;
CREATE TABLE dwh.f_order_line (
    order_line_sk          SERIAL        PRIMARY KEY
    ,order_id              INTEGER       REFERENCES dwh.f_order    (order_id)
    ,product_sk            INTEGER       REFERENCES dwh.d_product  (product_sk)
    ,supplier_sk           INTEGER       REFERENCES dwh.d_supplier (supplier_sk)
    ,quantity              INTEGER       NOT NULL
    ,updated_date          TIMESTAMP
    ,etl_updated_date      TIMESTAMP     DEFAULT NOW()
);

COMMIT;