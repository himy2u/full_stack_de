TRUNCATE TABLE work.wrk_f_order_line;

WITH watermarks AS (
    SELECT low_watermark    AS low
           ,high_watermark  AS high
      FROM etl_control.watermark
     WHERE table_name = 'dwh.f_order_line'
)
INSERT INTO work.wrk_f_order_line (order_id,product_sk,supplier_sk,quantity,updated_date)
SELECT stg.order_id                   AS order_id
       ,COALESCE(dp.product_sk, -1)   AS product_sk
       ,COALESCE(sd.supplier_sk, -1)  AS supplier_sk
       ,stg.quantity                  AS quantity
       ,stg.updated_ts                AS updated_date
  FROM stage.stg_customer_orders_products stg
  LEFT JOIN dwh.d_product dp ON stg.product_id = dp.product_id
  LEFT JOIN stage.stg_products sp ON  stg.product_id = sp.product_id
  LEFT JOIN stage.stg_suppliers ss ON ss.supplier_id = sp.supplier_id
  LEFT JOIN dwh.d_supplier sd ON sd.supplier_id = ss.supplier_id
WHERE stg.etl_time BETWEEN (SELECT low FROM watermarks) AND (SELECT high FROM watermarks)
      OR stg.order_id IN (SELECT table_id_col FROM etl_control.reproc_queue WHERE table_name = 'dwh.f_order_line')
;

BEGIN;

DELETE FROM dwh.f_order_line
 WHERE EXISTS (SELECT 1 
                 FROM work.wrk_f_order_line 
                WHERE dwh.f_order_line.order_id = work.wrk_f_order_line.order_id
                      AND dwh.f_order_line.product_sk = work.wrk_f_order_line.product_sk)
 ;

INSERT INTO dwh.f_order_line (order_id,product_sk,supplier_sk,quantity,updated_date)
SELECT order_id,product_sk,supplier_sk,quantity,updated_date
  FROM work.wrk_f_order_line
;

UPDATE etl_control.watermark
   SET low_watermark = high_watermark
 WHERE table_name = 'dwh.f_order_line'
;

COMMIT;
