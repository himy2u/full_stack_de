-- delete old (already re-processed) rows
DELETE FROM etl_control.reproc_queue
 WHERE table_name IN ('dwh.f_order', 'dwh.f_order_line')
;

-- insert new rows with -1 into the queue
INSERT INTO etl_control.reproc_queue (table_name, table_id_col, created_date)
SELECT 'dwh.f_order' AS table_name, order_id AS table_id_col, now() AS created_date
  FROM dwh.f_order
 WHERE address_sk = -1
       OR customer_sk = -1
       OR payment_sk = -1
       OR status_sk = -1
UNION ALL
SELECT DISTINCT 'dwh.f_order_line' AS table_name, order_id AS table_id_col, now() AS created_date
  FROM dwh.f_order_line
 WHERE product_sk = -1
       OR supplier_sk = -1
;