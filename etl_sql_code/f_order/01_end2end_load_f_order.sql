-- load into work
TRUNCATE TABLE work.wrk_f_order;

INSERT INTO work.wrk_f_order (order_id,address_sk,customer_sk,payment_sk,status_sk,place_date,paid_date,total_order_price,created_date,updated_date)
WITH watermarks AS (
    SELECT low_watermark    AS low
           ,high_watermark  AS high
      FROM etl_control.watermark
     WHERE table_name = 'dwh.f_order'
)
SELECT stg.order_id                     AS order_id
       ,COALESCE(addr.address_sk, -1)   AS adress_sk
       ,COALESCE(cst.customer_sk, -1)   AS customer_sk
       ,COALESCE(pay.payment_sk, -1)    AS payment_sk
       ,COALESCE(st.status_sk, -1)      AS status_sk
       ,stg.order_placed_date           AS place_date
       ,stg.order_paid_date             AS paid_date
       ,stg.total_order_price           AS total_order_amount
       ,stg.updated_ts                  AS created_date
       ,stg.updated_ts                  AS updated_date
  FROM stage.stg_customer_orders stg
  LEFT JOIN stage.stg_customers_addresses s_cst_adr ON s_cst_adr.customer_address_id = stg.customer_address_id
  LEFT JOIN dwh.d_address addr ON s_cst_adr.address_id = addr.address_id
  LEFT JOIN dwh.d_customer cst ON s_cst_adr.customer_id = cst.customer_id
  LEFT JOIN dwh.d_payment pay ON stg.payment_met_id = pay.payment_id
  LEFT JOIN dwh.d_status st ON stg.order_status_id = st.status_id
 WHERE stg.etl_time BETWEEN (SELECT low FROM watermarks) AND (SELECT high FROM watermarks)
       OR stg.order_id IN (SELECT table_id_col FROM etl_control.reproc_queue WHERE table_name = 'dwh.f_order')
;

BEGIN;

-- update changed rows in fact table
UPDATE dwh.f_order f
   SET address_sk = wrk.address_sk
       ,customer_sk = wrk.customer_sk
       ,payment_sk = wrk.payment_sk
       ,status_sk = wrk.status_sk
       ,place_date = wrk.place_date
       ,paid_date = wrk.paid_date
       ,total_order_price = wrk.total_order_price
       ,updated_date = wrk.updated_date
       ,etl_updated_date = now()
  FROM work.wrk_f_order wrk
 WHERE f.order_id = wrk.order_id
;

-- insert new fact rows
INSERT INTO dwh.f_order (order_id,address_sk,customer_sk,payment_sk,status_sk,place_date,paid_date,total_order_price,created_date,updated_date)
SELECT order_id,address_sk,customer_sk,payment_sk,status_sk,place_date,paid_date,total_order_price,created_date,updated_date
  FROM work.wrk_f_order
 WHERE NOT EXISTS (SELECT 1 FROM dwh.f_order WHERE dwh.f_order.order_id = work.wrk_f_order.order_id);
;

-- update high and low watermark so they are same again
UPDATE etl_control.watermark
   SET low_watermark = high_watermark
 WHERE table_name = 'dwh.f_order'
;

COMMIT;
