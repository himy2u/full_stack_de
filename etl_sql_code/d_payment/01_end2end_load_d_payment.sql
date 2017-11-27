TRUNCATE TABLE work.wrk_d_payment;

INSERT INTO work.wrk_d_payment (payment_id,name,created_date,updated_date)
SELECT stg.payment_met_id
       ,UPPER(stg.payment_met_name)
       ,stg.updated_ts AS created_date
       ,stg.updated_ts AS updated_date
  FROM stage.stg_payment_methods stg
  LEFT JOIN dwh.d_payment dim ON stg.payment_met_id = dim.payment_id
 WHERE stg.etl_time > COALESCE(dim.etl_updated_date, '1990-01-01'::TIMESTAMP)
;

BEGIN;

INSERT INTO dwh.d_payment (payment_id,name,created_date,updated_date,etl_created_date,etl_updated_date)
SELECT payment_id,name,created_date,updated_date,now() AS etl_created_date,now() AS etl_updated_date
  FROM work.wrk_d_payment 
 WHERE payment_sk IS NULL;

UPDATE dwh.d_payment dim
   SET payment_id = wrk.payment_id
       ,name = wrk.name
       ,updated_date = wrk.updated_date
       ,etl_updated_date = now()
  FROM (SELECT * FROM work.wrk_d_payment WHERE payment_sk IS NOT NULL) wrk
 WHERE dim.payment_sk = wrk.payment_sk;

COMMIT;