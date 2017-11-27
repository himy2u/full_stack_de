TRUNCATE TABLE work.wrk_d_status;

INSERT INTO work.wrk_d_status (status_id,name,created_date,updated_date)
SELECT stg.order_status_id
       ,UPPER(stg.order_status_name)
       ,stg.updated_ts AS created_date
       ,stg.updated_ts AS updated_date
  FROM stage.stg_order_status stg
  LEFT JOIN dwh.d_status dim ON stg.order_status_id = dim.status_id
 WHERE stg.etl_time > COALESCE(dim.etl_updated_date, '1990-01-01'::TIMESTAMP)
;


BEGIN;

INSERT INTO dwh.d_status (status_id,name,created_date,updated_date,etl_created_date,etl_updated_date)
SELECT status_id,name,created_date,updated_date,now() AS etl_created_date,now() AS etl_updated_date
  FROM work.wrk_d_status 
 WHERE status_sk IS NULL;

UPDATE dwh.d_status dim
   SET status_id = wrk.status_id
       ,name = wrk.name
       ,updated_date = wrk.updated_date
       ,etl_updated_date = now()
  FROM (SELECT * FROM work.wrk_d_status WHERE status_sk IS NOT NULL) wrk
 WHERE dim.status_sk = wrk.status_sk;

COMMIT;
