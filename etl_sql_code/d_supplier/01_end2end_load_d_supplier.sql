TRUNCATE TABLE work.wrk_d_supplier;

INSERT INTO work.wrk_d_supplier (supplier_id,name,phone,email,created_date,updated_date)
SELECT stg.supplier_id
       ,UPPER(stg.supplier_name)
       ,BTRIM(regexp_replace(stg.supplier_phone, '[\.x\(\)\+-]', '', 'gi'))
       ,stg.supplier_mail
       ,stg.updated_ts AS created_date
       ,stg.updated_ts AS updated_date
  FROM stage.stg_suppliers stg
  LEFT JOIN dwh.d_supplier dim ON stg.supplier_id = dim.supplier_id
 WHERE stg.etl_time > COALESCE(dim.etl_updated_date, '1990-01-01'::TIMESTAMP)
;


BEGIN;

INSERT INTO dwh.d_supplier (supplier_id,name,phone,email,created_date,updated_date,etl_created_date,etl_updated_date)
SELECT supplier_id,name,phone,email,created_date,updated_date,now() AS etl_created_date,now() AS etl_updated_date
  FROM work.wrk_d_supplier
 WHERE supplier_sk IS NULL;

UPDATE dwh.d_supplier dim
   SET supplier_id = wrk.supplier_id
       ,name = wrk.name
       ,phone = wrk.phone
       ,email = wrk.email
       ,updated_date = wrk.updated_date
       ,etl_updated_date = now()
  FROM (SELECT * FROM work.wrk_d_supplier WHERE supplier_sk IS NOT NULL) wrk
 WHERE dim.supplier_sk = wrk.supplier_sk;

COMMIT;