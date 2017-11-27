TRUNCATE TABLE work.wrk_d_customer;

-- insert into working and perform any cleaning/transformations
INSERT INTO work.wrk_d_customer (customer_sk,customer_id,first_name,last_name,mobile,email,date_of_birth,registration_ts,created_date,updated_date)
SELECT dim.customer_sk, 
       stg.customer_id, 
       UPPER(stg.first_name),
       UPPER(stg.last_name),
       BTRIM(regexp_replace(stg.mobile_no, '[\.x\(\)\+-]', '', 'gi')) AS mobile, 
       stg.email, 
       stg.date_of_birth, 
       stg.registration_ts,
       stg.updated_ts AS created_date,
       stg.updated_ts AS updated_date
  FROM stage.stg_customers stg
  LEFT JOIN dwh.d_customer dim ON stg.customer_id = dim.customer_id
 WHERE stg.etl_time > COALESCE(dim.etl_updated_date, '1900-01-01'::timestamp)
;

-- insert/update into dwh
BEGIN;

INSERT INTO dwh.d_customer (customer_id,first_name,last_name,mobile,email,date_of_birth,registration_ts,created_date,updated_date,etl_created_date,etl_updated_date)
SELECT customer_id,first_name,last_name,mobile,email,date_of_birth,registration_ts,created_date,updated_date,now() AS etl_created_date,now() AS etl_updated_date
  FROM work.wrk_d_customer
 WHERE customer_sk IS NULL;

UPDATE dwh.d_customer d
   SET customer_id = wrk.customer_id
       ,first_name = wrk.first_name
       ,last_name = wrk.last_name
       ,mobile = wrk.mobile
       ,email = wrk.email
       ,date_of_birth = wrk.date_of_birth
       ,registration_ts = wrk.registration_ts
       ,updated_date = wrk.updated_date
       ,etl_updated_date = now()
  FROM (SELECT * FROM work.wrk_d_customer WHERE customer_sk IS NOT NULL) wrk
 WHERE d.customer_sk = wrk.customer_sk;

COMMIT;
