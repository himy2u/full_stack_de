TRUNCATE TABLE work.wrk_d_address;

INSERT INTO work.wrk_d_address
SELECT dim.address_sk, 
       stg.address_id, 
       UPPER(stg.addresse_line1),
       UPPER(stg.addresse_line2),
       UPPER(stg.city), 
       stg.zip, 
       UPPER(stg.country), 
       stg.updated_ts AS created_date, 
       stg.updated_ts AS updated_date
  FROM stage.stg_addresses stg
  LEFT JOIN dwh.d_address dim ON stg.address_id = dim.address_id
 WHERE stg.etl_time > COALESCE(dim.etl_updated_date, '1990-01-01'::TIMESTAMP)
;

-- insert/update into dwh
BEGIN;

INSERT INTO dwh.d_address (address_id,address_line1,address_line2,city,zip,country,created_date,updated_date,etl_created_date,etl_updated_date)
SELECT address_id,address_line1,address_line2,city,zip,country,created_date,updated_date,now() AS etl_created_date,now() AS etl_updated_date
  FROM work.wrk_d_address
 WHERE address_sk IS NULL;

UPDATE dwh.d_address d
   SET address_id = wrk.address_id,
       address_line1 = wrk.address_line1,
       address_line2 = wrk.address_line2,
       city = wrk.city,
       zip = wrk.zip,
       country = wrk.country,
       updated_date = wrk.updated_date,
       etl_updated_date = now()
  FROM (SELECT * FROM work.wrk_d_address WHERE address_sk IS NOT NULL) wrk
 WHERE d.address_sk = wrk.address_sk;

COMMIT;