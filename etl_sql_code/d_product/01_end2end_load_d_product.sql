TRUNCATE TABLE work.wrk_d_product;

BEGIN;

-- insert new and updated things into work table
INSERT INTO work.wrk_d_product (product_sk, product_id, name, type, subtype, price, valid_from)
SELECT dim.product_sk
       ,stg_p.product_id
       ,UPPER(stg_p.product_name)
       ,UPPER(stg_t.product_type_name)
       ,UPPER(stg_s.product_subtype_name)
       ,stg_p.product_price
       ,CASE WHEN dim.product_sk IS NULL THEN '1900-01-01'::TIMESTAMP
             ELSE now()
         END AS valid_from
  FROM stage.stg_products stg_p
 INNER JOIN stage.stg_product_types stg_t ON stg_p.product_type_id = stg_t.product_type_id
 INNER JOIN stage.stg_product_subtypes stg_s ON stg_p.product_subtype_id = stg_s.product_subtype_id
  LEFT JOIN (SELECT * FROM dwh.d_product WHERE valid_to = '9999-01-01'::TIMESTAMP) dim 
         ON stg_p.product_id = dim.product_id
 WHERE (stg_p.etl_time > COALESCE(dim.etl_updated_date, '1990-01-01'::TIMESTAMP)
        OR stg_t.etl_time > COALESCE(dim.etl_updated_date, '1990-01-01'::TIMESTAMP)
        OR stg_s.etl_time > COALESCE(dim.etl_updated_date, '1990-01-01'::TIMESTAMP)
        )
;
-- close old records in dimension
UPDATE dwh.d_product
   SET valid_to = now() - interval '1 second'
       ,etl_updated_date = now()
 WHERE product_sk IN (SELECT DISTINCT product_sk FROM work.wrk_d_product WHERE product_sk IS NOT NULL)
;
-- add new records into dim
INSERT INTO dwh.d_product (product_id,name,type,subtype,price,valid_from)
SELECT product_id,name,type,subtype,price,valid_from
  FROM work.wrk_d_product
;

COMMIT;