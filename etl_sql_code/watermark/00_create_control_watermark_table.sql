BEGIN;

DROP TABLE IF EXISTS etl_control.watermark;
CREATE TABLE etl_control.watermark (
    table_name            VARCHAR
    ,low_watermark        TIMESTAMP    
    ,high_watermark       TIMESTAMP
    ,updated_date         TIMESTAMP
);

INSERT INTO etl_control.watermark (table_name, low_watermark, high_watermark, updated_date)
VALUES ('dwh.f_order', '1990-01-01'::TIMESTAMP, '1990-01-01'::TIMESTAMP, now())
       ,('dwh.f_order_line', '1990-01-01'::TIMESTAMP, '1990-01-01'::TIMESTAMP, now())
;

COMMIT;