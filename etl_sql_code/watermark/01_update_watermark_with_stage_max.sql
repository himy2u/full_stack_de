UPDATE etl_control.watermark
   SET high_watermark = CASE WHEN table_name = 'dwh.f_order' THEN (SELECT MAX(etl_time) FROM stage.stg_customer_orders)
                             WHEN table_name = 'dwh.f_order_line' THEN (SELECT MAX(etl_time) FROM stage.stg_customer_orders_products)
                         END
       ,updated_date = now()
;