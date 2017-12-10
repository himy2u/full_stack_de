-- orders value: realized vs. pending vs. lost
SELECT DATE_TRUNC('month', place_date)
       ,SUM(CASE WHEN grp = 'realized' THEN total_order_price ELSE 0 END) AS realized_value
       ,SUM(CASE WHEN grp = 'pending' THEN total_order_price ELSE 0 END)  AS pending_value
       ,SUM(CASE WHEN grp = 'lost' THEN total_order_price ELSE 0 END)     AS lost_value
  FROM (SELECT CASE WHEN st.name IN ('PAID', 'SHIPPED') THEN 'realized'
                    WHEN st.name IN ('PURCHASE ORDER') THEN 'pending'
                    WHEN st.name IN ('REFUNDED', 'CLOSED') THEN 'lost'
                END AS grp
               , total_order_price
               , place_date
          FROM dwh.f_order f
         INNER JOIN dwh.d_status st ON st.status_sk = f.status_sk) d
 WHERE DATE_TRUNC('month', place_date)::DATE BETWEEN '2010-01-01' AND '2017-10-01'
 GROUP BY DATE_TRUNC('month', place_date)
 ORDER BY DATE_TRUNC('month', place_date) ASC
;

-- lost orders (returns and cancelation) value ratio
SELECT date, (lost/rest)*100 AS lost_orders_val_ratio
       ,30 - (lost/rest)*100 AS dev_from_goal
       ,0 AS goal
  FROM (SELECT DATE_TRUNC('month', place_date) AS date
               ,SUM(CASE WHEN st.name IN ('REFUNDED', 'CLOSED') THEN total_order_price ELSE 0 END)     AS lost
               ,SUM(CASE WHEN st.name NOT IN ('REFUNDED', 'CLOSED') THEN total_order_price ELSE 0 END) AS rest
          FROM dwh.f_order f
         INNER JOIN dwh.d_status st ON st.status_sk = f.status_sk
         GROUP BY DATE_TRUNC('month', place_date)) d
  ORDER BY date
;

-- sales by quarter
SELECT d.year_actual, d.quarter_name, SUM(total_order_price)
  FROM dwh.f_order f
 INNER JOIN dwh.d_date d ON d.date_actual = f.place_date
 WHERE status_sk IN (2,3) AND d.year_actual >= 2016
 GROUP BY d.quarter_name, d.year_actual
 ORDER BY d.year_actual, d.quarter_name ASC
;

-- top sales by subtypes last 30 days
SELECT p.subtype, SUM(fl.quantity*p.price)
  FROM dwh.f_order_line fl
 INNER JOIN dwh.d_product p ON p.product_sk = fl.product_sk
 INNER JOIN dwh.f_order f ON f.order_id = fl.order_id
 INNER JOIN dwh.d_date d ON d.date_actual = f.place_date
 WHERE status_sk IN (2,3) AND d.date_actual >= '2017-11-01'::DATE - interval '30 days'
 GROUP BY p.subtype
 ORDER BY SUM(fl.quantity*p.price) DESC
 LIMIT 10
;

-- top sales by product last 30 days
SELECT p.name, SUM(fl.quantity*p.price)
  FROM dwh.f_order_line fl
 INNER JOIN dwh.d_product p ON p.product_sk = fl.product_sk
 INNER JOIN dwh.f_order f ON f.order_id = fl.order_id
 INNER JOIN dwh.d_date d ON d.date_actual = f.place_date
 WHERE status_sk IN (2,3) AND d.date_actual >= '2017-11-01'::DATE - interval '30 days'
 GROUP BY p.name
 ORDER BY SUM(fl.quantity*p.price) DESC
 LIMIT 10
;

-- top countries
SELECT a.country, SUM(f.total_order_price)
  FROM dwh.f_order f
 INNER JOIN dwh.d_address a ON f.address_sk = a.address_sk
 INNER JOIN dwh.d_date d ON d.date_actual = f.place_date
 WHERE status_sk IN (2,3) AND d.date_actual >= '2017-11-01'::DATE - interval '30 days'
 GROUP BY a.country
 ORDER BY SUM(f.total_order_price) DESC
 LIMIT 10
;

-- no of customers per quarter
SELECT d.year_actual, d.quarter_name, COUNT(DISTINCT customer_sk)
  FROM dwh.f_order f
 INNER JOIN dwh.d_date d ON d.date_actual = f.place_date
 WHERE status_sk IN (2,3) AND d.year_actual >= 2016
 GROUP BY d.quarter_name, d.year_actual
 ORDER BY d.year_actual, d.quarter_name ASC
;

-- sales month-over-month growth: (This month - Last month) / (Last month)
SELECT date, sales AS cur_month, LAG(sales) OVER (ORDER BY date) AS prev_month
       , ROUND((sales- LAG(sales) OVER (ORDER BY date))/ LAG(sales) OVER (ORDER BY date),2) AS mom_growth
  FROM (SELECT DATE_TRUNC('month', place_date)::DATE AS date, SUM(total_order_price) AS sales
          FROM dwh.f_order f
         WHERE DATE_TRUNC('month', place_date)::DATE BETWEEN '2010-01-01' AND '2017-10-01'
               AND status_sk IN (2,3)
         GROUP BY DATE_TRUNC('month', place_date)::DATE
         ORDER BY DATE_TRUNC('month', place_date)::DATE) x
;

-- KPIs
SELECT d.date_actual
       ,f_sls.sales AS sales
       ,f_flmt.avg_fulfilment AS avg_fulfilment
       ,f_orsz.avg_order AS avg_order_val
       ,f_ldev.dev_from_goal AS avg_lost_dev_from_goal
  FROM dwh.d_date d
 INNER JOIN (SELECT place_date ,AVG(paid_date-place_date)::INT AS avg_fulfilment
               FROM dwh.f_order f
              WHERE status_sk = 3
              GROUP BY place_date) f_flmt
       ON f_flmt.place_date = d.date_actual
 INNER JOIN (SELECT place_date
                    ,ROUND(AVG(total_order_price), 2) AS avg_order
               FROM dwh.f_order
              GROUP BY place_date) f_orsz
       ON f_orsz.place_date = d.date_actual
 INNER JOIN (SELECT d.date, ROUND(30 - (lost/d.all)*100, 2) AS dev_from_goal
               FROM (SELECT place_date AS date
                           ,SUM(CASE WHEN st.name IN ('REFUNDED', 'CLOSED') THEN total_order_price ELSE 0 END) AS lost
                           ,SUM(total_order_price) AS all
                       FROM dwh.f_order f
                      INNER JOIN dwh.d_status st ON st.status_sk = f.status_sk
                      GROUP BY place_date) d) f_ldev
       ON f_ldev.date = d.date_actual
 INNER JOIN (SELECT place_date, SUM(total_order_price) AS sales
               FROM dwh.f_order
              WHERE status_sk IN (2,3)
              GROUP BY place_date) f_sls
       ON f_sls.place_date = d.date_actual
 ORDER BY d.date_actual ASC
;


-- sales by quarter
SELECT d.quarter_name||'/'||d.year_actual AS quarter, SUM(total_order_price) AS total_sales
  FROM dwh.f_order f
 INNER JOIN dwh.d_date d ON d.date_actual = f.place_date
 WHERE (d.year_actual = 2017 AND d.quarter_name IN ('Q1', 'Q2', 'Q3') OR d.year_actual = 2016 AND d.quarter_name = 'Q3')
       AND status_sk IN (2,3)
 GROUP BY d.quarter_name, d.year_actual
 ORDER BY d.year_actual, d.quarter_name
;

-- unique customers by quarter
SELECT d.quarter_name||'/'||d.year_actual AS quarter, COUNT(DISTINCT customer_sk) AS unique_customers_cnt
  FROM dwh.f_order f
 INNER JOIN dwh.d_date d ON d.date_actual = f.place_date
 WHERE (d.year_actual = 2017 AND d.quarter_name IN ('Q1', 'Q2', 'Q3') OR d.year_actual = 2016 AND d.quarter_name = 'Q3')
       AND status_sk IN (2,3)
 GROUP BY d.quarter_name, d.year_actual
 ORDER BY d.year_actual, d.quarter_name
;


/*
top X customers table with: total_orders_val, avr_order_val, no_orders, total_purchased_items 
only some statuses are taken into accountfd
*/
SELECT cust.first_name
       ,cust.last_name
       ,SUM(total_order_price) AS total_orders_val
       ,ROUND(AVG(total_order_price),2) AS avg_order_val
       ,COUNT(*) AS total_no_orders
       ,MAX(oli.prods_count) AS total_purchased_items
  FROM dwh.f_order f
 INNER JOIN dwh.d_customer cust ON cust.customer_sk = f.customer_sk
 INNER JOIN dwh.d_status st ON st.status_sk = f.status_sk
 INNER JOIN (SELECT customer_sk, SUM(quantity) AS prods_count
               FROM dwh.f_order_line ol
              INNER JOIN dwh.f_order o ON o.order_id = ol.order_id
              GROUP BY customer_sk) oli ON oli.customer_sk = f.customer_sk
 WHERE st.name IN ('PURCHASE ORDER', 'PAID', 'SHIPPED')
 GROUP BY cust.customer_sk
 ORDER BY SUM(total_order_price) DESC
;
