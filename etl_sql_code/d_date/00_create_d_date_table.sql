BEGIN;

DROP TABLE IF EXISTS dwh.d_date;
CREATE TABLE dwh.d_date (
    date_sk             INTEGER      PRIMARY KEY
    ,date_actual        DATE         NOT NULL
    ,day_name           VARCHAR(9)   NOT NULL
    ,day_of_week        INTEGER      NOT NULL
    ,day_of_month       INTEGER      NOT NULL
    ,day_of_quarter     INTEGER      NOT NULL
    ,day_of_year        INTEGER      NOT NULL
    ,week_of_month      INTEGER      NOT NULL
    ,week_of_year       INTEGER      NOT NULL
    ,month_actual       INTEGER      NOT NULL
    ,month_name         VARCHAR(9)   NOT NULL
    ,quarter_actual     INTEGER      NOT NULL
    ,quarter_name       VARCHAR(9)   NOT NULL
    ,year_actual        INTEGER      NOT NULL
);

CREATE INDEX d_date_date_actual_idx ON dwh.d_date(date_actual);

INSERT INTO dwh.d_date (date_sk,date_actual,day_name,day_of_week,day_of_month,day_of_quarter,day_of_year,week_of_month,week_of_year,month_actual,month_name,quarter_actual,quarter_name,year_actual)
SELECT TO_CHAR(datum,'yyyymmdd')::INT                     AS date_sk,
       datum                                              AS date_actual,
       TO_CHAR(datum,'Day')                               AS day_name,
       EXTRACT(isodow FROM datum)                         AS day_of_week,
       EXTRACT(DAY FROM datum)                            AS day_of_month,
       datum - DATE_TRUNC('quarter',datum)::DATE +1       AS day_of_quarter,
       EXTRACT(doy FROM datum)                            AS day_of_year,
       TO_CHAR(datum,'W')::INT                            AS week_of_month,
       EXTRACT(week FROM datum)                           AS week_of_year,
       EXTRACT(MONTH FROM datum)                          AS month_actual,
       TO_CHAR(datum,'Month')                             AS month_name,
       EXTRACT(quarter FROM datum)                        AS quarter_actual,
       CASE
         WHEN EXTRACT(quarter FROM datum) = 1 THEN 'Q1'
         WHEN EXTRACT(quarter FROM datum) = 2 THEN 'Q2'
         WHEN EXTRACT(quarter FROM datum) = 3 THEN 'Q3'
         WHEN EXTRACT(quarter FROM datum) = 4 THEN 'Q4'
       END                                                AS quarter_name,
       EXTRACT(isoyear FROM datum)                        AS year_actual
FROM (SELECT '1970-01-01'::DATE+ SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES (0,29219) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;

COMMIT;
