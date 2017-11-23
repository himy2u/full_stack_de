DROP TABLE IF EXISTS os.customers CASCADE;
CREATE TABLE os.customers (
    customer_id           SERIAL       PRIMARY KEY
    ,first_name           VARCHAR(50)  NOT NULL
    ,last_name            VARCHAR(50)  NOT NULL
    ,mobile_no            VARCHAR(20)
    ,email                VARCHAR(50)  NOT NULL
    ,date_of_birth        DATE
    ,registration_ts      TIMESTAMP    NOT NULL
    ,updated_ts           TIMESTAMP    DEFAULT NOW()
);

DROP TABLE IF EXISTS os.addresses CASCADE;
CREATE TABLE os.addresses (
    address_id            SERIAL       PRIMARY KEY
    ,addresse_line1       VARCHAR(100) NOT NULL
    ,addresse_line2       VARCHAR(100)
    ,city                 VARCHAR(50)  NOT NULL
    ,zip                  VARCHAR(20)  NOT NULL
    ,country              VARCHAR(60)  NOT NULL
    ,updated_ts           TIMESTAMP    DEFAULT NOW()
);

DROP TABLE IF EXISTS os.customers_addresses CASCADE;
CREATE TABLE os.customers_addresses (
    customer_address_id   SERIAL       PRIMARY KEY
    ,customer_id          INTEGER      REFERENCES os.customers (customer_id)
    ,address_id           INTEGER      REFERENCES os.addresses (address_id)
    ,updated_ts           TIMESTAMP    DEFAULT NOW()
);

DROP TABLE IF EXISTS os.payment_methods CASCADE;
CREATE TABLE os.payment_methods (
    payment_met_id        SERIAL       PRIMARY KEY
    ,payment_met_name     VARCHAR(40)  NOT NULL
    ,updated_ts           TIMESTAMP    DEFAULT NOW()
);

DROP TABLE IF EXISTS os.order_status CASCADE;
CREATE TABLE os.order_status (
    order_status_id       SERIAL       PRIMARY KEY
    ,order_status_name    VARCHAR(40)  NOT NULL
    ,updated_ts           TIMESTAMP    DEFAULT NOW()
);

DROP TABLE IF EXISTS os.customer_orders CASCADE;
CREATE TABLE os.customer_orders (
    order_id              SERIAL       PRIMARY KEY
    ,customer_address_id  INTEGER      NOT NULL       REFERENCES os.customers_addresses (customer_address_id)
    ,payment_met_id       INTEGER      NOT NULL       REFERENCES os.payment_methods (payment_met_id)
    ,order_status_id      INTEGER      NOT NULL       REFERENCES os.order_status (order_status_id)
    ,order_placed_date    DATE         NOT NULL
    ,order_paid_date      DATE
    ,total_order_price    NUMERIC(9,2) NOT NULL
    ,updated_ts           TIMESTAMP    DEFAULT NOW()
);

DROP TABLE IF EXISTS os.product_types CASCADE;
CREATE TABLE os.product_types (
    product_type_id       SERIAL       PRIMARY KEY
    ,product_type_name    VARCHAR(40)  NOT NULL
    ,updated_ts           TIMESTAMP    DEFAULT NOW()
);

DROP TABLE IF EXISTS os.product_subtypes CASCADE;
CREATE TABLE os.product_subtypes (
    product_subtype_id    SERIAL       PRIMARY KEY
    ,product_subtype_name VARCHAR(40)  NOT NULL
    ,updated_ts           TIMESTAMP    DEFAULT NOW()
);

DROP TABLE IF EXISTS os.suppliers CASCADE;
CREATE TABLE os.suppliers (
    supplier_id           SERIAL       PRIMARY KEY
   ,supplier_name         VARCHAR(40)  NOT NULL
   ,supplier_phone        VARCHAR(20)  NOT NULL
   ,supplier_mail         VARCHAR(50)  NOT NULL
   ,updated_ts            TIMESTAMP    DEFAULT NOW()
);

DROP TABLE IF EXISTS os.products CASCADE;
CREATE TABLE os.products (
    product_id            SERIAL       PRIMARY KEY
    ,product_name         VARCHAR(100) NOT NULL
    ,product_type_id      INTEGER      NOT NULL        REFERENCES os.product_types (product_type_id)
    ,product_subtype_id   INTEGER      NOT NULL        REFERENCES os.product_subtypes (product_subtype_id)
    ,supplier_id          INTEGER      NOT NULL        REFERENCES os.suppliers (supplier_id)
    ,product_price        NUMERIC(9,2) NOT NULL
    ,updated_ts           TIMESTAMP    DEFAULT NOW()
);

DROP TABLE IF EXISTS os.customer_orders_products CASCADE;
CREATE TABLE os.customer_orders_products (
    order_id              INTEGER      NOT NULL        REFERENCES os.customer_orders (order_id) 
    ,product_id           INTEGER      NOT NULL        REFERENCES os.products (product_id)
    ,quantity             INTEGER      NOT NULL
    ,updated_ts           TIMESTAMP    DEFAULT NOW()
    ,PRIMARY KEY(order_id, product_id)
);
