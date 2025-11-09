CREATE DATABASE testdb;
\c testdb;

CREATE SCHEMA IF NOT EXISTS testdb;

CREATE TABLE public.product (
    shop_code VARCHAR(20) NOT NULL,
    jan_code VARCHAR(13) NOT NULL,
    quantity INTEGER,
    price NUMERIC(10,2),
    created_date TIMESTAMP(6) DEFAULT CURRENT_DATE,
    updated_ts TIMESTAMP(6),
    expiry_ts TIMESTAMP(6),  -- timestamp without time zone
    product_name VARCHAR(100),
    status CHAR(1),
    description TEXT,
    CONSTRAINT pk_product PRIMARY KEY (shop_code, jan_code)
);

