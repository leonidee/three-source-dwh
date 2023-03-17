START TRANSACTION;

/*
Creating schema for each layer
*/
DROP SCHEMA IF EXISTS cdm CASCADE;
DROP SCHEMA IF EXISTS stg CASCADE;
DROP SCHEMA IF EXISTS dds CASCADE;

CREATE SCHEMA cdm;
CREATE SCHEMA stg;
CREATE SCHEMA dds;

/*
CDM LAYER
*/

DROP TABLE IF EXISTS cdm.dm_settlement_report;

-- CREATE TABLE cdm.dm_settlement_report
-- (
--     id                       serial         NOT NULL,
--     restaurant_id            varchar(250)   NOT NULL,
--     restaurant_name          varchar(250)   NOT NULL,
--     settlement_date          date           NOT NULL,
--     orders_count             integer        NOT NULL,
--     orders_total_sum         numeric(14, 2) NOT NULL,
--     orders_bonus_payment_sum numeric(14, 2) NOT NULL,
--     orders_bonus_granted_sum numeric(14, 2) NOT NULL,
--     order_processing_fee     numeric(14, 2) NOT NULL,
--     restaurant_reward_sum    numeric(14, 2) NOT NULL
-- );

-- --Adding constraints
-- ALTER TABLE cdm.dm_settlement_report
--     ADD PRIMARY KEY (id);
-- ALTER TABLE cdm.dm_settlement_report
--     ADD CHECK (settlement_date > '2022-01-01' AND settlement_date < '2500-01-01');
-- ALTER TABLE cdm.dm_settlement_report
--     ALTER COLUMN orders_count SET DEFAULT 0;

-- ALTER TABLE cdm.dm_settlement_report
--     ALTER COLUMN orders_total_sum SET DEFAULT 0;

-- ALTER TABLE cdm.dm_settlement_report
--     ALTER COLUMN orders_bonus_payment_sum SET DEFAULT 0;

-- ALTER TABLE cdm.dm_settlement_report
--     ALTER COLUMN orders_bonus_granted_sum SET DEFAULT 0;

-- ALTER TABLE cdm.dm_settlement_report
--     ALTER COLUMN order_processing_fee SET DEFAULT 0;

-- ALTER TABLE cdm.dm_settlement_report
--     ALTER COLUMN restaurant_reward_sum SET DEFAULT 0;

-- ALTER TABLE cdm.dm_settlement_report
--     ADD CHECK (orders_count >= 0);

-- ALTER TABLE cdm.dm_settlement_report
--     ADD CHECK (orders_total_sum >= (0)::numeric);

-- ALTER TABLE cdm.dm_settlement_report
--     ADD CHECK (orders_bonus_payment_sum >= (0)::numeric);

-- ALTER TABLE cdm.dm_settlement_report
--     ADD CHECK (orders_bonus_granted_sum >= (0)::numeric);

-- ALTER TABLE cdm.dm_settlement_report
--     ADD CHECK (order_processing_fee >= (0)::numeric);

-- ALTER TABLE cdm.dm_settlement_report
--     ADD CHECK (restaurant_reward_sum >= (0)::numeric);

-- ALTER TABLE cdm.dm_settlement_report
--     ADD UNIQUE (restaurant_id, settlement_date);

CREATE TABLE cdm.dm_settlement_report
(
    id                       serial
        PRIMARY KEY,
    restaurant_id            varchar(250)             NOT NULL,
    restaurant_name          varchar(250)             NOT NULL,
    settlement_date          date                     NOT NULL
        CONSTRAINT dm_settlement_report_settlement_date_check
            CHECK ((settlement_date > '2022-01-01'::date) AND (settlement_date < '2500-01-01'::date)),
    orders_count             integer        DEFAULT 0 NOT NULL
        CONSTRAINT dm_settlement_report_orders_count_check
            CHECK (orders_count >= 0),
    orders_total_sum         numeric(14, 2) DEFAULT 0 NOT NULL
        CONSTRAINT dm_settlement_report_orders_total_sum_check
            CHECK (orders_total_sum >= (0)::numeric),
    orders_bonus_payment_sum numeric(14, 2) DEFAULT 0 NOT NULL
        CONSTRAINT dm_settlement_report_orders_bonus_payment_sum_check
            CHECK (orders_bonus_payment_sum >= (0)::numeric),
    orders_bonus_granted_sum numeric(14, 2) DEFAULT 0 NOT NULL
        CONSTRAINT dm_settlement_report_orders_bonus_granted_sum_check
            CHECK (orders_bonus_granted_sum >= (0)::numeric),
    order_processing_fee     numeric(14, 2) DEFAULT 0 NOT NULL
        CONSTRAINT dm_settlement_report_order_processing_fee_check
            CHECK (order_processing_fee >= (0)::numeric),
    restaurant_reward_sum    numeric(14, 2) DEFAULT 0 NOT NULL
        CONSTRAINT dm_settlement_report_restaurant_reward_sum_check
            CHECK (restaurant_reward_sum >= (0)::numeric),
    UNIQUE (restaurant_id, settlement_date)
);

/*
STG LAYER
 */

-- service table
DROP TABLE IF EXISTS stg.srv_wf_settings;

CREATE TABLE IF NOT EXISTS stg.srv_wf_settings
(
    id               bigint GENERATED ALWAYS AS IDENTITY NOT NULL,
    workflow_key     varchar UNIQUE                      NOT NULL,
    workflow_setting json                                NOT NULL
);

-- postgres source
DROP TABLE IF EXISTS stg.bonussystem_ranks;
DROP TABLE IF EXISTS stg.bonussystem_users;
DROP TABLE IF EXISTS stg.bonussystem_events;

--ranks
CREATE TABLE stg.bonussystem_ranks
(
    id                    integer        NOT NULL UNIQUE,
    name                  varchar(2048)  NOT NULL,
    bonus_percent         numeric(19, 5) NOT NULL,
    min_payment_threshold numeric(19, 5) NOT NULL
);

--users
CREATE TABLE stg.bonussystem_users
(
    id            integer NOT NULL UNIQUE,
    order_user_id text    NOT NULL
);

--outbox
CREATE TABLE stg.bonussystem_events
(
    id          integer   NOT NULL UNIQUE,
    event_ts    timestamp NOT NULL,
    event_type  varchar   NOT NULL,
    event_value text      NOT NULL
);
CREATE INDEX idx_bonussystem_events_event_ts
    ON stg.bonussystem_events (event_ts);

-- mongo source

DROP TABLE IF EXISTS stg.ordersystem_users;
DROP TABLE IF EXISTS stg.ordersystem_orders;
DROP TABLE IF EXISTS stg.ordersystem_restaurants;

CREATE TABLE stg.ordersystem_users
(
    id           serial                      NOT NULL,
    object_id    varchar(2048)               NOT NULL UNIQUE,
    object_value text                        NOT NULL,
    update_ts    timestamp WITHOUT TIME ZONE NOT NULL
);

CREATE TABLE stg.ordersystem_orders
(
    id           serial                      NOT NULL,
    object_id    varchar(2048)               NOT NULL UNIQUE,
    object_value text                        NOT NULL,
    update_ts    timestamp WITHOUT TIME ZONE NOT NULL
);

CREATE TABLE stg.ordersystem_restaurants
(
    id           serial                      NOT NULL,
    object_id    varchar(2048)               NOT NULL UNIQUE,
    object_value text                        NOT NULL,
    update_ts    timestamp WITHOUT TIME ZONE NOT NULL
);

/*
DDS LAYER
 */

DROP TABLE IF EXISTS dds.dm_users;
DROP TABLE IF EXISTS dds.dm_restaurants;
DROP TABLE IF EXISTS dds.dm_products;
DROP TABLE IF EXISTS dds.dm_timestamps;
DROP TABLE IF EXISTS dds.dm_orders;
DROP TABLE IF EXISTS dds.fct_product_sales;

CREATE TABLE dds.dm_users
(
    id         serial  NOT NULL PRIMARY KEY,
    user_id    varchar NOT NULL UNIQUE,
    user_name  varchar NOT NULL,
    user_login varchar NOT NULL
);

CREATE TABLE dds.dm_restaurants
(
    id              serial    NOT NULL PRIMARY KEY,
    restaurant_id   varchar   NOT NULL UNIQUE,
    restaurant_name varchar   NOT NULL,
    active_from     timestamp NOT NULL,
    active_to       timestamp NOT NULL
);

CREATE TABLE dds.dm_products
(
    id            serial                      NOT NULL PRIMARY KEY,
    restaurant_id int                         NOT NULL,
    product_id    varchar                     NOT NULL,
    product_name  varchar                     NOT NULL,
    product_price numeric(14, 2) DEFAULT 0    NOT NULL,
    active_from   timestamp WITHOUT TIME ZONE NOT NULL,
    active_to     timestamp WITHOUT TIME ZONE NOT NULL,
    CONSTRAINT dm_products_price_check_gt CHECK (product_price >= 0),
    CONSTRAINT dm_products_price_check_lt CHECK (product_price <= 999000000000.99),
    UNIQUE (restaurant_id, product_id)

);

ALTER TABLE dds.dm_products
    ADD CONSTRAINT dm_products_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants (id);


CREATE TABLE dds.dm_timestamps
(
    id    serial    NOT NULL PRIMARY KEY,
    ts    timestamp NOT NULL UNIQUE,
    year  smallint  NOT NULL,
    month smallint  NOT NULL,
    day   smallint  NOT NULL,
    time  time      NOT NULL,
    date  date      NOT NULL,

    CONSTRAINT dm_timestamps_year_check CHECK (year >= 2022 AND year < 2500),
    CONSTRAINT dm_timestamps_month_check CHECK (month >= 1 AND month <= 12),
    CONSTRAINT dm_timestamps_day_check CHECK (day >= 1 AND day <= 31)
);

CREATE TABLE dds.dm_orders
(
    id            serial  NOT NULL PRIMARY KEY,
    order_key     varchar NOT NULL UNIQUE,
    order_status  varchar NOT NULL,
    user_id       int     NOT NULL,
    restaurant_id int     NOT NULL,
    timestamp_id  int     NOT NULL
);

ALTER TABLE dds.dm_orders
    ADD FOREIGN KEY (user_id) REFERENCES dds.dm_users (id);

ALTER TABLE dds.dm_orders
    ADD FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants (id);

ALTER TABLE dds.dm_orders
    ADD FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps (id);


CREATE TABLE dds.fct_product_sales
(
    id            serial         NOT NULL PRIMARY KEY,
    product_id    int            NOT NULL,
    order_id      int            NOT NULL,
    count         int            NOT NULL DEFAULT 0,
    price         numeric(19, 5) NOT NULL DEFAULT 0,
    total_sum     numeric(19, 5) NOT NULL DEFAULT 0,
    bonus_payment numeric(19, 5) NOT NULL DEFAULT 0,
    bonus_grant   numeric(19, 5) NOT NULL DEFAULT 0,

    CONSTRAINT fct_product_sales_count_check CHECK ( count >= 0 ),
    CONSTRAINT fct_product_sales_price_check CHECK ( price >= 0 ),
    CONSTRAINT fct_product_sales_total_sum_check CHECK ( total_sum >= 0 ),
    CONSTRAINT fct_product_sales_bonus_payment_check CHECK ( bonus_payment >= 0 ),
    CONSTRAINT fct_product_sales_bonus_grant_check CHECK ( bonus_grant >= 0 ),
    UNIQUE(product_id, order_id)
);

ALTER TABLE dds.fct_product_sales
    ADD FOREIGN KEY (product_id) REFERENCES dds.dm_products (id),
    ADD FOREIGN KEY (order_id) REFERENCES dds.dm_orders (id);

/*
SERVICE TABLES
*/
-- for STG layer
DROP TABLE IF EXISTS stg.srv_wf_settings;
CREATE TABLE IF NOT EXISTS stg.srv_wf_settings
(
    id               int GENERATED ALWAYS AS IDENTITY NOT NULL,
    workflow_key     varchar UNIQUE                      NOT NULL,
    workflow_settings json                                NOT NULL
);

-- for DDS layer
DROP TABLE IF EXISTS dds.srv_wf_settings;
CREATE TABLE IF NOT EXISTS dds.srv_wf_settings
(
    id               int GENERATED ALWAYS AS IDENTITY NOT NULL,
    workflow_key     varchar UNIQUE                      NOT NULL,
    workflow_settings json                                NOT NULL
);

END TRANSACTION;