-- =============================================================================      
-- NoveoMart Retail Finance Accounting Setup  
-- File    :   sql/noveomart_setup.sql
-- Purpose :   Initial setup of Snowflake environment for NoveoMart project, including
--             warehouse, database, schemas, and stage for CSV uploads.
-- Run order: Run this setup script first before running the DDL and ELT scripts.
-- =============================================================================

USE ROLE sysadmin;

-- =============================================================================
-- SECTION — WAREHOUSE CREATION
-- =============================================================================

CREATE WAREHOUSE IF NOT EXISTS portfolio_compute_wh
    WAREHOUSE_SIZE      = 'X-SMALL'
    AUTO_SUSPEND        = 60
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'Single warehouse for all portfolio project workloads';

SHOW WAREHOUSES;

/* In production this would split into ingest, transform, and analyst warehouses

-- Ingestion warehouse (loading raw data)
CREATE WAREHOUSE IF NOT EXISTS nm_ingest_wh
    WAREHOUSE_SIZE      = 'SMALL'
    WAREHOUSE_TYPE      = 'STANDARD'
    AUTO_SUSPEND        = 60
    AUTO_RESUME         = TRUE
    MAX_CLUSTER_COUNT   = 1
COMMENT = 'Used for COPY INTO and Snowpipe ingestion for NoveoMart';

-- Transformation warehouse (dbt / ELT jobs)
CREATE WAREHOUSE IF NOT EXISTS nm_transform_wh
    WAREHOUSE_SIZE      = 'SMALL'
    WAREHOUSE_TYPE      = 'STANDARD'
    AUTO_SUSPEND        = 180
    MAX_CLUSTER_COUNT   = 2
    SCALING_POLICY      = 'ECONOMY'
COMMENT = 'Used for dbt runs and heavy transformation for NoveoMart';

-- Analyst warehouse (ad hoc queries)
CREATE WAREHOUSE IF NOT EXISTS nm_analyst_wh
    WAREHOUSE_SIZE      = 'SMALL'
    WAREHOUSE_TYPE      = 'STANDARD'
    AUTO_SUSPEND        = 60
    AUTO_RESUME         = TRUE
    MAX_CLUSTER_COUNT   = 3
    SCALING_POLICY      = 'STANDARD'
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'Used for analyst ad hoc queries for NoveoMart';

*/

-- =============================================================================
-- SECTION — DATABASE, SCHEMAS
-- =============================================================================

USE ROLE sysadmin;

-- create noveomart database
CREATE DATABASE IF NOT EXISTS noveomart;

-- change database name to noveomart_db
AlTER DATABASE IF EXISTS noveomart RENAME TO noveomart_db;

USE noveomart_db;

-- create raw schema
CREATE OR REPLACE SCHEMA noveomart_db.raw;  -- landed source data

-- create mart schema
CREATE OR REPLACE SCHEMA noveomart_db.mart; -- final BI-ready tables

-- create stage for CSV uploads
CREATE STAGE noveomart_db.raw.csv_stage
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"' NULL_IF = (''));

LIST @noveomart_db.raw.csv_stage;