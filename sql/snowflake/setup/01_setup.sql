-- File: 01_setup.sql
-- Purpose: Create the database, medallion schemas, storage integration, and cleaned external stage.
-- When to run: First, before creating tables or running the pipeline for the first time.
-- Dependencies: ACCOUNTADMIN or a role with privileges to create integrations, stages, and schemas.
-- Params that require manual substitution: storage integration name or stage name if you prefer different names.
-- Documentation note: This file is reference/setup SQL for humans and is NOT executed by Python code.
-- UPDATE THIS: Replace the placeholder STORAGE_AWS_ROLE_ARN with the IAM role you intend Snowflake to assume.
-- UPDATE THIS: Replace the placeholder S3 bucket path with your own cleaned-data bucket/prefix.
-- UPDATE THIS: After running CREATE STORAGE INTEGRATION, run DESC INTEGRATION and copy the generated IAM user / external ID values into the AWS trust policy for the IAM role.

CREATE DATABASE IF NOT EXISTS SOCCER_ANALYTICS;

CREATE SCHEMA IF NOT EXISTS SOCCER_ANALYTICS.STAGING;
CREATE SCHEMA IF NOT EXISTS SOCCER_ANALYTICS.BRONZE;
CREATE SCHEMA IF NOT EXISTS SOCCER_ANALYTICS.SILVER;
CREATE SCHEMA IF NOT EXISTS SOCCER_ANALYTICS.GOLD;

CREATE OR REPLACE STORAGE INTEGRATION SPORT_ANALYSIS_S3_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<account-id>:role/<snowflake-storage-role>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<your-bucket-name>/cleaned/');

-- Run this after the integration is created.
-- The output includes IAM values that must be copied into the AWS trust relationship.
DESC INTEGRATION SPORT_ANALYSIS_S3_INT;

CREATE OR REPLACE STAGE SOCCER_ANALYTICS.STAGING.cleaned_stage
  URL = 's3://<your-bucket-name>/cleaned/'
  STORAGE_INTEGRATION = SPORT_ANALYSIS_S3_INT
  FILE_FORMAT = (
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  );

LIST @SOCCER_ANALYTICS.STAGING.cleaned_stage;
