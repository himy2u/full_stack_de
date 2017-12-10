/* for OceanRecords operational database */
-- create user
CREATE USER "os_admin" WITH PASSWORD 'getme';
ALTER USER "os_admin" WITH LOGIN;
ALTER USER "os_admin" WITH SUPERUSER;

-- create database
DROP DATABASE IF EXISTS oceanrecords;
CREATE DATABASE oceanrecords;

-- create schema
DROP SCHEMA IF EXISTS os;
CREATE SCHEMA os;


/* for OceanRecords DWH */
CREATE USER "etl_usr" WITH PASSWORD 'meget';
ALTER USER "etl_usr" WITH LOGIN;
ALTER USER "etl_usr" WITH SUPERUSER;

CREATE USER "superset_usr" WITH PASSWORD 'sohard';
ALTER USER "superset_usr" WITH LOGIN;
GRANT USAGE ON SCHEMA dwh TO "superset_usr";
GRANT SELECT ON ALL TABLES IN SCHEMA dwh TO "superset_usr";

DROP SCHEMA IF EXISTS stage CASCADE;
CREATE SCHEMA stage;

DROP SCHEMA IF EXISTS work CASCADE;
CREATE SCHEMA work;

DROP SCHEMA IF EXISTS dwh CASCADE;
CREATE SCHEMA dwh;

DROP SCHEMA IF EXISTS etl_control CASCADE;
CREATE SCHEMA etl_control;

/* for Airflow */
DROP DATABASE IF EXISTS airflow;
CREATE DATABASE airflow;

CREATE USER "airflow" WITH PASSWORD 'airflow';
ALTER USER "airflow" WITH LOGIN;
ALTER USER "airflow" WITH SUPERUSER;