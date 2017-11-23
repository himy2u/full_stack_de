-- create users
CREATE USER "os_admin" WITH PASSWORD 'getme';
ALTER USER "os_admin" WITH LOGIN;
ALTER USER "os_admin" WITH SUPERUSER;

CREATE USER "etl_usr" WITH PASSWORD 'meget';
ALTER USER "etl_usr" WITH LOGIN;
ALTER USER "etl_usr" WITH SUPERUSER;

-- create database
DROP DATABASE IF EXISTS oceanrecords;
CREATE DATABASE oceanrecords;

-- create schemas
DROP SCHEMA IF EXISTS os;
CREATE SCHEMA os;

DROP SCHEMA IF EXISTS stage;
CREATE SCHEMA stage;