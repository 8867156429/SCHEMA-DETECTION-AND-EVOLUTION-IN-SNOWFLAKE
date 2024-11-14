-- Scripts

-- case 1 csv files

USE DATABASE SCHEMA_CHANGE;
USE SCHEMA EMPLOYEE;

-- loaded csv and json in stg
SHOW STAGES;
LIST @"SCHEMA_CHANGE"."EMPLOYEE"."EMPLOYEE" ;

-- create file format
CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  PARSE_HEADER = True
  FIELD_DELIMITER = ','    ;

-- create  employee_table
CREATE OR REPLACE TABLE employee_table
USING TEMPLATE (
SELECT ARRAY_AGG(object_construct(*))
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION=>'@"SCHEMA_CHANGE"."EMPLOYEE"."EMPLOYEE"/employees.csv',
      FILE_FORMAT=>'my_csv_format'
    )   ));

select * from employee_table ;

-- check enable schema evolution on the table is Y or N
show tables ;

--  enable schema evolution on the table to Y
ALTER TABLE employee_table SET ENABLE_SCHEMA_EVOLUTION = TRUE;

-- load data into  employee_table
COPY INTO employee_table FROM
@"SCHEMA_CHANGE"."EMPLOYEE"."EMPLOYEE"/employees.csv
FILE_FORMAT = (FORMAT_NAME= 'my_csv_format', error_on_column_count_mismatch=false )
MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE ;


select * from employee_table ;


-- load data into  employee_table with phone number
COPY INTO employee_table FROM
@"SCHEMA_CHANGE"."EMPLOYEE"."EMPLOYEE"/employees_+_ph_no..csv
FILE_FORMAT = (FORMAT_NAME= 'my_csv_format' , error_on_column_count_mismatch=false )
MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE ;




-- case 1 jsom files

USE DATABASE SCHEMA_CHANGE;
USE SCHEMA EMPLOYEE;

-- loaded csv and json in stg
SHOW STAGES;
LIST @"SCHEMA_CHANGE"."EMPLOYEE"."EMPLOYEE" ;


-- Create a file format that sets the file type as Parquet.
CREATE OR REPLACE FILE FORMAT my_json_format
  TYPE = json,
  STRIP_OUTER_ARRAY=TRUE,
  date_format=auto,
  time_format=auto,
  timestamp_format=auto;


-- --Create a table from the file directly.
CREATE OR REPLACE TABLE employee_table_json
USING TEMPLATE (
SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
FROM TABLE(
INFER_SCHEMA(
LOCATION=>'@"SCHEMA_CHANGE"."EMPLOYEE"."EMPLOYEE"/employees_json.json',
FILE_FORMAT=>'my_json_format'
) ));

select * from employee_table_json;

show tables;
--enable for the schema evolution
-- This is how we do it.
Alter table employee_table_json set enable_schema_evolution=true;

-- Add data to the employee_table_json

COPY INTO employee_table_json
FROM '@"SCHEMA_CHANGE"."EMPLOYEE"."EMPLOYEE"/'
FILES=('employees_json.json')
FILE_FORMAT=my_json_format
match_by_column_name=case_insensitive
purge=true;


select * from employee_table_json;

-- update field to the employee_table_json

COPY INTO employee_table_json
FROM '@"SCHEMA_CHANGE"."EMPLOYEE"."EMPLOYEE"/'
FILES=('employees_json_with_ph_no.json')
FILE_FORMAT=my_json_format
match_by_column_name=case_insensitive
purge=true;

select * from employee_table_json;
