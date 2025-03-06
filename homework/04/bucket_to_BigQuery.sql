-- create the External tables

CREATE OR REPLACE EXTERNAL TABLE my_nyc_tripdata.ext_fhv_taxi 
OPTIONS ( format = 'PARQUET', uris = ['gs://module-4-bucket/fhv/fhv_tripdata_2019-*.parquet'] );


CREATE OR REPLACE EXTERNAL TABLE my_nyc_tripdata.ext_green_taxi
OPTIONS (
    format = 'PARQUET',
    uris = [
        'gs://module-4-bucket/green/green_tripdata_2019-*.parquet',
        'gs://module-4-bucket/green/green_tripdata_2020-*.parquet'
    ]
);



CREATE OR REPLACE EXTERNAL TABLE my_nyc_tripdata.ext_yellow_taxi
OPTIONS (
    format = 'PARQUET',
    uris = [
        'gs://module-4-bucket/yellow/yellow_tripdata_2019-*.parquet',
        'gs://module-4-bucket/yellow/yellow_tripdata_2020-*.parquet'
    ]
);


--  query to check count of rows

SELECT count(*) from my_nyc_tripdata.ext_green_taxi
SELECT count(*) from my_nyc_tripdata.ext_yellow_taxi
SELECT count(*) from my_nyc_tripdata.ext_fhv_taxi 

