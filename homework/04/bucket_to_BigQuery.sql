-- create the External tables

CREATE OR REPLACE EXTERNAL TABLE `dbt-siksha.my_nyc_tripdata.ext_fhv_taxi`
(
  dispatching_base_num STRING,
  pickup_datetime TIMESTAMP,
  dropoff_datetime TIMESTAMP,
  PULocationID INT64,
  DOLocationID INT64,
  SR_Flag STRING,
  Affiliated_base_number STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://module-4-bucket/fhv_tripdata_*.csv.gz'],
  compression = 'GZIP',
  skip_leading_rows = 1
);


CREATE OR REPLACE EXTERNAL TABLE  `dbt-siksha.my_nyc_tripdata.ext_yellow_taxi`
(
  VendorID INT64,
  tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  passenger_count INT64,
  trip_distance FLOAT64,
  RatecodeID INT64,
  store_and_fwd_flag STRING,
  PULocationID INT64,
  DOLocationID INT64,
  payment_type INT64,
  fare_amount FLOAT64,
  extra FLOAT64,
  mta_tax FLOAT64,
  tip_amount FLOAT64,
  tolls_amount FLOAT64,
  improvement_surcharge FLOAT64,
  total_amount FLOAT64,
  congestion_surcharge FLOAT64
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://module-4-bucket/yellow_tripdata_*.csv.gz'],
  compression = 'GZIP',
  skip_leading_rows = 1
);

CREATE OR REPLACE EXTERNAL TABLE `dbt-siksha.my_nyc_tripdata.ext_green_taxi` (
  VendorID INT64,
  lpep_pickup_datetime TIMESTAMP,
  lpep_dropoff_datetime TIMESTAMP,
  store_and_fwd_flag STRING,
  RatecodeID INT64,
  PULocationID INT64,
  DOLocationID INT64,
  passenger_count INT64,
  trip_distance FLOAT64,
  fare_amount FLOAT64,
  extra FLOAT64,
  mta_tax FLOAT64,
  tip_amount FLOAT64,
  tolls_amount FLOAT64,
  ehail_fee FLOAT64,
  improvement_surcharge FLOAT64,
  total_amount FLOAT64,
  payment_type INT64,
  trip_type INT64,
  congestion_surcharge FLOAT64
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://module-4-bucket/green_tripdata_*.csv.gz'],
  compression = 'GZIP',
  skip_leading_rows = 1
);

--  query to check count of rows

SELECT count(*) from my_nyc_tripdata.ext_green_taxi
SELECT count(*) from my_nyc_tripdata.ext_yellow_taxi
SELECT count(*) from my_nyc_tripdata.ext_fhv_taxi 

