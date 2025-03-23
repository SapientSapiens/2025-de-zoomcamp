## Question 1: Install Spark and PySpark ##

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output

### Answer: 3.3.2 ###

After Installing Saprk

![alt text](images/image.png)

After Creating local Spark session

![alt text](images/image-1.png)


## Question 2: Yellow October 2024 ##

Read the October 2024 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 6MB
- 25MB
- 75MB
- 100MB



![alt text](images/image-2.png)


### Answer: 25MB ###

## Question 3: Question 3: Count records ##

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

- 85,567
- 105,567
- 125,567
- 145,567

![alt text](images/image-3.png)

### Answer: closest matching is 125,567 ###

## Question 4: Longest trip ##

What is the length of the longest trip in the dataset in hours?

- 122
- 142
- 162
- 182

![alt text](images/image-4.png)

### Answer: 162 ###

## Question 5: User Interface ##

Spark’s User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- 4040
- 8080

![alt text](images/image-5.png)

### Answer: 4040 ###


## Question 6: Least frequent pickup location zone  ##

Load the zone lookup data into a temp view in Spark:

wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

Using the zone lookup data and the Yellow October 2024 data, what is the name of the LEAST frequent pickup location Zone?

- Governor's Island/Ellis Island/Liberty Island
- Arden Heights
- Rikers Island
- Jamaica Bay

![alt text](images/image-6.png)

### Answer: Governor's Island/Ellis Island/Liberty Island ###