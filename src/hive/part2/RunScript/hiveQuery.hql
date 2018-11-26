DROP TABLE IF EXISTS airport_temp;

CREATE TABLE airport_temp (
iata String,
airport String,
city String,
state String,
country String,
lat int,
long Float)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)  
tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '${hiveconf:AIRPORTS_FILE_NAME}' OVERWRITE INTO TABLE airport_temp;

DROP TABLE IF EXISTS airport;

CREATE TABLE airport 
STORED AS ORC 
TBLPROPERTIES ("orc.compress"="SNAPPY")
as SELECT * FROM airport_temp;

DROP TABLE airport_temp; 

DROP TABLE IF EXISTS flying_temp;

CREATE TABLE flying_temp (
Year smallint,
Month tinyint,DayofMonth tinyint,DayOfWeek tinyint,DepTime smallint,CRSDepTime smallint,ArrTime smallint,CRSArrTime smallint,
UniqueCarrier String,FlightNum smallint,
TailNum String,ActualElapsedTime smallint,
CRSElapsedTime smallint,AirTime smallint,ArrDelay smallint,DepDelay smallint,
Origin String,Dest String,
Distance smallint,TaxiIn smallint,TaxiOut smallint,Cancelled smallint,CancellationCode String,Diverted smallint,CarrierDelay smallint,
WeatherDelay smallint,NASDelay smallint,SecurityDelay smallint,LateAircraftDelay smallint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '${hiveconf:FLYING_FILE_NAME}' OVERWRITE INTO TABLE flying_temp;

DROP TABLE IF EXISTS flying;

CREATE TABLE flying (
FlightDate timestamp, DayOfWeek tinyint,DepTime smallint,CRSDepTime smallint,ArrTime smallint,CRSArrTime smallint,
FlightNum smallint,
TailNum String,ActualElapsedTime smallint,
CRSElapsedTime smallint,AirTime smallint,ArrDelay smallint,DepDelay smallint,
Origin String,Dest String,
Distance smallint,TaxiIn smallint,TaxiOut smallint,Cancelled smallint,CancellationCode String,Diverted smallint,CarrierDelay smallint,
WeatherDelay smallint,NASDelay smallint,SecurityDelay smallint,LateAircraftDelay smallint
)
partitioned by(UniqueCarrier String)
STORED AS ORC 
TBLPROPERTIES ("orc.compress"="SNAPPY");

INSERT INTO flying 
PARTITION(UniqueCarrier)
SELECT  
to_date(concat_ws('-',cast(year as String), lpad(month,2,'0'), lpad(dayOfMonth,2,'0'))) as flightDate,
DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,FlightNum,TailNum,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay,UniqueCarrier
FROM flying_temp;

DROP TABLE flying_temp;

DROP TABLE IF EXISTS carrier_temp;

CREATE TABLE carrier_temp (
Code String,
Description String)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)  
tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '${hiveconf:CARRIERS_FILE_NAME}' OVERWRITE INTO TABLE carrier_temp;

DROP TABLE IF EXISTS carrier;

CREATE TABLE carrier 
STORED AS ORC 
TBLPROPERTIES ("orc.compress"="SNAPPY")
AS SELECT * FROM carrier_temp;

DROP TABLE carrier_temp;

select countCancelledQuery.UniqueCarrier, 
concat_ws(",", collect_set(countCancelledQuery.city)) cities,
sum(countCancelledQuery.countCancelled) cancelledFlights
from (
select /*+ MAPJOIN(airport)*/ f.UniqueCarrier, 
	   a.city, 
	   count(f.cancelled) countCancelled
  from airport a, 
       flying f
 where f.cancelled = 1
    and a.iata = f.Dest
 group by f.UniqueCarrier, a.city
 ) countCancelledQuery
 where countCancelledQuery.countCancelled > 1
 group by countCancelledQuery.UniqueCarrier
order by cancelledFlights desc; 
