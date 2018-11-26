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

select /*+ MAPJOIN(carrier)*/ c.description, count(f.flightnum) 
from carrier c, flying f
where c.code = f.uniquecarrier
group by c.description;

select /*+ MAPJOIN(airport)*/ count(f.flightnum)
  from airport a, flying f
where a.city='New York'
  and month(f.flightDate) = 6
  and (a.iata = f.origin or a.iata = f.dest);
  
select /*+ MAPJOIN(airport)*/ a.iata, a.airport, a.city, a.state, a.country 
from ( 
select countQuery.airport, row_number() over (order by countQuery.count desc) rowNumber 
from ( 
select airport, count(*) count 
from flying f 
LATERAL VIEW explode(array(f.origin, f.dest)) listAirs as airport 
where f.flightdate >= to_date('2007-06-01') 
and f.flightdate <= to_date('2007-08-31') 
group by airport) countQuery 
) orderedRowsQuery, 
airport a 
where a.iata = orderedRowsQuery.airport 
and orderedRowsQuery.rowNumber <= 5; 
   
select /*+ MAPJOIN(carrier)*/ c.code, c.description
from (
select subCountQuery.UniqueCarrier, row_number() over (order by subCountQuery.count desc) rowNumber
from (
select UniqueCarrier, count(*) as count
  from flying f
 group by UniqueCarrier
) subCountQuery
) orderedRowsQuery,
  carrier c
where orderedRowsQuery.rowNumber = 1
  and c.code = orderedRowsQuery.UniqueCarrier;