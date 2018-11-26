create temporary function agentparser AS 'AgentParserUDF';

DROP TABLE IF EXISTS city;

CREATE TABLE city (id SMALLINT, name STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n';

LOAD DATA LOCAL INPATH '${hiveconf:CITY_FILE_NAME}' OVERWRITE INTO TABLE city;

DROP TABLE IF EXISTS imp;

CREATE EXTERNAL TABLE imp (
BidID STRING,
Timestamp STRING,
LogType STRING,
iPinYouID STRING,
UserAgent STRING,
IP STRING,
RegionID SMALLINT,
CityID SMALLINT,
AdExchange STRING,
Domain STRING,
URL STRING,
AnonymousURL STRING,
AdSlotID STRING,
AdSlotWidth STRING,
AdSlotHeight STRING,
AdSlotVisibility STRING,
AdSlotFormat STRING,
AdSlotFloorPrice INT,
CreativeID STRING,
BiddingPrice INT,
PayingPrice INT,
LandingPageURL STRING,
AdvertiserID SMALLINT,
UserProfileIDs STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA LOCAL INPATH '${hiveconf:IMP_FILE_NAME}'
OVERWRITE INTO TABLE imp;

drop table if exists imp_partitioned;

CREATE TABLE imp_partitioned (
CityId SmallInt,
DeviceType STRING,
Browser STRING,
BrowserType STRING,
OS STRING);

INSERT OVERWRITE TABLE imp_partitioned 
SELECT 
CityId,
agentparser(useragent)[0],
agentparser(useragent)[1],
agentparser(useragent)[2],
agentparser(useragent)[3]
FROM imp;

select c.name, firstRowGroup.deviceType, firstRowGroup.browser, firstRowGroup.browserType, firstRowGroup.OS from (
select subQuery.cityId, row_number() over (partition by subQuery.cityId) rowNumber,
subQuery.deviceType, subQuery.browser, subQuery.browserType, subQuery.OS  from (
select cityId, 
deviceType, count(deviceType) over (partition by cityid, deviceType) countDeviceType,
browser, count(browser) over (partition by cityid, browser) countBrowser,
browserType, count(browserType) over (partition by cityid, browserType) countBrowserType,
OS, count(OS) over (partition by cityid, OS) countOS
from imp_partitioned imp
group by cityId, deviceType, browser, browserType, OS
order by cityid, countDeviceType desc, countBrowser desc, countBrowserType desc, countOS desc) subQuery
) firstRowGroup,
city c
where c.id = firstRowGroup.cityId and firstRowGroup.rowNumber = 1; 