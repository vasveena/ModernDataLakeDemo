drop schema if exists spectrum;

create external schema spectrum
from data catalog
database 'testdb'
region 'us-east-1' 
iam_role 'arn:aws:iam::620614497509:role/DemoSpectrumRole';

drop table if exists spectrum.trip;

create external table spectrum.trip(
tripId varchar(20),
routeId varchar(20),
startTime timestamp,
arrivalTime array<varchar(20)>,
departureTime array<varchar(20)>,
futureStopIds array<varchar(20)>,
numOfFutureStops integer,
currentStopSequence integer,
currentStatus varchar(20),
stopId varchar(20),
currentTs varchar(20)
)
stored as parquet
location 's3://your_s3_bucket/prefix/streaming_output/';

--select all non-array fields 
select tripId, routeId, startTime, numOfFutureStops, 
currentStopSequence, currentStatus, stopId, currentTs
from spectrum.trip limit 10;

--select future stop IDs for a trip  
select distinct f from spectrum.trip t 
join t.futureStopIds f on true 
where tripId = '019250_E..S'
order by currentTs;

--Total number of entries
select count (tripId) from spectrum.trip 

-- Number of trips started between two intervals 
select count(*) as numOfTrips, tripId, routeId, startTime from spectrum.trip 
where startTime BETWEEN '2021-09-20 02:30:00' AND '2021-09-20 03:05:00'
group by tripId, routeId, startTime order by 4 desc;

-- Get future number of stops per trip and route at the moment 
select tripId, routeId, currentTs, sum(numOfFutureStops) from
(select tripId , routeId, currentTs, numOfFutureStops,
rank() over (order by currentTs) as rnk
from spectrum.trip)
where rnk = 1
group by 1,2,3
order by 4 desc;
