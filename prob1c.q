DROP TABLE IF EXISTS oshkoshweather;
CREATE EXTERNAL TABLE IF NOT EXISTS oshkoshweather(year INT, month INT, day INT, timecst STRING, tempF FLOAT, dewpointF FLOAT, humidity FLOAT, sealvlpressin FLOAT, vismph FLOAT, windir STRING, windspeedmph FLOAT, gustspeedmph FLOAT, precipin STRING, events STRING, conditions STRING, winddirdegrees FLOAT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 'hdfs:/home/ubuntu/final/Oshkosh';
DROP VIEW IF EXISTS sevenday;
CREATE VIEW sevenday AS SELECT DISTINCT(CAST(CONCAT(year,'-',month,'-',day) AS date)) AS enddate, date_add(CAST(CONCAT(year,'-',month,'-',day) AS date), -6) AS begindate, 'osh' as cityid from oshkoshweather;
SELECT o.begindate, o.enddate FROM (SELECT avg(x.tempf) AS avgsevtemp,s.begindate, s.enddate, DENSE_RANK() OVER(ORDER BY avg(x.tempf)DESC) AS drank FROM(Select tempf, CAST(CONCAT(year,'-',month,'-',day)AS date) AS weatherdate, 'osh' AS cityid FROM oshkoshweather WHERE tempf<>-9999 AND year IS NOT NULL)x JOIN sevenday s ON s.cityid = x.cityid WHERE x.weatherdate>=s.begindate and x.weatherdate<=s.enddate GROUP BY s.begindate, s.enddate)o WHERE o.drank=1;






