import org.apache.spark.sql._

val oshweath = spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://localhost:54310/home/ubuntu/final/Oshkosh/OshkoshWeather.csv");
val oshweathmod = oshweath.withColumn("city", lit("Oshkosh"))

val icweath = spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://localhost:54310/home/ubuntu/final/IowaCity/IowaCityWeather.csv");
val icweathmod = icweath.withColumn("city", lit("IowaCity"))

oshweathmod.createOrReplaceTempView("oshweathView");
icweathmod.createOrReplaceTempView("icweathView");

spark.sqlContext.sql("Drop table if exists newweathTbl");
spark.sqlContext.sql("Create Table if not exists newweathTbl AS(Select from_unixtime(unix_timestamp(Concat((Year), '-', (Month), '-', (Day), ' ', (TimeCST)), 'yyyy-MM-dd hh:mm aa'), 'yyyy-MM-dd HH:mm') as date, TemperatureF, city, cast(Year as int) as year  from oshweathView where TemperatureF is not null and TemperatureF != '-9999.0' union select from_unixtime(unix_timestamp(Concat((Year), '-', (Month), '-', (Day), ' ', (TimeCST)), 'yyyy-MM-dd hh:mm aa'), 'yyyy-MM-dd HH:mm') as date, TemperatureF, city, cast(Year as int) as year from icweathView where TemperatureF is not null and TemperatureF != '-9999.0')");

spark.catalog.dropTempView("weathView");
spark.sqlContext.sql("Create Temp View weathView AS(Select city, year, cast(date as timestamp)as date, TemperatureF, (cast(date as timestamp)) + Interval 24 Hours as date2 from newweathTbl)");

spark.catalog.dropTempView("topweathView");
spark.sqlContext.sql("Drop Table if exists topweathTbl");
spark.sqlContext.sql("Create Table if not exists topweathTbl AS (Select year, city, begintemp, endtemp, tempdiff, begin, end from (Select year, city, max(tempdiff) as tempdiff, dense_rank() over (partition by year, city order by max(tempdiff) desc) as drank, begin, end, begintemp, endtemp from (Select x.year, x.city, x.TemperatureF as begintemp, y.TemperatureF as endtemp, abs(x.TemperatureF-y.TemperatureF) as tempdiff, x.date as begin, x.date2, y.date as end from weathView x join weathView y on x.city=y.city where y.date <= x.date2 and y.date>=x.date and x.date!=y.date)z group by year, city, begintemp, endtemp, begin, end)zz where drank=1)");


spark.catalog.dropTempView("topweathView");
spark.sqlContext.sql("Create Temp View topweathView AS (Select * from topweathTbl)");

spark.sqlContext.sql("Select city, tempdiff, begin, end from (Select city, tempdiff, begin, end, dense_rank() over (order by max(tempdiff) desc) as finalrank from topweathView group by city, tempdiff, begin, end)a where finalrank =1").show();
