import org.apache.spark.sql._

val genderLU = spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://localhost:54310/home/ubuntu/final/gender.csv");
val IM03 = spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://localhost:54310/home/ubuntu/final/im_world-championships_2003.csv");
val IM03_Mod = IM03.withColumn("year", lit("2003")).withColumn("firstname", split(col("name"), " ").getItem(0)).withColumn("firstname", upper(col("firstname")));
val IM04 = spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://localhost:54310/home/ubuntu/final/im_world-championships_2004.csv");
val IM04_Mod = IM04.withColumn("year", lit("2004")).withColumn("firstname", split(col("name"), " ").getItem(0)).withColumn("firstname", upper(col("firstname")));
val IM05 = spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://localhost:54310/home/ubuntu/final/im_world-championships_2005.csv");
val IM05_Mod = IM05.withColumn("year", lit("2005")).withColumn("firstname", split(col("name"), " ").getItem(0)).withColumn("firstname", upper(col("firstname")));
val IM06 = spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://localhost:54310/home/ubuntu/final/im_world-championships_2006.csv");
val IM06_Mod = IM06.withColumn("year", lit("2006")).withColumn("firstname", split(col("name"), " ").getItem(0)).withColumn("firstname", upper(col("firstname")));
val IM07 = spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://localhost:54310/home/ubuntu/final/im_world-championships_2007.csv");
val IM07_Mod = IM07.withColumn("year", lit("2007")).withColumn("firstname", split(col("name"), " ").getItem(0)).withColumn("firstname", upper(col("firstname")));
val IM08 = spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://localhost:54310/home/ubuntu/final/im_world-championships_2008.csv");
val IM08_Mod = IM08.withColumn("year", lit("2008")).withColumn("firstname", split(col("name"), " ").getItem(0)).withColumn("firstname", upper(col("firstname")));
val IM09 = spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://localhost:54310/home/ubuntu/final/im_world-championships_2009.csv");
val IM09_Mod = IM09.withColumn("year", lit("2009")).withColumn("firstname", split(col("name"), " ").getItem(0)).withColumn("firstname", upper(col("firstname")));
val IM10 = spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://localhost:54310/home/ubuntu/final/im_world-championships_2010.csv");
val IM10_Mod = IM10.withColumn("year", lit("2010")).withColumn("firstname", split(col("name"), " ").getItem(0)).withColumn("firstname", upper(col("firstname")));
val IM11 = spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://localhost:54310/home/ubuntu/final/im_world-championships_2011.csv");
val IM11_Mod = IM11.withColumn("year", lit("2011")).withColumn("firstname", split(col("name"), " ").getItem(0)).withColumn("firstname", upper(col("firstname")));
val IM12 = spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://localhost:54310/home/ubuntu/final/im_world-championships_2012.csv");
val IM12_Mod = IM12.withColumn("year", lit("2012")).withColumn("firstname", split(col("name"), " ").getItem(0)).withColumn("firstname", upper(col("firstname")));
val IM13 = spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://localhost:54310/home/ubuntu/final/im_world-championships_2013.csv");
val IM13_Mod = IM13.withColumn("year", lit("2013")).withColumn("firstname", split(col("name"), " ").getItem(0)).withColumn("firstname", upper(col("firstname")));
val IM14 = spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://localhost:54310/home/ubuntu/final/im_world-championships_2014.csv");
val IM14_Mod = IM14.withColumn("year", lit("2014")).withColumn("firstname", split(col("name"), " ").getItem(0)).withColumn("firstname", upper(col("firstname")));
val IM15 = spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://localhost:54310/home/ubuntu/final/im_world-championships_2015.csv");
val IM15_Mod = IM15.withColumn("year", lit("2015")).withColumn("firstname", split(col("name"), " ").getItem(0)).withColumn("firstname", upper(col("firstname")));


IM03_Mod.createOrReplaceTempView("IM03View");
IM04_Mod.createOrReplaceTempView("IM04View");
IM05_Mod.createOrReplaceTempView("IM05View");
IM06_Mod.createOrReplaceTempView("IM06View");
IM07_Mod.createOrReplaceTempView("IM07View");
IM08_Mod.createOrReplaceTempView("IM08View");
IM09_Mod.createOrReplaceTempView("IM09View");
IM10_Mod.createOrReplaceTempView("IM10View");
IM11_Mod.createOrReplaceTempView("IM11View");
IM12_Mod.createOrReplaceTempView("IM12View");
IM13_Mod.createOrReplaceTempView("IM13View");
IM14_Mod.createOrReplaceTempView("IM14View");
IM15_Mod.createOrReplaceTempView("IM15View");
genderLU.createOrReplaceTempView("genderView");

spark.catalog.dropTempView("IMALLView");
spark.sqlContext.sql("Create Temp View IMALLView AS select * from IM03View union select * from IM04View union select * from IM05View union select * from IM06View union select * from IM07View union select * from IM08View union select * from IM09View union select * from IM10View union select * from IM11View union select * from IM12View union select * from IM13View union select * from IM14View union select * from IM15View");

//start of gender derivation using precedence logic
//genderRank from IM data files to associate a gender with a person
//generate a list of first name from IM genderRank to get international names
//remove dupes from the IM name file
//consolidate IM names with the census names 

//create view for US census file from https://deron.meranda.us/data
//rank was used assuming a lower gender rank made it a particular gender more 
//likely  example chris, male, 4    chris, female, 15.  Male would be assigned
spark.catalog.dropTempView("genderNameView");
spark.sqlContext.sql("Create Temp View genderNameView As (Select firstname, gender from (Select *, row_number() over (partition by firstname order by genderrank asc) as seqnum from genderView) a where seqnum = 1)");

//create view from IMALL dataset.  Used genderRank in years it was populated with the windows partition function to assign gender for each name.  Assign by year, genderRank (within the year), firstname.    
//Men assumed to have faster times, so row seq = 1 assigned name to Male and row seq = 2 as Female.
spark.catalog.dropTempView("genderNameIMView");
spark.sqlContext.sql("Create Temp View genderNameIMView As (Select distinct(name), firstname, case when rank = 1 THEN 'Male' Else 'Female' END as gender from (Select name, firstname, year, cast(genderRank as int), row_number() over(partition by year, genderRank order by year, genderRank, overall asc) as rank from IMALLVIEW where genderRank !='---' order by year, genderRank, overall asc)a)");

//create view from genderrankview to product a list of distinct first names and gender.  This would reduce duplicate rows by gender.  However it did produce 37 instances of names that were assigned to both genders. 
spark.catalog.dropTempView("genderfirstNameIMView");
spark.sqlContext.sql("Create Temp View genderfirstNameIMView As (Select distinct(firstname) as firstname, case when rank = 1 THEN 'Male' Else 'Female' END as gender from (Select name, firstname, year, cast(genderRank as int), row_number() over(partition by year, genderRank order by year, genderRank, overall asc) as rank from IMALLVIEW where genderRank !='---' order by year, genderRank, overall asc)a)");

//create view to identify the names with both male and female genders.  37 rows were found  
spark.catalog.dropTempView("genderNameIMDupesView");
spark.sqlContext.sql("Create Temp View genderNameIMDupesView As (Select firstname from (Select firstname, count(gender) as recs from genderfirstNameIMView group by firstname)a where recs >1)");

//create view to remove the 37 dupes above.  Instead of choose a gender, the name was removed.  Gender will remain NULL if a match is not made on name.
spark.catalog.dropTempView("genderNameIMNoDupesView");
spark.sqlContext.sql("Create Temp View genderNameIMNoDupesView As (Select firstname, gender from genderfirstNameIMView where firstname not in (Select firstname from genderNameIMDupesView))");

//Create view union census firstnames with the IM Names.  Precedence was taken from the IM Name list, so those names were removed from the census name list before the union
spark.catalog.dropTempView("genderfirstNameView");
spark.sqlContext.sql("Create Temp View genderfirstNameView As (Select * from genderNameView where firstname not in (Select firstname from genderNameIMNoDupesView) union Select * from genderNameIMNoDupesView)");

//create a table that brings in gender to IMALL Results.
//spark.sqlContext.sql("Drop table if exists IMALLGenderTbl");
spark.sqlContext.sql("Create Table if not exists IMALLGenderTbl As (Select * from (Select i.*, coalesce(g.gender, c.gender) as gender from IMALLView i left join genderNameIMView g on i.name = g.name left join genderfirstNameView c on i.firstname = c.firstname)a )"); 

//Describe table created
//spark.sqlContext.sql("Describe table IMALLGenderTbl").show(25);

//Summarize stats on gender assignment.  93.72% assignment rate. 
//spark.sqlContext.sql("Select count(*), gender from IMALLGenderTbl group by gender").show(10); 

//Show top number of finishes
spark.sqlContext.sql("Select totalraces, name, gender, raceyears, divisions from (Select name, gender, collect_list(x.year) as raceyears, collect_list(x.division) as divisions, count(year) as totalraces, dense_rank() over (order by count(year) desc) as drank from (Select name, gender, division, year from IMALLGenderTbl where overall not in ('DNS', 'DNF', 'DQ') order by name, year, division asc)x group by name, gender order by totalraces desc)y where drank < 4 order by drank asc").show();

//Did transition times impact top 5 placement (podium) in an age group in 2013?
spark.sqlContext.sql("Select name, gender, division, overall, concat(lpad(cast(netO/3600 as int), 2, 0), ':', lpad(cast((netO/60)%60 as int), 2, 0), ':', lpad(cast(netO%60 as int), 2, 0)) as adjoverall, divRank, adjdivRank from (Select name, gender, division, divRank, overall, toverall as actO, toverall-ttime as netO, dense_rank() over(partition by division, gender order by toverall-ttime asc) as adjdivRank from (Select name, gender, division, cast(divRank as int), cast(genderRank as int), cast(overallRank as int), overall, hour(t1)*3600+hour(t2)*3600+minute(t1)*60+minute(t2)*60+second(t1)+second(t2) as ttime, hour(overall)*3600+minute(overall)*60+second(overall) as toverall from IMALLGenderTbl where year = 2013 and overall not in ('DNS', 'DNF', 'DQ'))x order by division, gender, divRank asc) y where divRank < 20 and adjdivRank < 6 and adjdivRank < divRank").show(20);

//How much do times increase over each 5 year block by age and gender

spark.sqlContext.sql("Select year, division, gender, fastest, slowest, concat(lpad(cast(divAvgO/3600 as int), 2, 0), ':', lpad(cast((divAvgO/60)%60 as int), 2, 0), ':', lpad(cast(divAvgO%60 as int), 2, 0)) as Top5Avg,  cast(((divAvgO/nullif(lag(divAvgO) over (partition by division, gender order by year, division, gender), 0))-1)*100 as decimal(5,2)) as percent_change from (Select distinct year, division, gender, min(overall) over (partition by year, division, gender order by year, division, gender) as fastest, max(overall) over (partition by year, division, gender  order by year, division, gender) as slowest, avg(toverall) over (partition by year, division, gender order by year, division, gender) as divAvgO from (Select year, division, gender, overall, divRank, hour(overall)*3600+minute(overall)*60+second(overall) as toverall from IMALLGenderTbl where year > 2011 and overall not in ('DNS', 'DNF', 'DQ') and division in ('25-29', '30-34') and divRank < 6)x group by year, division, gender, overall, toverall order by division asc, gender asc, year)y").show(20);
