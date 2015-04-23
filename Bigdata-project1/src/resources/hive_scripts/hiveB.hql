ADD JAR /home/khorda/git/BigData/Bigdata-project1/src/resources/udf_jars/udtf.jar;  
CREATE TEMPORARY FUNCTION  buildSets as 'project1.hive.udf.ProductSetsUDTF';
CREATE TABLE IF NOT EXISTS rawData(ticket String);
LOAD DATA LOCAL INPATH '/home/khorda/git/BigData/Bigdata-project1/src/resources/esempio.txt' OVERWRITE INTO TABLE rawData ;
create table if not exists insiemi as select buildSets(ticket) as (sets) from rawData;
