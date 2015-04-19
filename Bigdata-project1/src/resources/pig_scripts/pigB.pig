REGISTER /home/khorda/git/BigData/Bigdata-project1/src/resources/udf_jars/udf.jar

myinput = LOAD '/home/khorda/git/BigData/Bigdata-project1/src/resources/esempio.txt' USING PigStorage(',');

gruppi = foreach myinput generate FLATTEN(project1.udf.CreaGruppi($1 ..));

grouped_gruppi = group gruppi by $0 ..;

count = foreach grouped_gruppi generate $0,SIZE($1);
