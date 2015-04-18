REGISTER /home/khorda/git/BigData/Bigdata-project1/src/resources/udf_jars/udf.jar

myinput = LOAD '/home/khorda/git/BigData/Bigdata-project1/src/resources/esempio.txt' USING PigStorage(',');

coppie = FOREACH myinput GENERATE FLATTEN(project1.udf.Accoppia($1 ..));

raggruppati = GROUP coppie BY (e1,e2);

contati = FOREACH raggruppati GENERATE $0,COUNT($1);

ordinati = ORDER contati BY $1;

primi_dieci = LIMIT ordinati 10; 






