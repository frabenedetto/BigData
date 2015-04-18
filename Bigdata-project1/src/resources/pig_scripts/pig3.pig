REGISTER /Users/khorda/Documents/Universita/BigData/myudf.jar

myinput = LOAD '/Users/khorda/git/BigData/Bigdata-project1/src/resources/esempio.txt' USING PigStorage(',');

coppie = FOREACH myinput GENERATE FLATTEN(project1.udf.Accoppia($1 ..));

raggruppati = GROUP coppie BY e1,e2;

contati = FOREACH raggruppati GENERATE ($0,SIZE($1));






