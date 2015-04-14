REGISTER /home/khorda/Scrivania/myudf.jar

myinput = LOAD '/home/khorda/Documenti/esempio.txt' USING PigStorage();

del_date = FOREACH myinput GENERATE $1 ..;

coppie = FOREACH date_del GENERATE project1.udf.Accoppia();

A =  FOREACH coppie GENERATE FLATTEN(TOBAG(*));
B =  FOREACH A GENERATE FLATTEN(TOBAG(*));

group = GROUP B BY $0;
