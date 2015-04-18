REGISTER /home/francesco/BigData/hw/exPig/UDFs/myUdf.jar

/*carica il file*/
file = LOAD '/home/francesco/BigData/hw/exPig/inputFiles/esempio1.txt' USING  PigStorage(',');

/*genera coppie data-prodotto*/
data2product = FOREACH file GENERATE FLATTEN(TOBAG($0)) as data, FLATTEN(TOBAG($1 ..)) as product;

/*raggruppa tutto per prodotto. (fanne 2 copie per fare successivamente il cross-join)*/
grouped = GROUP data2product BY product;
cleaned = FOREACH grouped {
									dates = FOREACH data2product GENERATE data as data; 
									GENERATE group, dates;
										};
cleaned1 = FOREACH grouped {
									dates = FOREACH data2product GENERATE data as data; 
									GENERATE group, dates;
										};


/*cross-join*/
crossed = CROSS cleaned, cleaned1;

/*filtra le coppie prodotto-date--prodotto-date con prodotto uguale. (es uova->(date)-uova->(date))*/
filtered = FILTER crossed BY $0 != $2;

/*genera (a,b)->(dataA,dataB)*/
partial_result = FOREACH filtered GENERATE $0 as a, $2 as b, COUNT($1) as countA, TOBAG($1,$3) as dadb;

/*genera il risultato finale*/
result = FOREACH partial_result GENERATE project1.udf.IntersezioneDate();







