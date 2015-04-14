file = LOAD '/home/francesco/BigData/hw/exPig/inputFiles/esempio.txt' USING PigStorage(',');
just_products = FOREACH file GENERATE $1 .. ;
products = FOREACH just_products GENERATE FLATTEN(TOBAG(*));
grouped = GROUP products BY $0;
product2quantity = FOREACH grouped GENERATE group, COUNT(products);
sorted = ORDER product2quantity BY $1 DESC;
store sorted into '/home/francesco/BigData/hw/exPig/pigoutput/ex1' using PigStorage();
