file = LOAD '/home/francesco/BigData/hw/exPig/inputFiles/esempio.txt' USING TextLoader() AS (line:chararray);

 /*seleziona solo gli scontrini del primo trimestre*/
just_first_trimester = FOREACH (filter file by  (INDEXOF($0, '-1-')!=-1 OR INDEXOF($0, '-2-')!=-1 OR INDEXOF($0, '-3-')!=-1)) GENERATE $0;

/*elimina il giorno dalla data*/
cleaned_data = FOREACH just_first_trimester GENERATE CONCAT(CONCAT(SUBSTRING($0, 0, 6), ','), $0);

/*splitta l'intera linea tramite il separatore ','*/
splt = FOREACH cleaned_data GENERATE FLATTEN(STRSPLIT($0, ',')) ;

 /*genera il primo campo, ottenuto dalla separazione, come date (la data), il secondo come espansione in bag dei prodotti (inizialmente saranno presenti anche coppie (data, data) che poi verrano filtrate).  (prova col TOKENIZER)*/
date_vals = FOREACH splt GENERATE $0 AS date, FLATTEN(TOBAG(*)) as value;

 /*filtro le coppie (data, data). Uso il fatto che i prodotti non hanno il carattere '-'*/
product2data = FOREACH (FILTER date_vals by INDEXOF(value, '-') == -1) GENERATE  value as product, date;

 /*raggruppa per prodotto e data*/
grouped = GROUP product2data BY (product, date);

/*sulle coppie prodotto-data conto le quantità vendute(di quel prodotto in quel mese)*/
counts = FOREACH grouped GENERATE group, COUNT(product2data) as quantity;

 /*riorganizzo ogni riga esplodendo tutto in 3 colonne: una per il prodotto, una per la data e una per la quantità*/
organized = FOREACH counts GENERATE $0.product as product, $0.date as date,  $1 as quantity;

 /*raggruppo per prodotto: avrò da una parte il prodotto e dall'altra la coppia data-quantità*/
grouped_for_result = GROUP organized BY product;

/*organizza il risultato in un formato leggibile per l'output. (prodotto, {data1, quantità1}, {data2, quantità2}...)*/
result = FOREACH grouped_for_result {
							date2quantity = FOREACH organized GENERATE date, quantity;
							GENERATE group, date2quantity;
}; 

/*scrivi il risultato*/
store result into '/home/francesco/BigData/hw/exPig/pigoutput/ex2' using PigStorage();

