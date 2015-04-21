/*carica e crea una funzione che dalla tabella con i dati "destrutturati" crea una nuova tabella con le colonne data, prodotto*/
ADD JAR /home/francesco/BigData/hw/exHive/myUdf3.jar;  
CREATE TEMPORARY FUNCTION  productpair as 'project1.hive.udf.ProductPairUDTF';

/*crea una prima tabella in cui vengono caricati i dati immettendo in un'unica colonna le righe del file*/
CREATE TABLE IF NOT EXISTS rawData(ticket String);

/*carica il file*/
LOAD DATA LOCAL INPATH '/home/francesco/BigData/hw/exPig/inputFiles/esempio1.txt' OVERWRITE INTO TABLE rawData ;

/*crea il risultato per l'esercizio 3*/
CREATE TABLE IF NOT EXISTS ex3 AS
	SELECT pairs.p1 as p1, pairs.p2 as p2, COUNT(1) as quantity FROM 
		(SELECT productpair(ticket) as (p1,p2) FROM rawData) pairs
		group by pairs.p1, pairs.p2;