/*carica e crea una funzione che dalla tabella con i dati "destrutturati" crea una nuova tabella con le colonne data, prodotto*/
ADD JAR /home/francesco/BigData/hw/exHive/myUdf.jar;  
CREATE TEMPORARY FUNCTION  data2product as 'project1.hive.udf.Data2productUDTF';

/*crea una prima tabella in cui vengono caricati i dati immettendo in un'unica colonna le righe del file*/
CREATE TABLE IF NOT EXISTS rawData(ticket String);

/*carica il file*/
LOAD DATA LOCAL INPATH '/home/francesco/BigData/hw/exPig/inputFiles/esempio.txt' OVERWRITE INTO TABLE rawData ;


/*esercizio 1*/
CREATE TABLE IF NOT EXISTS ex1 AS 
	SELECT tickets.product, COUNT(1) as count from
		(SELECT data2product(ticket) as (date, product) FROM rawData) tickets
			 group by   tickets.product 
						order by count desc;






