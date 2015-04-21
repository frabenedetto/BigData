/*carica e crea una funzione che dalla tabella con i dati "destrutturati" crea una nuova tabella con le colonne data, prodotto*/
ADD JAR /home/francesco/BigData/hw/exHive/myUdf2.jar;  
CREATE TEMPORARY FUNCTION  firstTrimester2product as 'project1.hive.udf.Month2ProductUDTF';

/*crea una prima tabella in cui vengono caricati i dati immettendo in un'unica colonna le righe del file*/
CREATE TABLE IF NOT EXISTS rawData(ticket String);

/*carica il file*/
LOAD DATA LOCAL INPATH '/home/francesco/BigData/hw/exPig/inputFiles/esempio.txt' OVERWRITE INTO TABLE rawData ;

/*esercizio 2*/
CREATE TABLE IF NOT EXISTS ex2 AS
	SELECT tickets.product, tickets.date, COUNT(1) as quantity from
		(SELECT firstTrimester2product(ticket) as (date, product) FROM rawData) tickets
	GROUP BY tickets.product, tickets.date;

   /*create table prova(m MAP<String, STRUCT <String:BigInt> >);*/
