/*carica e crea una funzione che dalla tabella con i dati "destrutturati" crea una nuova tabella con le colonne data, prodotto*/
ADD JAR /home/francesco/BigData/hw/exHive/myUdf.jar;  
CREATE TEMPORARY FUNCTION  data2product as 'project1.hive.udf.Data2productUDTF';

/*crea una prima tabella in cui vengono caricati i dati immettendo in un'unica colonna le righe del file*/
CREATE TABLE IF NOT EXISTS rawData(ticket String);

/*carica il file*/
LOAD DATA LOCAL INPATH '/home/francesco/BigData/hw/exPig/inputFiles/esempio1.txt' OVERWRITE INTO TABLE rawData ;


/*crea una tabella in cui per ogni prodotto vengono contate le occorrenze in cui esso compare*/
CREATE TABLE IF NOT EXISTS product2quantity AS 
	SELECT tickets.product, COUNT(1) as quantity from
		(SELECT data2product(ticket) as (date, product) FROM rawData) tickets
			 group by   tickets.product;

/*tabella con prodotto-data*/
CREATE TABLE IF NOT EXISTS product2data AS 
	SELECT data2product(ticket) as (date, product) FROM rawData;

/*tabella intersezioni*/
CREATE TABLE IF NOT EXISTS couple2intersection AS
	select pd1.product as p1, pd2.product as p2, COUNT(1) as intersection from
	product2data pd1 JOIN product2data pd2  
	 where pd1.date=pd2.date AND pd1.product!=pd2.product
	group by pd1.product,pd2.product; 



/*risultato ex 4*/
CREATE TABLE IF NOT EXISTS ex4 AS
	SELECT ci.p1, ci.p2, (ci.intersection/pq.quantity)*100 as ratio FROM
	product2quantity pq JOIN couple2intersection ci  ON pq.product=ci.p1; 
