CREATE TABLE scontrini(scontrino STRING)
STORED AS SEQUENCEFILE;

LOAD DATA LOCAL INPATH /home/khorda/git/BigData/Bigdata-project1/src/resources/esempio.txt INTO TABLE scontrini;