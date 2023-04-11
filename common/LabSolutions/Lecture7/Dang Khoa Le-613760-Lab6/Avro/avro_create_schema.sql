CREATE EXTERNAL TABLE records (year STRING, temperature INT, quality INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/hive/input/writeFileToAvro';

CREATE TABLE as_avro (year STRING, temperature INT, quality INT)
STORED AS AVRO;

