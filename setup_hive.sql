DROP TABLE IF EXISTS temp_students;
DROP TABLE IF EXISTS parquet_students;

CREATE TABLE temp_students (id INT, name STRING, dept STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH 'students.csv' INTO TABLE temp_students;

CREATE TABLE parquet_students (id INT, name STRING, dept STRING)
STORED AS PARQUET;

INSERT OVERWRITE TABLE parquet_students SELECT * FROM temp_students;
SELECT * FROM parquet_students;
