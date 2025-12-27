+----------------------------------------------------+
|                   createtab_stmt                   |
+----------------------------------------------------+
| CREATE EXTERNAL TABLE `energy_production`(         |
|   `hourutc` string,                                |
|   `hourdk` string,                                 |
|   `municipalityno` string,                         |
|   `solarmwh` double,                               |
|   `offshorewindlt100mw_mwh` double,                |
|   `offshorewindge100mw_mwh` double,                |
|   `onshorewindmwh` double,                         |
|   `thermalpowermwh` double,                        |
|   `ts` bigint,                                     |
|   `year` int,                                      |
|   `month` int,                                     |
|   `day` int,                                       |
|   `hour` int)                                      |
| ROW FORMAT SERDE                                   |
|   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'  |
| STORED AS INPUTFORMAT                              |
|   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'  |
| OUTPUTFORMAT                                       |
|   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' |
| LOCATION                                           |
|   'hdfs://namenode:9000/user/hive/warehouse/energy_by_municipality' |
| TBLPROPERTIES (                                    |
|   'bucketing_version'='2',                         |
|   'transient_lastDdlTime'='1765819136')            |
+----------------------------------------------------+

================================

+----------------------------------------------------+
|                   createtab_stmt                   |
+----------------------------------------------------+
| CREATE EXTERNAL TABLE `weather_raw`(               |
|   `station_id` string,                             |
|   `station_name` string,                           |
|   `parameter_id` string,                           |
|   `value` double,                                  |
|   `longitude` double,                              |
|   `latitude` double,                               |
|   `year` int,                                      |
|   `month` int,                                     |
|   `day` int,                                       |
|   `hour` int,                                      |
|   `minute` int)                                    |
| PARTITIONED BY (                                   |
|   `dummy` string)                                  |
| ROW FORMAT SERDE                                   |
|   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'  |
| STORED AS INPUTFORMAT                              |
|   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'  |
| OUTPUTFORMAT                                       |
|   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' |
| LOCATION                                           |
|   'hdfs://namenode:9000/user/hive/warehouse/weather_raw' |
| TBLPROPERTIES (                                    |
|   'bucketing_version'='2',                         |
|   'parquet.compression'='SNAPPY',                  |
|   'transient_lastDdlTime'='1765568691')            |
+----------------------------------------------------+

================================

+----------------------------------------------------+
|                   createtab_stmt                   |
+----------------------------------------------------+
| CREATE TABLE `weather_wide`(                       |
|   `station_id` string,                             |
|   `station_name` string,                           |
|   `latitude` double,                               |
|   `longitude` double,                              |
|   `year` int,                                      |
|   `month` int,                                     |
|   `day` int,                                       |
|   `hour` int,                                      |
|   `minute` int,                                    |
|   `temp_dry` double,                               |
|   `humidity` double,                               |
|   `temp_dew` double,                               |
|   `wind_dir` double,                               |
|   `wind_speed` double,                             |
|   `wind_max` double,                               |
|   `pressure` double,                               |
|   `pressure_at_sea` double,                        |
|   `precip_past10min` double,                       |
|   `precip_past1min` double,                        |
|   `precip_past1h` double,                          |
|   `precip_dur_past10min` double,                   |
|   `precip_dur_past1h` double,                      |
|   `visib_mean_last10min` double,                   |
|   `visibility` double,                             |
|   `radia_glob` double,                             |
|   `radia_glob_past1h` double,                      |
|   `sun_last10min_glob` double,                     |
|   `sun_last1h_glob` double,                        |
|   `wind_min` double,                               |
|   `wind_min_past1h` double,                        |
|   `wind_speed_past1h` double,                      |
|   `wind_dir_past1h` double,                        |
|   `wind_gust_always_past1h` double,                |
|   `wind_max_per10min_past1h` double,               |
|   `temp_grass` double,                             |
|   `temp_grass_mean_past1h` double,                 |
|   `temp_grass_min_past1h` double,                  |
|   `temp_grass_max_past1h` double,                  |
|   `temp_soil` double,                              |
|   `temp_soil_mean_past1h` double,                  |
|   `temp_soil_max_past1h` double,                   |
|   `temp_soil_min_past1h` double,                   |
|   `temp_mean_past1h` double,                       |
|   `temp_max_past1h` double,                        |
|   `temp_min_past1h` double,                        |
|   `temp_min_past12h` double,                       |
|   `temp_max_past12h` double,                       |
|   `cloud_cover` double,                            |
|   `cloud_height` double,                           |
|   `weather` string,                                |
|   `leav_hum_dur_past10min` double,                 |
|   `leav_hum_dur_past1h` double,                    |
|   `humidity_past1h` double)                        |
| ROW FORMAT SERDE                                   |
|   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'  |
| STORED AS INPUTFORMAT                              |
|   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'  |
| OUTPUTFORMAT                                       |
|   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' |
| LOCATION                                           |
|   'hdfs://namenode:9000/user/hive/warehouse/weather_wide' |
| TBLPROPERTIES (                                    |
|   'bucketing_version'='2',                         |
|   'transient_lastDdlTime'='1766062268')            |
+----------------------------------------------------+

================================