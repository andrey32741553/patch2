DROP TABLE IF EXISTS ${ods_schema_name}.pensionregistry_dsrc;

CREATE TABLE ${ods_schema_name}.pensionregistry_dsrc(
  `vd` string COMMENT '',
  `po` int COMMENT '',
  `sm` double COMMENT '',
  `pm` int COMMENT '',
  `d` string COMMENT '',
  `hash` string COMMENT '',
  `hash_dc` string COMMENT '',
  `table_name` string COMMENT '',
  `hash_table` string COMMENT '',
  `snapshot` int COMMENT '',
  `creation_date` string COMMENT '',
  `deleted_flag` boolean COMMENT '',
  `system_id` string COMMENT '')
PARTITIONED BY (
  `date` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  '${hdfs_path}pensionregistry'
TBLPROPERTIES (
  'transient_lastDdlTime'='1639669397');