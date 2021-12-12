CREATE SCHEMA IF NOT EXISTS ${ods_schema_name};


DROP TABLE IF EXISTS ${ods_schema_name}.pensionregistry_dsrc;

DROP TABLE IF EXISTS ${ods_schema_name}.pensionregistry_delta;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.pensionregistry_dsrc
(
    vd                      string,
    po                      int,
    sm                      double,
    pm                      bigint,
    d                       string,
    hash                    string,
    hash_dc                 string,
    table_name              string,
    hash_table              string,
    snapshop                int,
    creation_date           string,
    deleted_flag            boolean,
    system_id               string
    ) PARTITIONED BY (date string)
    STORED AS AVRO
    LOCATION 'hdfs:///user/user/pensionregistry';


CREATE TABLE IF NOT EXISTS ${ods_schema_name}.pensionregistry_delta
(
    dws_job                 string,
    dws_act                 string,
    insert_date             string,
    vd                      string,
    po                      int,
    sm                      double,
    pm                      int,
    d                       string,
    hash                    string,
    table_name              string,
    hash_table              string,
    hash_dc                 string
) PARTITIONED BY (date string)
    STORED AS ORC;


DROP TABLE IF EXISTS ${ods_schema_name}.pensionreceiver;

DROP TABLE IF EXISTS ${ods_schema_name}.pensionreceiver_delta;

DROP TABLE IF EXISTS ${ods_schema_name}.pensionreceiver_double;


CREATE TABLE IF NOT EXISTS ${ods_schema_name}.pensionreceiver
(
    nk                      string,
    dws_job                 string,
    deleted_flag            string,
    default_flag            string,
    insert_date             string,
    gn                      string,
    nm/f                    string,
    nm/i                    string,
    nm/o                    string,
    dc                      string,
    ad                      string,
    hash                    string,
    table_name              string,
    hash_table              string,
    hash_dc                 string
    )
    STORED AS ORC;


CREATE TABLE IF NOT EXISTS ${ods_schema_name}.pensionreceiver_delta
(
    nk                      string,
    dws_job                 string,
    dws_act                 string,
    dws_uniact              string,
    insert_date             string,
    deleted_flag            string,
    default_flag            string,
    insert_date             string,
    gn                      string,
    nm/f                    string,
    nm/i                    string,
    nm/o                    string,
    dc                      string,
    ad                      string,
    hash                    string,
    table_name              string,
    hash_table              string,
    hash_dc                 string
    )
    STORED AS ORC;


CREATE TABLE IF NOT EXISTS ${ods_schema_name}.pensionreceiver_double
(
    dws_job                 string,
    insert_date             string,
    effective_flag          string,
    algorithm_ncode         int,
    nk                      string,
    deleted_flag            string,
    default_flag            string,
    gn                      string,
    nm/f                    string,
    nm/i                    string,
    nm/o                    string,
    dc                      string,
    ad                      string,
    hash                    string,
    table_name              string,
    hash_table              string,
    hash_doc                string
    )
    STORED AS ORC;