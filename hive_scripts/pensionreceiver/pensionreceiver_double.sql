DROP TABLE IF EXISTS ${ods_schema_name}.pensionreceiver_double;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.pensionreceiver_double
(
    dws_job                 varchar(100),
    insert_date             varchar(20),
    effective_flag          boolean,
    algorithm_ncode         int,
    nk                      varchar(100),
    deleted_flag            varchar(8),
    default_flag            varchar(8),
    gn                      varchar(20),
    "nm/f"                  varchar(100),
    "nm/i"                  varchar(100),
    "nm/o"                  varchar(100),
    dc                      varchar(200),
    ad                      varchar(500),
    hash                    varchar(200),
    table_name              varchar(200),
    hash_table              varchar(200),
    hash_dc                varchar(200)
    );