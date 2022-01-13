DROP TABLE IF EXISTS ${ods_schema_name}.pensionreceiver;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.pensionreceiver
(
    nk                      varchar(100),
    dws_job                 varchar(100),
    deleted_flag            varchar(8),
    default_flag            varchar(8),
    insert_date             varchar(20),
    gn                      varchar(20),
    "nm/f"                  varchar(100),
    "nm/i"                  varchar(100),
    "nm/o"                  varchar(100),
    dc                      varchar(500),
    ad                      varchar(500),
    hash                    varchar(500),
    table_name              varchar(200),
    hash_table              varchar(500),
    hash_dc                 varchar(500)
    );