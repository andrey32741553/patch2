DROP TABLE IF NOT EXISTS ${ods_schema_name}.pensionreceiver_dsrc;

CREATE TABLE ${ods_schema_name}.pensionreceiver_dsrc
(
    gn                      int,
    "nm/f"                  varchar(100),
    "nm/i"                  varchar(100),
    "nm/o"                  varchar(100),
    dc                      varchar(50),
    ad                      varchar(500),
    hash                    varchar(100),
    table_name              varchar(100),
    hash_table              varchar(100),
    hash_dc                 varchar(100),
    snapshot                int,
    creation_date           timestamp,
    deleted_flag            boolean,
    date                    date
    system_id               varchar(8)
) PARTITION BY              date;