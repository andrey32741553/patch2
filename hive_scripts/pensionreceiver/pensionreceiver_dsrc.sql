DROP TABLE IF EXISTS ${ods_schema_name}.pensionreceiver_dsrc;

CREATE TABLE ${ods_schema_name}.pensionreceiver_dsrc
(
    gn varchar(20),
    "nm/f" varchar(100),
    "nm/i" varchar(100),
    "nm/o" varchar(100),
    dc varchar(500),
    ad varchar(500),
    hash varchar(500),
    table_name varchar(500),
    hash_table varchar(500),
    hash_dc varchar(500),
    snapshot int,
    creation_date varchar(20),
    deleted_flag boolean,
    date varchar(20),
    system_id varchar(20)
) PARTITION BY date;