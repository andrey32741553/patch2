CREATE VIEW ${ods_schema_name}.pensionreceiver_nk_link AS
SELECT
    nk                      varchar(100),
    hash_dc                 varchar(500),
    dws_job                 varchar(100),
    insert_date             varchar(20)
    FROM ${ods_schema_name}.pensionreceiver;
