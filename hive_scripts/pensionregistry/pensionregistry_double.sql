DROP TABLE IF EXISTS ${ods_schema_name}.pensionregistry_double;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.pensionregistry_double
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
) STORED AS ORC;