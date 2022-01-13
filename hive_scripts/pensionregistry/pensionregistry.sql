DROP TABLE IF EXISTS ${ods_schema_name}.pensionregistry;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.pensionregistry
(
    dws_job                 string,
    deleted_flag            string,
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