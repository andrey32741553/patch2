INSERT OVERWRITE TABLE ${ods_schema_name}.pensionregistry
        SELECT dws_job, deleted_flag, insert_date, vd, po, sm, pm, d, hash, table_name, hash_table, hash_dc
        FROM (SELECT *, row_number() over (partition by hash_dc, d, vd, sm order by insert_date desc) as row_num
            FROM (
                 SELECT
                    dws_job, deleted_flag, insert_date, vd, po, sm, pm, d, hash, table_name, hash_table, hash_dc
                 FROM ${ods_schema_name}.pensionregistry
                 UNION ALL
                 SELECT
                    '${dws_job}' as dws_job,
                        CASE WHEN (dws_act = 'D') THEN 'Y' ELSE 'N' END as deleted_flag,
                        '${insert_date}' as insert_date,
                        vd, po, sm, pm, d, hash, table_name, hash_table, hash_dc
                FROM ${ods_schema_name}.pensionregistry_delta
                WHERE `date` = '${insert_date_delta}'
                ) union_delta_mirror) mirror where row_num = 1;