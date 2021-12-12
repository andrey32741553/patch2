FROM (SELECT *,
                      count(vd) over (partition by vd, hash_doc order by insert_date desc) as count_num,
                      row_number() over (partition by vd, hash_doc order by insert_date desc) as row_num FROM
            (SELECT
                '${dws_job}'                                AS dws_job,
                dws_act,
                '${insert_date}'                            AS insert_date,
                vd,
                po,
                sm,
                pm,
                d,
                hash,
                table_name,
                hash_table,
                hash_dc
            FROM (SELECT
                         CASE
                                 WHEN (mirror.vd is null) or (mirror.hash_doc is null)
                                     THEN 'I'
                                 WHEN (src.vd is null) or (src.hash_doc is null)
                                     THEN 'D'
                                 ELSE ''
                             END                                 AS dws_act,


                         FROM ${ods_schema_name}.pensionregistry_dsrc AS src

                INSERT INTO TABLE ${ods_schema_name}.pensionregistry_delta partition (`date`)
                    SELECT
                dws_job,
                dws_act,
                insert_date,
                vd,
                po,
                sm,
                pm,
                d,
                hash,
                table_name,
                hash_table,
                hash_dc
                        '${date}' as `date`
                    WHERE row_num = 1

               INSERT INTO TABLE ${ods_schema_name}.pensionregistry_double
                    SELECT
                dws_job,
                dws_act,
                insert_date,
                vd,
                po,
                sm,
                pm,
                d,
                hash,
                table_name,
                hash_table,
                hash_dc
                    WHERE count_num > 1;