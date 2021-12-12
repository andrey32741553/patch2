INSERT OVERWRITE TABLE ${ods_schema_name}.pensionreceiver
                 SELECT nk,
                        dws_job,
                        dws_act,
                        dws_uniact,
                        insert_date,
                        deleted_flag,
                        default_flag,
                        insert_date,
                        gn,
                        nm/f,
                        nm/i,
                        nm/o,
                        dc,
                        ad,
                        hash,
                        table_name,
                        hash_table,
                        hash_doc
FROM (SELECT *, row_number() over (partition hash_doc id order by insert_date desc) as row_num
      FROM (
               SELECT nk,
                      dws_job,
                      dws_act,
                      dws_uniact,
                      insert_date,
                      deleted_flag,
                      default_flag,
                      insert_date,
                      gn,
                      nm/f,
                      nm/i,
                      nm/o,
                      dc,
                      ad,
                      hash,
                      table_name,
                      hash_table,
                      hash_doc
               FROM ${ods_schema_name}.pensionreceiver
               UNION ALL
               SELECT '${dws_job}'                                    as dws_job,
                      CASE WHEN (dws_act = 'D') THEN 'Y' ELSE 'N' END as deleted_flag,
                      '${insert_date}'                                as insert_date,
                      nk,
                      "N"                                             as default_flag,
                      dws_act,
                      dws_uniact,
                      insert_date,
                      gn,
                      nm/f,
                      nm/i,
                      nm/o,
                      dc,
                      ad,
                      hash,
                      table_name,
                      hash_table,
                      hash_doc
               FROM ${ods_schema_name}.pensionreceiver_delta
               WHERE date = '${insert_date_delta}'
           ) union_delta_mirror) mirror
where row_num = 1