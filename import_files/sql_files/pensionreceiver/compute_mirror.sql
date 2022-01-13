CREATE LOCAL TEMPORARY VIEW tmp_view_mirror
    AS (SELECT *,
CASE
    WHEN hash is null THEN row_number() over (partition by hash_dc order by insert_date desc)
ELSE row_number() over (partition by hash order by insert_date desc) END      AS row_num
      FROM (SELECT nk,
                    dws_job,
                    deleted_flag,
                    default_flag,
                    insert_date,
                    gn,
                    "nm/f",
                    "nm/i",
                    "nm/o",
                    dc,
                    ad,
                    hash,
                    table_name,
                    hash_table,
                    hash_dc
               FROM ${ods_schema_name}.pensionreceiver
               UNION ALL
               SELECT
                    nk,
                   '${dws_job}' as dws_job,
                      'N' as deleted_flag,
                      'N' as default_flag,
                      '${insert_date}' as insert_date,
                      gn,
                      "nm/f",
                      "nm/i",
                      "nm/o",
                      dc,
                      ad,
                      hash,
                      table_name,
                      hash_table,
                      hash_dc
               FROM ${ods_schema_name}.pensionreceiver_delta
           ) union_delta_mirror);

TRUNCATE TABLE ${ods_schema_name}.pensionreceiver;

INSERT INTO ${ods_schema_name}.pensionreceiver
                 SELECT nk,
                        dws_job,
                        deleted_flag,
                        default_flag,
                        insert_date,
                        gn,
                        "nm/f",
                        "nm/i",
                        "nm/o",
                        dc,
                        ad,
                        hash,
                        table_name,
                        hash_table,
                        hash_dc
FROM tmp_view_mirror where row_num = 1;