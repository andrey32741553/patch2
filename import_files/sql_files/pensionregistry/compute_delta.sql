set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

msck repair table ${ods_schema_name}.pensionregistry_dsrc;

FROM (SELECT *, count(hash_dc) over (partition by hash_dc, d, vd, sm order by insert_date desc) as count_num,
                row_number() over (partition by hash_dc, d, vd, sm order by insert_date desc) as row_num FROM
(SELECT
    dws_job, insert_date, dws_act, vd, po, sm, pm, d, hash, table_name, hash_table, hash_dc, `date` FROM
                         (SELECT
                         		  src.`date`								  AS `date`,
                                  '${dws_job}'                                AS dws_job,
                                  '${insert_date}'                            AS insert_date,

                         CASE
                                WHEN ((mirror.hash_dc is null and mirror.d is null and mirror.vd is null and
                                    mirror.sm is null) or
                                    (mirror.hash_dc = src.hash_dc and mirror.d = src.d and mirror.vd = src.vd and
                                    mirror.sm = src.sm and mirror.deleted_flag = 'Y' and
                                    '${insert_date}' > mirror.insert_date))
                                     THEN 'I'
                                WHEN (src.hash_dc is null and mirror.d is null and mirror.vd is null and
                                    mirror.sm is null and mirror.deleted_flag = 'N')
                                     THEN 'D'
                                WHEN (mirror.deleted_flag = 'N') and
                                     ((src.po <=> mirror.po) = FALSE or
                                     (src.pm <=> mirror.pm) = FALSE or
                                     (src.hash <=> mirror.hash) = FALSE)
                                        THEN 'U'
                                 ELSE null
                             END                                                AS dws_act,

                         CASE
                                WHEN (src.vd is not null)
                                    THEN src.vd
                                WHEN (mirror.vd is not null)
                                    THEN mirror.vd
                                ELSE null
                         END                                                         AS vd,

                         CASE
                                WHEN (src.po is not null)
                                    THEN src.po
                                WHEN (mirror.po is not null)
                                    THEN mirror.po
                                ELSE null
                         END                                                        AS po,

                         CASE
                                WHEN (src.sm is not null)
                                    THEN src.sm
                                WHEN (mirror.sm is not null)
                                    THEN mirror.sm
                                ELSE null
                         END                                                        AS sm,

                         CASE
                                WHEN (src.pm is not null)
                                    THEN src.pm
                                WHEN (mirror.pm is not null)
                                    THEN mirror.pm
                                ELSE null
                         END                                                         AS pm,

                         CASE
                                WHEN (src.d is not null)
                                    THEN src.d
                                WHEN (mirror.d is not null)
                                    THEN mirror.d
                                ELSE null
                         END                                                            AS d,

                         CASE
                                WHEN (src.hash is not null)
                                    THEN src.hash
                                WHEN (mirror.hash is not null)
                                    THEN mirror.hash
                                ELSE null
                         END                                                            AS hash,

                         CASE
                                WHEN (src.table_name is not null)
                                    THEN src.table_name
                                WHEN (mirror.table_name is not null)
                                    THEN mirror.table_name
                                ELSE null
                         END                                                            AS table_name,

                         CASE
                                WHEN (src.hash_table is not null)
                                    THEN src.hash_table
                                WHEN (mirror.hash_table is not null)
                                    THEN mirror.hash_table
                                ELSE null
                         END                                                            AS hash_table,

                         CASE
                                WHEN (src.hash_dc is not null)
                                    THEN src.hash_dc
                                WHEN (mirror.hash_dc is not null)
                                    THEN mirror.hash_dc
                                ELSE null
                         END                                                            AS hash_dc

                         FROM ${ods_schema_name}.pensionregistry_dsrc AS src
                         FULL OUTER JOIN ${ods_schema_name}.pensionregistry AS mirror
                         ON (src.hash_dc = mirror.hash_dc) AND
                            (src.d = mirror.d) AND
                            (src.vd = mirror.vd) AND
                            (src.sm = mirror.sm) WHERE `date` = '${date}') delta) delta_num) rsl


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
                        hash_dc,
                        '${date}' as `date` WHERE row_num = 1


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
                         hash_dc WHERE count_num > 1;
