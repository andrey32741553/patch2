FROM (SELECT *,
                      count(hash_doc) over (partition by hash_doc order by insert_date desc) as count_num,
                      row_number() over (partition by hash_doc order by insert_date desc) as row_num FROM
            (SELECT
                        nk,
                        dws_job,
                        dws_act,
                        dws_uniact,
                        insert_date,
                        deleted_flag,
                        default_flag,
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
            FROM (SELECT coalesce(mirror.nk, reflect("java.util.UUID", "randomUUID")) AS nk,
                         '${dws_job}'                                            AS dws_job,
                         '${insert_date}'                                    AS insert_date,
                         'N'                                                  AS dws_uniact,
                         CASE
                                 WHEN ((mirror.hash_doc is null)
                                        or (mirror.hash_doc = src.hash_doc and mirror.deleted_flag = 'Y' and
                                        '${insert_date}' > mirror.insert_date))
                                     THEN 'I'
                                 WHEN (src.hash_doc is null and mirror.deleted_flag = 'N')
                                     THEN 'D'
                                 WHEN (mirror.deleted_flag = 'N') and
                                     ((src.gn <=> mirror.gn) = FALSE or
                                     (src.nm/f <=> mirror.nm/f) = FALSE or
                                     (src.nm/i <=> mirror.nm/i) = FALSE or
                                     (src.nm/o <=> mirror.nm/o) = FALSE or
                                     (src.dc <=> mirror.dc) = FALSE or
                                     (src.ad <=> mirror.ad) = FALSE or
                                     (src.hash <=> mirror.hash) = FALSE or
                                     (src.table_name <=> mirror.table_name) = FALSE or
                                     (src.hash_table <=> mirror.hash_table) = FALSE)
                                        THEN 'U'
                                 ELSE ''
                             END                                                AS dws_act,

                         CASE
                                WHEN (src.deleted_flag is not null)
                                    THEN src.deleted_flag
                                WHEN (mirror.deleted_flag is not null)
                                    THEN mirror.deleted_flag
                                ELSE null
                         END                                                AS deleted_flag,

                         CASE
                                WHEN (src.default_flag is not null)
                                    THEN src.default_flag
                                WHEN (mirror.default_flag is not null)
                                    THEN mirror.default_flag
                                ELSE null
                         END                                                AS default_flag,

                         CASE
                                WHEN (src.gn is not null)
                                    THEN src.gn
                                WHEN (mirror.gn is not null)
                                    THEN mirror.gn
                                ELSE null
                         END                                                         AS gn,

                         CASE
                                WHEN (src.nm/f is not null)
                                    THEN src.nm/f
                                WHEN (mirror.nm/f is not null)
                                    THEN mirror.nm/f
                                ELSE null
                         END                                                        AS nm/f,

                         CASE
                                WHEN (src.nm/i is not null)
                                    THEN src.nm/i
                                WHEN (mirror.nm/i is not null)
                                    THEN mirror.nm/i
                                ELSE null
                         END                                                         AS nm/i,

                         CASE
                                WHEN (src.nm/o is not null)
                                    THEN src.nm/o
                                WHEN (mirror.nm/o is not null)
                                    THEN mirror.nm/o
                                ELSE null
                         END                                                         AS nm/o,

                         CASE
                                WHEN (src.dc is not null)
                                    THEN src.dc
                                WHEN (mirror.dc is not null)
                                    THEN mirror.dc
                                ELSE null
                         END                                                            AS dc,

                         CASE
                                WHEN (src.ad is not null)
                                    THEN src.ad
                                WHEN (mirror.ad is not null)
                                    THEN mirror.ad
                                ELSE null
                         END                                                            AS ad

                         FROM ${ods_schema_name}.pensionreciever_dsrc AS src
                         FULL OUTER JOIN ${ods_schema_name}.pensionreciever
                         AS mirror ON src.hash_doc = mirror.hash_doc) delta WHERE delta.dws_act <> "") delta_num ) rsl

                INSERT INTO TABLE ${ods_schema_name}.pensionreceiver_delta partition (`date`)
                    SELECT
                        nk,
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
                        '${date}' as `date`
                    WHERE row_num = 1

               INSERT INTO TABLE ${ods_schema_name}.pensionreceiver_double
                    SELECT
                            dws_job,
                            insert_date,
                            effective_flag,
                            algorithm_ncode,
                            nk,
                            deleted_flag,
                            default_flag,
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
                    WHERE count_num > 1;