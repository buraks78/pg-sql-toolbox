WITH
    sti AS (
        -- summarize index statistics for all indexes in a table
        SELECT
            psui.relid,

            COUNT(psui.indexrelid)  AS sti_idx_count,

            SUM(psui.idx_scan)      AS sti_idx_scan,
            SUM(psui.idx_tup_read)  AS sti_idx_tup_read,
            SUM(psui.idx_tup_fetch) AS sti_idx_tup_fetch,

            SUM(psuiio.idx_blks_read) AS sti_idx_blks_read,
            SUM(psuiio.idx_blks_hit)  AS sti_idx_blks_hit,

            SUM(pc.reltuples) AS sti_reltuples,
            SUM(pc.relpages)  AS sti_relpages,

            -- weighted average of index tuples per page biased on activity
            COALESCE(ROUND(SUM(psui.idx_tup_read * i.idx_tup_per_blk)::numeric / NULLIF(SUM(psui.idx_tup_read), 0) , 2), 0) AS sti_idx_tup_per_blk,
            -- weighted average of index tuples read per page biased on activity
            COALESCE(ROUND(SUM(psui.idx_tup_read * i.idx_tup_read_per_blk)::numeric / NULLIF(SUM(psui.idx_tup_read), 0), 2), 0) AS sti_idx_tup_read_per_blk,
            -- weighted average of index block cache hit ratio biased on activity
            COALESCE(ROUND(SUM(psui.idx_tup_read * i.idx_cache_hit)::numeric / NULLIF(SUM(psui.idx_tup_read), 0), 6), 0) AS sti_idx_cache_hit

        FROM pg_stat_user_indexes AS psui
        INNER JOIN pg_statio_user_indexes  AS psuiio ON psuiio.relid = psui.relid AND psuiio.indexrelid = psui.indexrelid
        INNER JOIN pg_class AS pc ON pc.oid = psui.indexrelid
        CROSS JOIN LATERAL (
            SELECT
                -- index tuples per block
                COALESCE(pc.reltuples::numeric / NULLIF(pc.relpages, 0), 0) AS idx_tup_per_blk,
                -- index tuples read per block
                COALESCE(psui.idx_tup_read::numeric / NULLIF(psuiio.idx_blks_hit + psuiio.idx_blks_read, 0), 0) AS idx_tup_read_per_blk,
                -- index block hit ratio
                COALESCE(psuiio.idx_blks_hit::numeric / NULLIF(psuiio.idx_blks_hit + psuiio.idx_blks_read, 0), 0) AS idx_cache_hit
        ) AS i
        GROUP BY psui.relid
    ),
    si AS (
        -- summarize index statistics for all indexes in a schema
        SELECT
            psui.schemaname,

            SUM(psui.idx_scan)      AS si_idx_scan,
            SUM(psui.idx_tup_read)  AS si_idx_tup_read,
            SUM(psui.idx_tup_fetch) AS si_idx_tup_fetch,

            SUM(psuiio.idx_blks_read) AS si_idx_blks_read,
            SUM(psuiio.idx_blks_hit)  AS si_idx_blks_hit,

            SUM(pc.reltuples) AS si_reltuples,
            SUM(pc.relpages)  AS si_relpages

        FROM pg_stat_user_indexes    AS psui
        INNER JOIN pg_statio_user_indexes AS psuiio ON psuiio.relid = psui.relid AND psuiio.indexrelid = psui.indexrelid
        INNER JOIN pg_class AS pc ON pc.oid = psui.indexrelid
        GROUP BY psui.schemaname
    ),
    st AS (
        -- summarize table statistics for all tables in a schema
        SELECT
            psut.schemaname,

            SUM(psut.seq_scan)        AS st_seq_scan,
            SUM(psut.seq_tup_read)    AS st_seq_tup_read,
            SUM(psut.idx_scan)        AS st_idx_scan,
            SUM(psut.idx_tup_fetch)   AS st_idx_tup_fetch,

            SUM(psut.n_tup_ins)       AS st_n_tup_ins,
            SUM(psut.n_tup_upd)       AS st_n_tup_upd,
            SUM(psut.n_tup_del)       AS st_n_tup_del,
            SUM(psut.n_tup_hot_upd)   AS st_n_tup_hot_upd,
            SUM(psut.n_live_tup)      AS st_n_live_tup,
            SUM(psut.n_dead_tup)      AS st_n_dead_tup,

            SUM(psutio.heap_blks_read)  AS st_heap_blks_read,
            SUM(psutio.heap_blks_hit)   AS st_heap_blks_hit,
            SUM(psutio.idx_blks_read)   AS st_idx_blks_read,
            SUM(psutio.idx_blks_hit)    AS st_idx_blks_hit,
            SUM(psutio.toast_blks_read) AS st_toast_blks_read,
            SUM(psutio.toast_blks_hit)  AS st_toast_blks_hit,
            SUM(psutio.tidx_blks_read)  AS st_tidx_blks_read,
            SUM(psutio.tidx_blks_hit)   AS st_tidx_blks_hit,

            SUM(pc.reltuples) AS st_reltuples,
            SUM(pc.relpages)  AS st_relpages

        FROM pg_stat_user_tables AS psut
        INNER JOIN pg_statio_user_tables AS psutio ON psutio.relid = psut.relid
        INNER JOIN pg_class AS pc ON pc.oid = psut.relid
        GROUP BY psut.schemaname
    ),
    s AS (
        SELECT
            psut.relid,
            psut.schemaname,
            psut.relname,

            sti.sti_idx_tup_per_blk,
            sti.sti_idx_tup_read_per_blk,
            sti.sti_idx_cache_hit,

            s.s_heap_tup_per_blk,
            s.s_heap_tup_read_per_blk,
            s.s_heap_cache_hit,

            w.w_seq_scan,
            w.w_seq_tup_read,
            w.w_idx_scan,
            w.w_idx_tup_fetch,
            w.w_ins,
            w.w_upd,
            w.w_del,
            w.w_hot_upd,
            w.w_live,
            w.w_dead,
            w.w_tup,
            w.w_reltuples,
            w.w_relpages,
            w.w_heap_blks_read,
            w.w_heap_blks_hit,
            w.w_idx_blks_read,
            w.w_idx_blks_hit,
            w.w_sti_idx_scan,
            w.w_sti_idx_tup_read,
            w.w_sti_idx_tup_fetch,
            w.w_sti_idx_blks_read,
            w.w_sti_idx_blks_hit,
            w.w_sti_reltuples,
            w.w_sti_relpages,

            -- index read cost
            ROUND(
                sti.sti_idx_blks_read *
                f.c_idx_blk_read
            , 2) as c_idx_read,

            -- index write cost
            ROUND(
                sti.sti_idx_count *
                (psut.n_tup_ins + psut.n_tup_upd - psut.n_tup_hot_upd + psut.n_tup_del) *
                (1 / COALESCE(NULLIF(sti.sti_idx_tup_per_blk, 0), 1)) *
                f.c_idx_blk_write
            , 2) AS c_idx_write,

            -- index scan cost (heap fetch)
            ROUND(
                psut.idx_tup_fetch *
                (1 / COALESCE(NULLIF(s.s_heap_tup_read_per_blk, 0), 1)) *
                f.c_heap_blk_read *
                (1 - s.s_heap_cache_hit)
            , 2) AS c_idx_scan,

            -- sequential scan cost (heap fetch)
            ROUND(
                psut.seq_tup_read *
                (1 / COALESCE(NULLIF(s.s_heap_tup_read_per_blk, 0), 1)) *
                f.c_heap_blk_read *
                (1 - s.s_heap_cache_hit)
            , 2) AS c_seq_scan,

            -- index gain (sequential scan cost that was eliminated)
            ROUND(
                psut.idx_scan *
                -- upper limit per op
                pc.relpages *
                f.c_heap_blk_read *
                -- adjusted miss rate
                (1 - POWER(s.s_heap_cache_hit, 1 + f.k_cache_adjustor * r.r_relpages))
            , 2) AS c_idx_gain

        FROM pg_stat_user_tables AS psut
        INNER JOIN pg_statio_user_tables AS psutio ON psutio.relid = psut.relid
        INNER JOIN pg_class AS pc ON pc.oid = psut.relid
        INNER JOIN st ON st.schemaname = psut.schemaname
        INNER JOIN si ON si.schemaname = psut.schemaname
        INNER JOIN sti ON sti.relid = psut.relid
        CROSS JOIN LATERAL (
            SELECT
                COALESCE(ROUND(psut.seq_scan::numeric / NULLIF(st.st_seq_scan, 0) * 100, 2), 0) AS w_seq_scan,
                COALESCE(ROUND(psut.seq_tup_read::numeric / NULLIF(st.st_seq_tup_read, 0) * 100, 2), 0) AS w_seq_tup_read,
                COALESCE(ROUND(psut.idx_scan::numeric / NULLIF(st.st_idx_scan, 0) * 100, 2), 0) AS w_idx_scan,
                COALESCE(ROUND(psut.idx_tup_fetch::numeric / NULLIF(st.st_idx_tup_fetch, 0) * 100, 2), 0) AS w_idx_tup_fetch,
                COALESCE(ROUND(psut.n_tup_ins::numeric / NULLIF(st.st_n_tup_ins, 0) * 100, 2), 0) AS w_ins,
                COALESCE(ROUND(psut.n_tup_upd::numeric / NULLIF(st.st_n_tup_upd, 0) * 100, 2), 0) AS w_upd,
                COALESCE(ROUND(psut.n_tup_del::numeric / NULLIF(st.st_n_tup_del, 0) * 100, 2), 0) AS w_del,
                COALESCE(ROUND(psut.n_tup_hot_upd::numeric / NULLIF(st.st_n_tup_hot_upd, 0) * 100, 2), 0) AS w_hot_upd,
                COALESCE(ROUND(psut.n_live_tup::numeric / NULLIF(st.st_n_live_tup, 0) * 100, 2), 0) AS w_live,
                COALESCE(ROUND(psut.n_dead_tup::numeric / NULLIF(st.st_n_dead_tup, 0) * 100, 2), 0) AS w_dead,
                COALESCE(ROUND((psut.n_live_tup + psut.n_dead_tup)::numeric / NULLIF(st.st_n_live_tup + st.st_n_dead_tup, 0) * 100, 2), 0) AS w_tup,

                COALESCE(ROUND(pc.reltuples::numeric / NULLIF(st.st_reltuples::numeric, 0) * 100, 2), 0) AS w_reltuples,
                COALESCE(ROUND(pc.relpages::numeric / NULLIF(st.st_relpages, 0) * 100, 2), 0) AS w_relpages,

                COALESCE(ROUND(psutio.heap_blks_read::numeric / NULLIF(st.st_heap_blks_read, 0) * 100, 2), 0) AS w_heap_blks_read,
                COALESCE(ROUND(psutio.heap_blks_hit::numeric / NULLIF(st.st_heap_blks_hit, 0) * 100, 2), 0) AS w_heap_blks_hit,
                COALESCE(ROUND(psutio.idx_blks_read::numeric / NULLIF(st.st_idx_blks_read, 0) * 100, 2), 0) AS w_idx_blks_read,
                COALESCE(ROUND(psutio.idx_blks_hit::numeric / NULLIF(st.st_idx_blks_hit, 0) * 100, 2), 0) AS w_idx_blks_hit,
                COALESCE(ROUND(psutio.toast_blks_read::numeric / NULLIF(st.st_toast_blks_read, 0) * 100, 2), 0) AS w_toast_blks_read,
                COALESCE(ROUND(psutio.toast_blks_hit::numeric / NULLIF(st.st_toast_blks_hit, 0) * 100, 2), 0) AS w_toast_blks_hit,
                COALESCE(ROUND(psutio.tidx_blks_read::numeric / NULLIF(st.st_tidx_blks_read, 0) * 100, 2), 0) AS w_tidx_blks_read,
                COALESCE(ROUND(psutio.tidx_blks_hit::numeric / NULLIF(st.st_tidx_blks_hit, 0) * 100, 2), 0) AS w_tidx_blks_hit,

                COALESCE(ROUND(sti.sti_idx_scan::numeric / NULLIF(si.si_idx_scan, 0) * 100, 2), 0) AS w_sti_idx_scan,
                COALESCE(ROUND(sti.sti_idx_tup_read::numeric / NULLIF(si.si_idx_tup_read, 0) * 100, 2), 0) AS w_sti_idx_tup_read,
                COALESCE(ROUND(sti.sti_idx_tup_fetch::numeric / NULLIF(si.si_idx_tup_fetch, 0) * 100, 2), 0) AS w_sti_idx_tup_fetch,
                COALESCE(ROUND(sti.sti_idx_blks_read::numeric / NULLIF(si.si_idx_blks_read, 0) * 100, 2), 0) AS w_sti_idx_blks_read,
                COALESCE(ROUND(sti.sti_idx_blks_hit::numeric / NULLIF(si.si_idx_blks_hit, 0) * 100, 2), 0) AS w_sti_idx_blks_hit,

                COALESCE(ROUND(sti.sti_reltuples::numeric / NULLIF(si.si_reltuples::numeric, 0) * 100, 2), 0) AS w_sti_reltuples,
                COALESCE(ROUND(sti.sti_relpages::numeric / NULLIF(si.si_relpages, 0) * 100, 2), 0) AS w_sti_relpages

        ) AS w
        CROSS JOIN LATERAL (
            SELECT
                COALESCE(psut.n_live_tup::numeric / NULLIF(st.st_n_live_tup, 0), 0) AS r_live,
                COALESCE(psut.n_dead_tup::numeric / NULLIF(st.st_n_dead_tup, 0), 0) AS r_dead,
                COALESCE((psut.n_live_tup + psut.n_dead_tup)::numeric / NULLIF(st.st_n_live_tup + st.st_n_dead_tup, 0), 0) AS r_tup,

                COALESCE(pc.reltuples::numeric / NULLIF(st.st_reltuples::numeric, 0), 0) AS r_reltuples,
                COALESCE(pc.relpages::numeric / NULLIF(st.st_relpages, 0), 0) AS r_relpages,

                COALESCE(psutio.heap_blks_read::numeric / NULLIF(st.st_heap_blks_read, 0), 0) AS r_heap_blks_read,
                COALESCE(psutio.heap_blks_hit::numeric / NULLIF(st.st_heap_blks_hit, 0), 0) AS r_heap_blks_hit,
                COALESCE(psutio.idx_blks_read::numeric / NULLIF(st.st_idx_blks_read, 0), 0) AS r_idx_blks_read,
                COALESCE(psutio.idx_blks_hit::numeric / NULLIF(st.st_idx_blks_hit, 0), 0) AS r_idx_blks_hit
        ) AS r
        CROSS JOIN LATERAL (
            SELECT
                COALESCE(ROUND(pc.reltuples::numeric / NULLIF(pc.relpages, 0), 2), 0) AS s_heap_tup_per_blk,
                COALESCE(ROUND((psut.seq_tup_read + psut.idx_tup_fetch)::numeric / NULLIF(psutio.heap_blks_hit + psutio.heap_blks_read, 0), 2), 0) AS s_heap_tup_read_per_blk,
                COALESCE(ROUND(psutio.heap_blks_hit::numeric / NULLIF(psutio.heap_blks_hit + psutio.heap_blks_read, 0), 6), 0) AS s_heap_cache_hit
        ) AS s
        CROSS JOIN LATERAL (
            SELECT
                1 AS c_idx_blk_write,
                1 AS c_idx_blk_read,
                1 AS c_heap_blk_read,
                1 AS k_cache_adjustor
        ) AS f

    )
SELECT
    s.relid,
    s.relname,
    s.schemaname,
    s.sti_idx_tup_per_blk,
    s.sti_idx_tup_read_per_blk,
    s.sti_idx_cache_hit,
    s.s_heap_tup_per_blk,
    s.s_heap_tup_read_per_blk,
    s.s_heap_cache_hit,
    s.w_seq_scan,
    s.w_seq_tup_read,
    s.w_idx_scan,
    s.w_idx_tup_fetch,
    s.w_ins,
    s.w_upd,
    s.w_del,
    s.w_hot_upd,
    s.w_live,
    s.w_dead,
    s.w_tup,
    s.w_reltuples,
    s.w_relpages,
    s.w_heap_blks_read,
    s.w_heap_blks_hit,
    s.w_idx_blks_read,
    s.w_idx_blks_hit,
    s.w_sti_idx_scan,
    s.w_sti_idx_tup_read,
    s.w_sti_idx_tup_fetch,
    s.w_sti_idx_blks_read,
    s.w_sti_idx_blks_hit,
    s.w_sti_reltuples,
    s.w_sti_relpages,

    COALESCE(ROUND(s.c_idx_read::numeric / NULLIF(SUM(s.c_idx_read::numeric) OVER (PARTITION BY s.schemaname), 0) * 100, 2), 0) AS w_c_idx_read,
    COALESCE(ROUND(s.c_idx_write::numeric / NULLIF(SUM(s.c_idx_write::numeric) OVER (PARTITION BY s.schemaname), 0) * 100, 2), 0) AS w_c_idx_write,
    COALESCE(ROUND(s.c_idx_scan::numeric / NULLIF(SUM(s.c_idx_scan::numeric) OVER (PARTITION BY s.schemaname), 0) * 100, 2), 0) AS w_c_idx_scan,
    COALESCE(ROUND(s.c_seq_scan::numeric / NULLIF(SUM(s.c_seq_scan::numeric) OVER (PARTITION BY s.schemaname), 0) * 100, 2), 0) AS w_c_seq_scan,
    COALESCE(ROUND(s.c_idx_gain::numeric / NULLIF(SUM(s.c_idx_gain::numeric) OVER (PARTITION BY s.schemaname), 0) * 100, 2), 0) AS w_c_idx_gain

FROM s
ORDER BY s.schemaname, s.relname;
