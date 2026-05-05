/* =============================================================================
   CPP / DataPreProcessor Application-Level Recon & Feed File Analysis
   -----------------------------------------------------------------------------
   Purpose:
   You are tracking from one Spark/YARN application_id / run_id that ran for a
   long duration, for example 5 hours.

   This script helps answer:
   1. For this application_id, how long did it run?
   2. Which recons were submitted in this application_id?
   3. For each recon, how many feed/file rows were submitted?
   4. What are the file names, statuses, record counts, and timing?
   5. Which recons had the highest file count / record count / runtime spread?
   6. What metadata/status exists for those recons?
   7. What expected feed/file patterns exist in metadata vs actual feed files?

   Main audit table assumed:
   FASTTRAC.OM_RECON_AUDIT_LOG

   Main config/metadata tables/views used:
   RECONMGMT.CITI_RECON
   RECONMGMT.AUDIT_CITI_RECON
   RECONMGMT.V_OM_ONE_RECON_CONFIG
   RECONMGMT.V_OM_ONE_RECON_MERGER_CONFIG
   FASTTRAC.RECONCILIATION
   FASTTRAC.FILTERFEED
   FASTTRAC.GENERATEDFEED

   IMPORTANT:
   - This script assumes audit timestamp column is CREATED_TIME.
   - If your environment uses START_TIME instead of CREATED_TIME, replace
     CREATED_TIME with START_TIME in this script.
   - This script assumes application id is stored in RUN_ID.
   - If application id is present only in MESSAGE, use Section 02B.

   Replace these values before running:
   APP_ID    = application id / run id you are investigating
   START_TS  = optional lower timestamp boundary
   END_TS    = optional upper timestamp boundary
============================================================================= */

DEFINE APP_ID   = '<<APPLICATION_ID>>'
DEFINE START_TS = '<<START_TS_YYYY-MM-DD HH24:MI:SS>>'
DEFINE END_TS   = '<<END_TS_YYYY-MM-DD HH24:MI:SS>>'


/* =============================================================================
   00. OPTIONAL COLUMN CHECK
   -----------------------------------------------------------------------------
   Use this first if you are unsure about the available columns in the audit table.
============================================================================= */

SELECT
    owner,
    table_name,
    column_id,
    column_name,
    data_type
FROM all_tab_columns
WHERE owner = 'FASTTRAC'
  AND table_name = 'OM_RECON_AUDIT_LOG'
ORDER BY column_id;


/* =============================================================================
   01. APPLICATION RUN SUMMARY
   -----------------------------------------------------------------------------
   Gives one-row summary for the application_id/run_id.
============================================================================= */

SELECT
    run_id,
    COUNT(*) AS audit_row_count,
    COUNT(DISTINCT recon_id) AS distinct_recon_count,
    COUNT(DISTINCT file_name) AS distinct_file_count,
    SUM(NVL(record_count, 0)) AS total_record_count,
    MIN(created_time) AS first_audit_time,
    MAX(created_time) AS last_audit_time,
    ROUND((CAST(MAX(created_time) AS DATE) - CAST(MIN(created_time) AS DATE)) * 24 * 60, 2) AS app_span_minutes,
    ROUND((CAST(MAX(created_time) AS DATE) - CAST(MIN(created_time) AS DATE)) * 24, 2) AS app_span_hours
FROM fasttrac.om_recon_audit_log
WHERE run_id = '&APP_ID'
  AND comp_nm IN ('DataPreProcessor', 'CorePreProcessor')
GROUP BY run_id;


/* =============================================================================
   02. APPLICATION COMPONENT SUMMARY
   -----------------------------------------------------------------------------
   Shows component/status breakdown for the same application_id.
============================================================================= */

SELECT
    run_id,
    comp_nm,
    status,
    COUNT(*) AS row_count,
    COUNT(DISTINCT recon_id) AS distinct_recon_count,
    COUNT(DISTINCT file_name) AS distinct_file_count,
    SUM(NVL(record_count, 0)) AS total_record_count,
    MIN(created_time) AS first_time,
    MAX(created_time) AS last_time,
    ROUND((CAST(MAX(created_time) AS DATE) - CAST(MIN(created_time) AS DATE)) * 24 * 60, 2) AS span_minutes
FROM fasttrac.om_recon_audit_log
WHERE run_id = '&APP_ID'
GROUP BY run_id, comp_nm, status
ORDER BY first_time, comp_nm, status;


/* =============================================================================
   02B. FALLBACK: FIND APPLICATION ID FROM MESSAGE
   -----------------------------------------------------------------------------
   Use this if RUN_ID is not populated but application id appears in MESSAGE.
============================================================================= */

SELECT
    REGEXP_SUBSTR(message, 'application_[0-9]+_[0-9]+') AS extracted_application_id,
    comp_nm,
    status,
    COUNT(*) AS row_count,
    COUNT(DISTINCT recon_id) AS distinct_recon_count,
    COUNT(DISTINCT file_name) AS distinct_file_count,
    MIN(created_time) AS first_time,
    MAX(created_time) AS last_time
FROM fasttrac.om_recon_audit_log
WHERE REGEXP_SUBSTR(message, 'application_[0-9]+_[0-9]+') = '&APP_ID'
  AND created_time >= TO_TIMESTAMP('&START_TS', 'YYYY-MM-DD HH24:MI:SS')
  AND created_time <  TO_TIMESTAMP('&END_TS', 'YYYY-MM-DD HH24:MI:SS')
GROUP BY REGEXP_SUBSTR(message, 'application_[0-9]+_[0-9]+'), comp_nm, status
ORDER BY first_time;


/* =============================================================================
   03. WHICH RECONS GOT SUBMITTED IN THIS APPLICATION
   -----------------------------------------------------------------------------
   Main manager/report query: recons, feed/file count, record count, timing.
============================================================================= */

SELECT
    recon_id,
    MAX(recon_name) AS recon_name,
    cob_date,
    run_id,
    COUNT(*) AS feed_file_row_count,
    COUNT(DISTINCT file_name) AS distinct_file_count,
    COUNT(DISTINCT file_type) AS distinct_file_type_count,
    SUM(NVL(record_count, 0)) AS total_record_count,
    MIN(created_time) AS first_file_time,
    MAX(created_time) AS last_file_time,
    ROUND((CAST(MAX(created_time) AS DATE) - CAST(MIN(created_time) AS DATE)) * 24 * 60, 2) AS recon_span_minutes,
    SUM(CASE WHEN UPPER(status) = 'SUCCESS' THEN 1 ELSE 0 END) AS success_file_count,
    SUM(CASE WHEN UPPER(status) = 'FAILED' THEN 1 ELSE 0 END) AS failed_file_count,
    SUM(CASE WHEN UPPER(status) NOT IN ('SUCCESS', 'FAILED') OR status IS NULL THEN 1 ELSE 0 END) AS other_status_count
FROM fasttrac.om_recon_audit_log
WHERE run_id = '&APP_ID'
  AND comp_nm IN ('DataPreProcessor', 'CorePreProcessor')
GROUP BY recon_id, cob_date, run_id
ORDER BY feed_file_row_count DESC, total_record_count DESC, first_file_time;


/* =============================================================================
   04. FILE-LEVEL DETAILS FOR ALL RECONS IN THIS APPLICATION
   -----------------------------------------------------------------------------
   Detailed evidence showing every file/feed row submitted under this application.
============================================================================= */

SELECT
    run_id,
    recon_id,
    recon_name,
    cob_date,
    comp_nm,
    status,
    file_name,
    file_type,
    record_count,
    created_time,
    message
FROM fasttrac.om_recon_audit_log
WHERE run_id = '&APP_ID'
  AND comp_nm IN ('DataPreProcessor', 'CorePreProcessor')
ORDER BY created_time, recon_id, file_name;


/* =============================================================================
   05. FEED/FILE TYPE BREAKDOWN PER RECON
   -----------------------------------------------------------------------------
   Shows how many files were submitted by recon and file_type.
============================================================================= */

SELECT
    recon_id,
    MAX(recon_name) AS recon_name,
    file_type,
    status,
    COUNT(*) AS feed_file_count,
    COUNT(DISTINCT file_name) AS distinct_file_count,
    SUM(NVL(record_count, 0)) AS total_record_count,
    MIN(created_time) AS first_file_time,
    MAX(created_time) AS last_file_time
FROM fasttrac.om_recon_audit_log
WHERE run_id = '&APP_ID'
  AND comp_nm IN ('DataPreProcessor', 'CorePreProcessor')
GROUP BY recon_id, file_type, status
ORDER BY recon_id, file_type, status;


/* =============================================================================
   06. STATUS BREAKDOWN PER RECON
   -----------------------------------------------------------------------------
   Shows SUCCESS/FAILED/other status count per recon for this application_id.
============================================================================= */

SELECT
    recon_id,
    MAX(recon_name) AS recon_name,
    status,
    COUNT(*) AS status_row_count,
    COUNT(DISTINCT file_name) AS distinct_file_count,
    SUM(NVL(record_count, 0)) AS total_record_count,
    MIN(created_time) AS first_status_time,
    MAX(created_time) AS last_status_time
FROM fasttrac.om_recon_audit_log
WHERE run_id = '&APP_ID'
  AND comp_nm IN ('DataPreProcessor', 'CorePreProcessor')
GROUP BY recon_id, status
ORDER BY recon_id, status;


/* =============================================================================
   07. FEED COMPLETION CADENCE / TIME GAP BETWEEN CONSECUTIVE FILE EVENTS
   -----------------------------------------------------------------------------
   Shows the time gap between consecutive file audit events within the same app.
   CREATED_TIME may represent audit write/completion time, not true process start.
============================================================================= */

WITH file_events AS (
    SELECT
        run_id,
        recon_id,
        recon_name,
        file_name,
        file_type,
        status,
        record_count,
        created_time,
        LAG(created_time) OVER (PARTITION BY run_id ORDER BY created_time, recon_id, file_name) AS previous_file_time,
        LAG(file_name) OVER (PARTITION BY run_id ORDER BY created_time, recon_id, file_name) AS previous_file_name,
        LAG(recon_id) OVER (PARTITION BY run_id ORDER BY created_time, recon_id, file_name) AS previous_recon_id
    FROM fasttrac.om_recon_audit_log
    WHERE run_id = '&APP_ID'
      AND comp_nm IN ('DataPreProcessor', 'CorePreProcessor')
)
SELECT
    run_id,
    recon_id,
    recon_name,
    file_name,
    file_type,
    status,
    record_count,
    previous_recon_id,
    previous_file_name,
    previous_file_time,
    created_time AS current_file_time,
    ROUND((CAST(created_time AS DATE) - CAST(previous_file_time AS DATE)) * 24 * 60, 2) AS gap_from_previous_file_minutes
FROM file_events
ORDER BY current_file_time, recon_id, file_name;


/* =============================================================================
   08. TOP GAPS BETWEEN FILE EVENTS
   -----------------------------------------------------------------------------
   Quickly identifies the largest time gaps inside this application_id.
============================================================================= */

WITH file_events AS (
    SELECT
        run_id,
        recon_id,
        recon_name,
        file_name,
        file_type,
        status,
        created_time,
        LAG(created_time) OVER (PARTITION BY run_id ORDER BY created_time, recon_id, file_name) AS previous_file_time,
        LAG(file_name) OVER (PARTITION BY run_id ORDER BY created_time, recon_id, file_name) AS previous_file_name,
        LAG(recon_id) OVER (PARTITION BY run_id ORDER BY created_time, recon_id, file_name) AS previous_recon_id
    FROM fasttrac.om_recon_audit_log
    WHERE run_id = '&APP_ID'
      AND comp_nm IN ('DataPreProcessor', 'CorePreProcessor')
),
gaps AS (
    SELECT
        run_id,
        previous_recon_id,
        previous_file_name,
        previous_file_time,
        recon_id AS current_recon_id,
        file_name AS current_file_name,
        created_time AS current_file_time,
        ROUND((CAST(created_time AS DATE) - CAST(previous_file_time AS DATE)) * 24 * 60, 2) AS gap_minutes
    FROM file_events
    WHERE previous_file_time IS NOT NULL
)
SELECT *
FROM gaps
ORDER BY gap_minutes DESC NULLS LAST;


/* =============================================================================
   09. RECON STATUS METADATA FOR RECONS IN THIS APPLICATION
   -----------------------------------------------------------------------------
   Pulls current recon metadata/status for only the recons present in this app.
============================================================================= */

WITH app_recons AS (
    SELECT DISTINCT TO_CHAR(recon_id) AS recon_id
    FROM fasttrac.om_recon_audit_log
    WHERE run_id = '&APP_ID'
      AND comp_nm IN ('DataPreProcessor', 'CorePreProcessor')
)
SELECT
    cr.recon_id,
    cr.rec_portal_id,
    cr.recon_name,
    cr.user_group,
    cr.recon_status,
    cr.created_by,
    cr.created_ts,
    cr.modified_by,
    cr.modified_ts
FROM reconmgmt.citi_recon cr
JOIN app_recons ar ON TO_CHAR(cr.recon_id) = ar.recon_id
ORDER BY cr.recon_status, cr.recon_name;


/* =============================================================================
   10. LATEST AUDIT_CITI_RECON STATUS FOR RECONS IN THIS APPLICATION
   -----------------------------------------------------------------------------
   Pulls latest audited metadata/status for recons in this app.
============================================================================= */

WITH app_recons AS (
    SELECT DISTINCT TO_CHAR(recon_id) AS recon_id
    FROM fasttrac.om_recon_audit_log
    WHERE run_id = '&APP_ID'
      AND comp_nm IN ('DataPreProcessor', 'CorePreProcessor')
),
latest_audit_status AS (
    SELECT *
    FROM (
        SELECT
            acr.*,
            ROW_NUMBER() OVER (
                PARTITION BY TO_CHAR(acr.recon_id)
                ORDER BY NVL(acr.modified_ts, acr.created_ts) DESC
            ) AS rn
        FROM reconmgmt.audit_citi_recon acr
        JOIN app_recons ar ON TO_CHAR(acr.recon_id) = ar.recon_id
    )
    WHERE rn = 1
)
SELECT
    recon_id,
    rec_portal_id,
    recon_name,
    user_group,
    recon_status,
    created_by,
    created_ts,
    modified_by,
    modified_ts
FROM latest_audit_status
ORDER BY recon_status, recon_name;


/* =============================================================================
   11. EXPECTED FEED / FILE PATTERN METADATA FOR RECONS IN THIS APPLICATION
   -----------------------------------------------------------------------------
   Shows expected metadata/configured feed/file patterns for the recons submitted.
============================================================================= */

WITH app_recons AS (
    SELECT DISTINCT TO_CHAR(recon_id) AS recon_id
    FROM fasttrac.om_recon_audit_log
    WHERE run_id = '&APP_ID'
      AND comp_nm IN ('DataPreProcessor', 'CorePreProcessor')
)
SELECT
    mc.reconid,
    mc.feed_id,
    mc.filepattern,
    mc.src_system_name,
    mc.delimiter,
    mc.filetype,
    mc.created_time,
    mc.modified_time
FROM reconmgmt.v_om_one_recon_merger_config mc
JOIN app_recons ar ON TO_CHAR(mc.reconid) = ar.recon_id
ORDER BY mc.reconid, mc.feed_id, mc.filepattern;


/* =============================================================================
   12. EXPECTED METADATA COUNT VS ACTUAL FILE COUNT FOR THIS APPLICATION
   -----------------------------------------------------------------------------
   Compares configured expected feed/file patterns with actual file rows.
============================================================================= */

WITH app_actual AS (
    SELECT
        TO_CHAR(recon_id) AS recon_id,
        MAX(recon_name) AS recon_name,
        COUNT(*) AS actual_feed_file_rows,
        COUNT(DISTINCT file_name) AS actual_distinct_file_count,
        COUNT(DISTINCT file_type) AS actual_distinct_file_type_count,
        SUM(NVL(record_count, 0)) AS actual_total_record_count,
        MIN(created_time) AS first_file_time,
        MAX(created_time) AS last_file_time
    FROM fasttrac.om_recon_audit_log
    WHERE run_id = '&APP_ID'
      AND comp_nm IN ('DataPreProcessor', 'CorePreProcessor')
    GROUP BY TO_CHAR(recon_id)
),
expected_metadata AS (
    SELECT
        TO_CHAR(reconid) AS recon_id,
        COUNT(*) AS expected_file_pattern_count,
        COUNT(DISTINCT feed_id) AS expected_feed_count
    FROM reconmgmt.v_om_one_recon_merger_config
    WHERE TO_CHAR(reconid) IN (
        SELECT DISTINCT TO_CHAR(recon_id)
        FROM fasttrac.om_recon_audit_log
        WHERE run_id = '&APP_ID'
          AND comp_nm IN ('DataPreProcessor', 'CorePreProcessor')
    )
    GROUP BY TO_CHAR(reconid)
)
SELECT
    a.recon_id,
    a.recon_name,
    NVL(e.expected_feed_count, 0) AS expected_feed_count,
    NVL(e.expected_file_pattern_count, 0) AS expected_file_pattern_count,
    a.actual_feed_file_rows,
    a.actual_distinct_file_count,
    a.actual_distinct_file_type_count,
    a.actual_total_record_count,
    a.first_file_time,
    a.last_file_time,
    ROUND((CAST(a.last_file_time AS DATE) - CAST(a.first_file_time AS DATE)) * 24 * 60, 2) AS actual_recon_span_minutes
FROM app_actual a
LEFT JOIN expected_metadata e ON e.recon_id = a.recon_id
ORDER BY a.actual_feed_file_rows DESC, a.actual_total_record_count DESC;


/* =============================================================================
   13. FASTTRAC LEFT/RIGHT FEED METADATA FOR RECONS IN THIS APPLICATION
   -----------------------------------------------------------------------------
   Pulls left/right feed ids and filterfeed metadata for app recons.
============================================================================= */

WITH app_recons AS (
    SELECT DISTINCT TO_CHAR(recon_id) AS recon_id
    FROM fasttrac.om_recon_audit_log
    WHERE run_id = '&APP_ID'
      AND comp_nm IN ('DataPreProcessor', 'CorePreProcessor')
)
SELECT
    r.id AS recon_id,
    r.name AS recon_name,
    r.leftfeed_id,
    lf_left.userfeedid AS left_userfeedid,
    lf_left.filtercriteriaxml AS left_filtercriteriaxml,
    lf_left.aggregationinfo AS left_aggregationinfo,
    lf_left.aggregatefiltercriteriaxml AS left_aggregatefiltercriteriaxml,
    r.rightfeed_id,
    lf_right.userfeedid AS right_userfeedid,
    lf_right.filtercriteriaxml AS right_filtercriteriaxml,
    lf_right.aggregationinfo AS right_aggregationinfo,
    lf_right.aggregatefiltercriteriaxml AS right_aggregatefiltercriteriaxml
FROM fasttrac.reconciliation r
JOIN app_recons ar ON TO_CHAR(r.id) = ar.recon_id
LEFT JOIN fasttrac.filterfeed lf_left ON lf_left.id = r.leftfeed_id
LEFT JOIN fasttrac.filterfeed lf_right ON lf_right.id = r.rightfeed_id
ORDER BY r.id;


/* =============================================================================
   14. GENERATED FEED / TRANSFORMATION XML FOR RECONS IN THIS APPLICATION
   -----------------------------------------------------------------------------
   Pulls generated feed transformation XML for left/right feeds.
============================================================================= */

WITH app_recons AS (
    SELECT DISTINCT TO_CHAR(recon_id) AS recon_id
    FROM fasttrac.om_recon_audit_log
    WHERE run_id = '&APP_ID'
      AND comp_nm IN ('DataPreProcessor', 'CorePreProcessor')
)
SELECT
    r.id AS recon_id,
    r.name AS recon_name,
    r.leftfeed_id,
    gf_left.feed_id AS left_generated_feed_id,
    gf_left.xmldata AS left_transformation_xml,
    r.rightfeed_id,
    gf_right.feed_id AS right_generated_feed_id,
    gf_right.xmldata AS right_transformation_xml
FROM fasttrac.reconciliation r
JOIN app_recons ar ON TO_CHAR(r.id) = ar.recon_id
LEFT JOIN fasttrac.generatedfeed gf_left ON gf_left.feed_id = r.leftfeed_id
LEFT JOIN fasttrac.generatedfeed gf_right ON gf_right.feed_id = r.rightfeed_id
ORDER BY r.id;


/* =============================================================================
   15. COMPONENT TIMELINE FOR SAME RECONS AROUND APPLICATION WINDOW
   -----------------------------------------------------------------------------
   For app recons, shows related component events within START_TS/END_TS.
============================================================================= */

WITH app_recons AS (
    SELECT DISTINCT TO_CHAR(recon_id) AS recon_id
    FROM fasttrac.om_recon_audit_log
    WHERE run_id = '&APP_ID'
      AND comp_nm IN ('DataPreProcessor', 'CorePreProcessor')
)
SELECT
    a.recon_id,
    a.recon_name,
    a.cob_date,
    a.comp_nm,
    a.status,
    a.run_id,
    a.file_name,
    a.file_type,
    a.record_count,
    a.created_time,
    a.message
FROM fasttrac.om_recon_audit_log a
JOIN app_recons ar ON TO_CHAR(a.recon_id) = ar.recon_id
WHERE a.created_time >= TO_TIMESTAMP('&START_TS', 'YYYY-MM-DD HH24:MI:SS')
  AND a.created_time <  TO_TIMESTAMP('&END_TS', 'YYYY-MM-DD HH24:MI:SS')
  AND a.comp_nm IN (
        'DataConnector',
        'DataMerger',
        'DataPreProcessor',
        'CorePreProcessor',
        'ReconSchedule',
        'ReconScheduler',
        'ReconLauncher',
        'ReconEngine'
  )
ORDER BY a.recon_id, a.created_time, a.comp_nm;


/* =============================================================================
   16. DATA MERGER TO DPP TIMING FOR RECONS IN THIS APPLICATION WINDOW
   -----------------------------------------------------------------------------
   Compares DataMerger and DataPreProcessor/CorePreProcessor timing.
============================================================================= */

WITH app_recons AS (
    SELECT DISTINCT TO_CHAR(recon_id) AS recon_id
    FROM fasttrac.om_recon_audit_log
    WHERE run_id = '&APP_ID'
      AND comp_nm IN ('DataPreProcessor', 'CorePreProcessor')
),
component_times AS (
    SELECT
        TO_CHAR(a.recon_id) AS recon_id,
        MAX(a.recon_name) AS recon_name,
        MIN(CASE
            WHEN a.comp_nm = 'DataMerger'
             AND UPPER(a.status) IN ('SUCCESS', 'FINISHED', 'COMPLETED')
            THEN a.created_time
        END) AS datamerger_success_time,
        MIN(CASE
            WHEN a.comp_nm IN ('DataPreProcessor', 'CorePreProcessor')
             AND a.run_id = '&APP_ID'
            THEN a.created_time
        END) AS dpp_first_file_time,
        MAX(CASE
            WHEN a.comp_nm IN ('DataPreProcessor', 'CorePreProcessor')
             AND a.run_id = '&APP_ID'
            THEN a.created_time
        END) AS dpp_last_file_time,
        COUNT(CASE
            WHEN a.comp_nm IN ('DataPreProcessor', 'CorePreProcessor')
             AND a.run_id = '&APP_ID'
            THEN 1
        END) AS dpp_file_rows
    FROM fasttrac.om_recon_audit_log a
    JOIN app_recons ar ON TO_CHAR(a.recon_id) = ar.recon_id
    WHERE a.created_time >= TO_TIMESTAMP('&START_TS', 'YYYY-MM-DD HH24:MI:SS')
      AND a.created_time <  TO_TIMESTAMP('&END_TS', 'YYYY-MM-DD HH24:MI:SS')
    GROUP BY TO_CHAR(a.recon_id)
)
SELECT
    recon_id,
    recon_name,
    datamerger_success_time,
    dpp_first_file_time,
    dpp_last_file_time,
    dpp_file_rows,
    ROUND((CAST(dpp_first_file_time AS DATE) - CAST(datamerger_success_time AS DATE)) * 24 * 60, 2) AS datamerger_to_dpp_first_file_minutes,
    ROUND((CAST(dpp_last_file_time AS DATE) - CAST(dpp_first_file_time AS DATE)) * 24 * 60, 2) AS dpp_span_minutes
FROM component_times
ORDER BY datamerger_to_dpp_first_file_minutes DESC NULLS LAST, dpp_span_minutes DESC NULLS LAST;


/* =============================================================================
   17. MANAGER-READY SUMMARY FOR APPLICATION_ID
   -----------------------------------------------------------------------------
   One clean summary row per recon, enriched with recon status and expected metadata.
============================================================================= */

WITH app_actual AS (
    SELECT
        TO_CHAR(recon_id) AS recon_id,
        MAX(recon_name) AS recon_name,
        cob_date,
        run_id,
        COUNT(*) AS actual_feed_file_rows,
        COUNT(DISTINCT file_name) AS actual_distinct_file_count,
        SUM(NVL(record_count, 0)) AS actual_total_record_count,
        MIN(created_time) AS first_file_time,
        MAX(created_time) AS last_file_time,
        ROUND((CAST(MAX(created_time) AS DATE) - CAST(MIN(created_time) AS DATE)) * 24 * 60, 2) AS dpp_span_minutes,
        SUM(CASE WHEN UPPER(status) = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
        SUM(CASE WHEN UPPER(status) = 'FAILED' THEN 1 ELSE 0 END) AS failed_count
    FROM fasttrac.om_recon_audit_log
    WHERE run_id = '&APP_ID'
      AND comp_nm IN ('DataPreProcessor', 'CorePreProcessor')
    GROUP BY TO_CHAR(recon_id), cob_date, run_id
),
expected_metadata AS (
    SELECT
        TO_CHAR(reconid) AS recon_id,
        COUNT(*) AS expected_file_pattern_count,
        COUNT(DISTINCT feed_id) AS expected_feed_count
    FROM reconmgmt.v_om_one_recon_merger_config
    WHERE TO_CHAR(reconid) IN (
        SELECT DISTINCT TO_CHAR(recon_id)
        FROM fasttrac.om_recon_audit_log
        WHERE run_id = '&APP_ID'
          AND comp_nm IN ('DataPreProcessor', 'CorePreProcessor')
    )
    GROUP BY TO_CHAR(reconid)
)
SELECT
    a.run_id,
    a.recon_id,
    a.recon_name,
    cr.rec_portal_id,
    cr.user_group,
    cr.recon_status,
    a.cob_date,
    NVL(e.expected_feed_count, 0) AS expected_feed_count,
    NVL(e.expected_file_pattern_count, 0) AS expected_file_pattern_count,
    a.actual_feed_file_rows,
    a.actual_distinct_file_count,
    a.actual_total_record_count,
    a.first_file_time,
    a.last_file_time,
    a.dpp_span_minutes,
    a.success_count,
    a.failed_count,
    CASE
        WHEN a.failed_count > 0 THEN 'HAS_FAILURE'
        WHEN a.success_count = a.actual_feed_file_rows THEN 'ALL_SUCCESS'
        ELSE 'CHECK_STATUS'
    END AS app_recon_result
FROM app_actual a
LEFT JOIN reconmgmt.citi_recon cr ON TO_CHAR(cr.recon_id) = a.recon_id
LEFT JOIN expected_metadata e ON e.recon_id = a.recon_id
ORDER BY a.actual_feed_file_rows DESC, a.dpp_span_minutes DESC, a.recon_id;

/* =============================================================================
   END OF SCRIPT
============================================================================= */
