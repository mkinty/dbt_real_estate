{{
    config(
        schema=var('cur_schema'),
        materialized=var('cur_materialized')
    )
}}

WITH investor_hashed AS (
    SELECT
        *,
        HASH(investor_name, invested) AS row_hash
    FROM {{ ref('stg_investors') }}
),

with_next_ts AS (

    SELECT
        *,
        LEAD(event_timestamp) OVER (
            PARTITION BY project_identifier, entity_identifier
            ORDER BY event_timestamp
        ) AS next_event_timestamp
    FROM investor_hashed
),

-- Détection suppressions
deleted_investors AS (

    SELECT
        cur.project_identifier,
        cur.entity_identifier,
        cur.investor_identifier,
        cur.investor_name,
        cur.invested,
        next_event_timestamp AS deletion_timestamp

    FROM with_next_ts cur

    LEFT JOIN investor_hashed nxt
      ON cur.project_identifier = nxt.project_identifier
     AND cur.entity_identifier  = nxt.entity_identifier
     AND cur.investor_identifier = nxt.investor_identifier
     AND cur.next_event_timestamp = nxt.event_timestamp

    WHERE nxt.investor_identifier IS NULL
      AND cur.next_event_timestamp IS NOT NULL
),

-- Détection changements
investor_with_prev_hash AS (

    SELECT
        *,
        LAG(row_hash) OVER (
            PARTITION BY project_identifier, entity_identifier, investor_identifier
            ORDER BY event_timestamp
        ) AS prev_row_hash

    FROM investor_hashed
),

scd2_changes AS (

    SELECT
        project_identifier,
        entity_identifier,
        investor_identifier,
        investor_name,
        invested,

        event_timestamp AS valid_from,

        LEAD(event_timestamp) OVER (
            PARTITION BY project_identifier, entity_identifier, investor_identifier
            ORDER BY event_timestamp
        ) AS valid_to,

        FALSE AS is_deleted

    FROM investor_with_prev_hash
    WHERE prev_row_hash IS NULL OR prev_row_hash <> row_hash
),

-- Fermeture des suppressions
scd2_deletions AS (

    SELECT
        project_identifier,
        entity_identifier,
        investor_identifier,
        investor_name,
        invested,

        deletion_timestamp AS valid_from,
        deletion_timestamp AS valid_to,

        TRUE AS is_deleted

    FROM deleted_investors
)

-- UNION final aligné
SELECT * FROM scd2_changes

UNION ALL

SELECT * FROM scd2_deletions
