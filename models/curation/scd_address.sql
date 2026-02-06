{{
    config(
        schema=var('cur_schema'),
        materialized=var('cur_materialized')
    )
}}

WITH address_hashed AS (
    SELECT
        *,
        HASH(address_line1, address_line2, address_city, address_country) AS row_hash
    FROM {{ ref('stg_address') }}
),

address_with_prev_hash AS (
    SELECT
        *,
        LAG(row_hash) OVER (
            PARTITION BY project_identifier, entity_identifier
            ORDER BY event_timestamp
        ) AS prev_row_hash

    FROM address_hashed
),

scd2_ready AS (
    SELECT
        *,
        CASE
            WHEN prev_row_hash = row_hash THEN FALSE
            ELSE TRUE
        END AS is_changed
    FROM address_with_prev_hash
)

SELECT
    *,
    event_timestamp AS valid_from,
    LEAD(event_timestamp) OVER (
        PARTITION BY project_identifier, entity_identifier
        ORDER BY event_timestamp
    ) AS valid_to
FROM scd2_ready
WHERE is_changed = TRUE

