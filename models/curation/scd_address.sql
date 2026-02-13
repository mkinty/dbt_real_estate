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
        event_timestamp AS valid_from,
        LEAD(event_timestamp) OVER (
            PARTITION BY project_identifier, entity_identifier
            ORDER BY event_timestamp
        ) AS valid_to
    FROM address_with_prev_hash
    WHERE prev_row_hash IS NULL OR prev_row_hash <> row_hash
)

SELECT
    *
FROM scd2_ready

