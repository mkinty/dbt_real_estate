WITH projects_raw AS (

    SELECT
        raw_payload:project_identifier::INT     AS project_identifier,
        raw_payload:entity_identifier::INT      AS entity_identifier,

        raw_payload:name::STRING                AS project_name,
        raw_payload:project_start::DATE         AS project_start,
        raw_payload:is_active::BOOLEAN          AS is_active,
        raw_payload:event_timestamp::TIMESTAMP  AS event_timestamp,
        raw_payload:phase::STRING               AS phase

    FROM {{ source('raw_real_estate_data', 'raw_events') }}
)
SELECT
    *,
    
    event_timestamp AS valid_from,
    LEAD(event_timestamp) OVER (
        PARTITION BY project_identifier, entity_identifier
        ORDER BY event_timestamp
    ) AS valid_to,

    CASE
        WHEN LEAD(event_timestamp) OVER (
            PARTITION BY project_identifier, entity_identifier
            ORDER BY event_timestamp
        ) IS NULL THEN TRUE
        ELSE FALSE
    END AS is_current
    
FROM projects_raw