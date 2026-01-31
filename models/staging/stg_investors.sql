-- Aplatir tous les événements investisseurs
WITH flattened_events AS (
    SELECT
        raw_payload:project_identifier::INT AS project_identifier,
        raw_payload:entity_identifier::INT  AS entity_identifier,

        inv.value:identifier::INT     AS investor_identifier,
        inv.value:name::STRING        AS investor_name,
        inv.value:invested::FLOAT     AS invested,

        raw_payload:event_timestamp::TIMESTAMP AS event_timestamp,

        -- Flag is_active
        CASE 
            WHEN inv.value:invested::FLOAT > 0 THEN TRUE
            ELSE FALSE
        END AS is_active

    FROM {{ source('raw_real_estate_data', 'raw_events') }},
         LATERAL FLATTEN(
             input => raw_payload:investors, 
             outer => true
         ) inv
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

FROM flattened_events