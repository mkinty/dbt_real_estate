-- Aplatir tous les événements investisseurs
WITH flattened_events AS (
    SELECT
        raw_payload:project_identifier::INT AS project_identifier,
        raw_payload:entity_identifier::INT  AS entity_identifier,

        inv.value:identifier::INT     AS investor_identifier,
        inv.value:name::STRING        AS investor_name,
        inv.value:invested::FLOAT     AS invested,

        raw_payload:event_timestamp::TIMESTAMP AS event_timestamp

    FROM {{ source('raw_real_estate_data', 'raw_events') }},
         LATERAL FLATTEN(
             input => raw_payload:investors, 
             outer => false
         ) inv
)
SELECT
    *
FROM flattened_events