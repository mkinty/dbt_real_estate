WITH investors_raw AS (

    SELECT
        inv.value:identifier::INT     AS investor_identifier,
        inv.value:name::STRING        AS investor_name,
        inv.value:invested::FLOAT     AS invested,

        raw_payload:project_identifier::INT AS project_identifier,
        raw_payload:entity_identifier::INT  AS entity_identifier,
        raw_payload:event_timestamp::TIMESTAMP AS event_timestamp,

        ROW_NUMBER() OVER (
            PARTITION BY
                inv.value:identifier::INT,
                raw_payload:project_identifier::INT,
                raw_payload:entity_identifier::INT
            ORDER BY raw_payload:event_timestamp::TIMESTAMP DESC
        ) AS rn

    FROM {{ source('raw_real_estate_data', 'raw_events') }},
        -- investors est un array d’objets, donc il faut exploser le tableau avec LATERAL FLATTEN
         LATERAL FLATTEN(
             input => raw_payload:investors, 
             outer => true -- gérer les cas sans investors (optionnel)
         ) inv
)

SELECT
    investor_identifier,
    investor_name,
    invested,
    project_identifier,
    entity_identifier,
    event_timestamp
FROM investors_raw
WHERE rn = 1

