WITH investors_raw AS (

    SELECT
        inv.value:identifier::INT     AS investor_identifier,
        inv.value:name::STRING        AS investor_name,
        inv.value:invested::FLOAT     AS invested,

        raw_payload:project_identifier::INT AS project_identifier,
        raw_payload:entity_identifier::INT  AS entity_identifier

    FROM realestate.raw.raw_events,
    -- investors est un ARRAY d’objets, En Snowflake, il faut exploser le tableau avec LATERAL FLATTEN
         LATERAL FLATTEN(
             input => raw_payload:investors, 
             outer => true -- Gère les cas sans investors (optionnel)
         ) inv
)

SELECT *
FROM investors_raw
