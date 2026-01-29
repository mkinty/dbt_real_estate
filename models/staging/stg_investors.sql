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
),

-- Identifier l’événement précédent
events_with_prev AS (
    SELECT
        *,
        LAG(event_timestamp) OVER (
            PARTITION BY project_identifier, entity_identifier
            ORDER BY event_timestamp
        ) AS prev_event_timestamp
    FROM flattened_events
),

-- Investisseurs présents au précédent événement
prev_event_investors AS (
    SELECT
        project_identifier,
        entity_identifier,
        event_timestamp AS prev_event_timestamp,
        investor_identifier
    FROM flattened_events
),

-- Détecter les sortants
exiting_investors AS (
    SELECT
        p.project_identifier,
        p.entity_identifier,
        e.event_timestamp,
        p.investor_identifier,

        0::FLOAT AS invested,
        FALSE AS is_active

    FROM prev_event_investors p
    JOIN events_with_prev e
      ON p.project_identifier = e.project_identifier
     AND p.entity_identifier  = e.entity_identifier
     AND p.prev_event_timestamp = e.prev_event_timestamp

    LEFT JOIN flattened_events c
      ON p.project_identifier = c.project_identifier
     AND p.entity_identifier  = c.entity_identifier
     AND p.investor_identifier = c.investor_identifier
     AND p.prev_event_timestamp = c.event_timestamp

    WHERE c.investor_identifier IS NULL
)

-- Union finale
SELECT
    investor_identifier,
    investor_name,
    invested,
    is_active,
    project_identifier,
    entity_identifier,
    event_timestamp
FROM flattened_events

UNION ALL

SELECT
    investor_identifier,
    NULL AS investor_name,
    invested,
    is_active,
    project_identifier,
    entity_identifier,
    event_timestamp
FROM exiting_investors
