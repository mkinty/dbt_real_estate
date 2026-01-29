WITH address_raw AS (

    SELECT
        raw_payload:project_identifier::INT     AS project_identifier,
        raw_payload:entity_identifier::INT      AS entity_identifier,

        raw_payload:address.line1::STRING       AS address_line1,
        raw_payload:address.line2::STRING       AS address_line2,
        raw_payload:address.city::STRING        AS address_city,
        raw_payload:address.country::STRING     AS address_country,

        raw_payload:event_timestamp::TIMESTAMP  AS event_timestamp,

    FROM {{ source('raw_real_estate_data', 'raw_events') }}
    WHERE raw_payload:address IS NOT NULL
)

SELECT
    -- Génération d'un address_id unique de type INT (unique_key basé sur project + entity + hash des champs de l'adresse)
    TO_NUMBER(
        ABS(HASH(
            CONCAT(
                project_identifier::STRING,
                entity_identifier::STRING,
                address_line1,
                address_line2,
                address_city,
                address_country
            )
        ))
    ) AS address_id,
    *

FROM address_raw
