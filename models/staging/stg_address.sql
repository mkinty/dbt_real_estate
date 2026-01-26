WITH address_raw AS (

    SELECT
        raw_payload:address.line1::STRING       AS address_line1,
        raw_payload:address.line2::STRING       AS address_line2,
        raw_payload:address.city::STRING        AS address_city,
        raw_payload:address.country::STRING     AS address_country,

        raw_payload:project_identifier::INT     AS project_identifier,
        raw_payload:entity_identifier::INT      AS entity_identifier,
        raw_payload:event_timestamp::TIMESTAMP  AS event_timestamp,

        ROW_NUMBER() OVER (
            PARTITION BY
                raw_payload:project_identifier::INT,
                raw_payload:entity_identifier::INT
            ORDER BY raw_payload:event_timestamp::TIMESTAMP DESC
        ) AS rn

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
    
    project_identifier,
    entity_identifier,

    address_line1,
    address_line2,
    address_city,
    address_country,
    event_timestamp,

    -- Hash des colonnes importantes pour SCD2
    {{ dbt_utils.generate_surrogate_key([
        'address_line1',
        'address_line2',
        'address_city',
        'address_country'
    ]) }} AS hash_value

FROM address_raw
WHERE rn = 1
