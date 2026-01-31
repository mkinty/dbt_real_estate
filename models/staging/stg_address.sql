WITH address_raw AS (
    SELECT
        raw_payload:project_identifier::INT     AS project_identifier,
        raw_payload:entity_identifier::INT      AS entity_identifier,

        raw_payload:address:line1::string         AS address_line1,
        raw_payload:address:line2::string           AS address_line2,
        raw_payload:address:city::string    AS address_city,
        raw_payload:address:country::string        AS address_country,
        raw_payload:event_timestamp::timestamp_ntz AS event_timestamp
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

FROM address_raw