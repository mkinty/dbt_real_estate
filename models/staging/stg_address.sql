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
    WHERE raw_payload:address IS NOT NULL

)
SELECT
    *
FROM address_raw