-- Creation d'un vue address
WITH address_raw AS (

    SELECT
        raw_payload:address.line1::STRING       AS line1,
        raw_payload:address.line2::STRING       AS line2,
        raw_payload:address.city::STRING        AS city,
        raw_payload:address.country::STRING     AS country,

        raw_payload:project_identifier::INT     AS project_identifier,
        raw_payload:entity_identifier::INT      AS entity_identifier

    FROM {{ source("raw_real_estate_data", "raw_events") }}
)

SELECT *
FROM address_raw