{{
    config(
        schema=var("curation_schema", "curation")
    )
}}

WITH address_raw AS (

    SELECT
        address_id,
        project_identifier,
        entity_identifier,
        address_line1,
        address_line2,
        address_city,
        address_country,
        event_timestamp

    FROM {{ ref('snap_address') }}
    WHERE DBT_VALID_TO IS NULL
)

SELECT
    *
FROM address_raw
