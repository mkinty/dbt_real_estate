{{
    config(
        schema=var("curation_schema", "curation")
    )
}}

WITH investors_raw AS (

    SELECT
        investor_identifier,
        investor_name,
        invested,
        is_active,
        project_identifier,
        entity_identifier,
        event_timestamp

    FROM {{ ref('snap_investors') }}
    WHERE DBT_VALID_TO IS NULL
    AND is_active = TRUE
        
)

SELECT
    *
FROM investors_raw

