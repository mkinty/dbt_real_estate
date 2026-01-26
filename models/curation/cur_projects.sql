{{
    config(
        schema=var("curation_schema", "curation")
    )
}}

WITH projects_raw AS (

    SELECT
        project_identifier,
        entity_identifier,
        project_name,
        project_start,
        is_active,
        event_timestamp,
        phase

    FROM {{ ref('snap_projects') }}
    WHERE DBT_VALID_TO IS NULL
)

SELECT
    *
FROM projects_raw