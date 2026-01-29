WITH projects_deduplicated AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                project_identifier,
                entity_identifier
            ORDER BY event_timestamp DESC
        ) AS rn

    FROM {{ ref('stg_projects') }}
)

SELECT
    *
FROM projects_deduplicated
WHERE rn = 1