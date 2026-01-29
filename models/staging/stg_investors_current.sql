WITH investors_deduplicated AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                investor_identifier,
                project_identifier,
                entity_identifier
            ORDER BY event_timestamp DESC
        ) AS rn

    FROM {{ ref('stg_investors') }}
)

SELECT
    *
FROM investors_deduplicated
WHERE rn = 1