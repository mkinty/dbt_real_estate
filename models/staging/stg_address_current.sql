WITH address_deduplicated AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                address_id
            ORDER BY event_timestamp DESC
        ) AS rn

    FROM {{ ref('stg_address') }}
)

SELECT
    *
FROM address_deduplicated
WHERE rn = 1