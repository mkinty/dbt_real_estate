WITH investors_raw AS (

    SELECT
        inv.value:identifier::INT     AS investor_identifier,
        inv.value:name::STRING        AS investor_name,
        inv.value:invested::FLOAT     AS invested,

        raw_payload:project_identifier::INT AS project_identifier,
        raw_payload:entity_identifier::INT  AS entity_identifier,
        raw_payload:event_timestamp::TIMESTAMP AS event_timestamp,

        -- Flag is_active
        CASE 
            WHEN inv.value:invested::FLOAT > 0 THEN TRUE
            ELSE FALSE
        END AS is_active,

        ROW_NUMBER() OVER (
            PARTITION BY
                inv.value:identifier::INT,
                raw_payload:project_identifier::INT,
                raw_payload:entity_identifier::INT
            ORDER BY raw_payload:event_timestamp::TIMESTAMP DESC
        ) AS rn

    FROM {{ source('raw_real_estate_data', 'raw_events') }},
         LATERAL FLATTEN(
             input => raw_payload:investors, 
             outer => true
         ) inv
)

SELECT
    investor_identifier,
    investor_name,
    invested,
    is_active,
    project_identifier,
    entity_identifier,
    event_timestamp,

    -- Hash des colonnes importantes pour SCD2
    {{ dbt_utils.generate_surrogate_key([
        'investor_name',
        'invested',
        'is_active'
    ]) }} AS hash_value

FROM investors_raw
WHERE rn = 1
