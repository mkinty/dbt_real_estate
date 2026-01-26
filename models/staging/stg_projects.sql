WITH projects_raw AS (

    SELECT
        raw_payload:project_identifier::INT     AS project_identifier,
        raw_payload:entity_identifier::INT      AS entity_identifier,

        raw_payload:name::STRING                AS project_name,
        raw_payload:project_start::DATE         AS project_start,
        raw_payload:is_active::BOOLEAN          AS is_active,
        raw_payload:event_timestamp::TIMESTAMP  AS event_timestamp,
        raw_payload:phase::STRING               AS phase,

        ROW_NUMBER() OVER (
            PARTITION BY
                raw_payload:project_identifier::INT,
                raw_payload:entity_identifier::INT
            ORDER BY raw_payload:event_timestamp::TIMESTAMP DESC
        ) AS rn

    FROM {{ source('raw_real_estate_data', 'raw_events') }}
)

SELECT
    project_identifier,
    entity_identifier,
    project_name,
    project_start,
    is_active,
    phase,
    event_timestamp,

    -- Hash des colonnes importantes pour SCD2
    {{ dbt_utils.generate_surrogate_key([
        'project_name',
        'project_start',
        'is_active',
        'phase'
    ]) }} AS hash_value
    
FROM projects_raw
WHERE rn = 1