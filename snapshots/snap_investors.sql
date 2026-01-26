{% snapshot snap_investors %}

{{
    config(
        target_database='realestate',
        target_schema='snapshots',
        strategy='check',
        check_cols=['hash_value'],
        unique_key=dbt_utils.generate_surrogate_key([
            'investor_identifier',
            'project_identifier',
            'entity_identifier'
        ])
    )
}}

SELECT
    investor_identifier,
    investor_name,
    invested,
    is_active,
    project_identifier,
    entity_identifier,
    event_timestamp,
    hash_value
    
FROM {{ ref("stg_investors") }}

{% endsnapshot %}
