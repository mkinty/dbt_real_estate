{% snapshot investors_snapshot %}

{{
    config(
        target_database='realestate',
        target_schema='snapshots',

        strategy='timestamp',
        updated_at='event_timestamp',

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
    project_identifier,
    entity_identifier,
    event_timestamp

FROM {{ ref('stg_investors') }}

{% endsnapshot %}
