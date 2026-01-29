{% snapshot snap_investors %}

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
    *
FROM {{ ref('stg_investors_current') }}

{% endsnapshot %}
