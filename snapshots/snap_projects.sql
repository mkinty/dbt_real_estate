{% snapshot snap_projects %}

{{
    config(
      target_database='realestate',
      target_schema='snapshots',

      strategy='timestamp',
      updated_at='event_timestamp',

      unique_key=dbt_utils.generate_surrogate_key([
        'project_identifier',
        'entity_identifier'
      ])
    )
}}

SELECT
    project_identifier,
    entity_identifier,
    project_name,
    project_start,
    is_active,
    phase,
    event_timestamp
FROM {{ ref('stg_projects') }}

{% endsnapshot %}
