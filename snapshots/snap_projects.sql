{% snapshot snap_projects %}

{{
    config(
      target_database='realestate',
      target_schema='snapshots',
      strategy='check',
      check_cols=['hash_value'],
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
    event_timestamp,
    hash_value

FROM {{ ref('stg_projects') }}

{% endsnapshot %}
