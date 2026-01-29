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
    *
FROM {{ ref('stg_projects_current') }}

{% endsnapshot %}
