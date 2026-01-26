{% snapshot snap_address %}

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
    address_id,

    project_identifier,
    entity_identifier,

    address_line1,
    address_line2,
    address_city,
    address_country,

    event_timestamp

FROM {{ ref('stg_address') }}

{% endsnapshot %}
