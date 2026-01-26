{% snapshot snap_address %}

{{
    config(
      target_database='realestate',
      target_schema='snapshots',
      strategy='check',
      check_cols=['hash_value'],
      unique_key='address_id' 
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
    event_timestamp,
    hash_value
    
FROM {{ ref('stg_address') }}

{% endsnapshot %}
