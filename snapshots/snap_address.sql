{% snapshot snap_address %}

{{
    config(
      target_database='realestate',
      target_schema='snapshots',
      strategy='timestamp',
      updated_at='event_timestamp',
      unique_key='address_id' 
    )
}}

SELECT
    *
FROM {{ ref('stg_address_current') }}

{% endsnapshot %}
