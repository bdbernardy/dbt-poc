{{ config(
    materialized='incremental',
    unique_key=['id', 'atomic_location_type'],
    indexes=[
      {'columns': ['atomic_location_type, updated_at']},
      {'columns': ['id', 'atomic_location_type'], 'unique': True},
    ]
) }}

{%- if is_incremental() -%}
  WITH last_updated AS (
    SELECT atomic_location_type, MAX(updated_at) AS updated_at
    FROM {{ this }}
    GROUP BY 1
  )
{%- endif %}
SELECT 
	poi.id,
	poi.name,
	'point-of-interest' AS atomic_location_type,
  poi.updated_at
FROM {{ source('public', 'points_of_interest') }} poi
{% if is_incremental() -%}
  LEFT JOIN last_updated u ON u.atomic_location_type = 'point-of-interest'
  WHERE u.updated_at IS NULL OR poi.updated_at > u.updated_at
{%- endif %}

UNION ALL

SELECT 
	c.id::text,
	c.name,
	'city' AS atomic_location_type,
  c.updated_at
FROM {{ source('public', 'cities') }} c
{% if is_incremental() -%}
  LEFT JOIN last_updated u ON u.atomic_location_type = 'city'
  WHERE u.updated_at IS NULL OR c.updated_at > u.updated_at
{%- endif %}

UNION ALL

SELECT 
	l.id::text,
	l.name,
	'location' AS atomic_location_type,
  l.updated_at
FROM {{ source('public', 'locations') }} l
{% if is_incremental() -%}
  LEFT JOIN last_updated u ON u.atomic_location_type = 'location'
  WHERE u.updated_at IS NULL OR l.updated_at > u.updated_at
{%- endif %}