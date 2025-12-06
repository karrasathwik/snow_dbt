{{ 
    config(
        materialized='table'
    ) 
}}

with rest as (
    select  distinct
        REST_NAME,
        RESTAURANT_TYPE,
    from {{ ref('stg_clean') }} 
),

surrogate as (
    select
        *,
        {{ dbt_utils.generate_surrogate_key(
            ['REST_NAME', 'RESTAURANT_TYPE']
        ) }} as REST_SK
    from rest
)

select *
from surrogate
