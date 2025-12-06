{{ 
    config(
        materialized='table'
    ) 
}}

with rest as (
    select 
        REST_NAME,
        RESTAURANT_TYPE,
    from {{ ref('stg_clean') }}
),

surrogate as (
    select
        *,
        {{ dbt_utils.generate_surrogate_key(
            ['REST_NAME', 'RESTAURANT_TYPE']
        ) }} as rest_sk
    from rest
)

select *
from surrogate
