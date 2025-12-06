{{ 
  config(
    materialized='table'
  ) 
}}

with source_data as (

    select
        AREA,
        LOCAL_ADDRESS
    from {{ ref('stg_clean') }}

),

surrogate_keyed as (

    select
        *,
        {{ dbt_utils.generate_surrogate_key([
                'AREA',
                'LOCAL_ADDRESS'
            ]
        ) }} as loal_id
    from source_data

)

select *
from surrogate_keyed
