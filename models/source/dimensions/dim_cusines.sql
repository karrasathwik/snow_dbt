{{
    config(
        materialized='table'
    )
}}

with base as (
    select 
       distinct( trim(f.value)) as cuisine_name
    from {{ ref('stg_clean') }} sc,
    lateral flatten(input => split(sc.CUISINES_TYPE, ',')) f
),

new_cusines as (
    select
        *,
        {{ dbt_utils.generate_surrogate_key(['cuisine_name']) }} as cuisine_sk
    from base
)

select * from new_cusines
