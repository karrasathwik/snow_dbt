{{
    config(
        materialized='table'
    )
}}

with  adress as(
    select distinct AREA,
    LOCAL_ADDRESS,
    from {{ ref('stg_clean') }}
),

adew as(
    select *, {{ dbt_utils.generate_surrogate_key(
            ['AREA', 'LOCAL_ADDRESS']
        ) }} as LOAL_ID
        from adress
)
select * FROM adew
