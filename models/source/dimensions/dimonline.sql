{{
    config(
        materialized='table'
    )
}}


with new_online as (
    select ONLINE_ORDERS,TABLE_BOOKING from {{ ref('stg_clean') }}
),

online_new as (
    select *, {{
        dbt_utils.generate_surrogate_key(
            ['ONLINE_ORDERS','TABLE_BOOKING'])
    }} as online_id from new_online
) 

select * from online_new