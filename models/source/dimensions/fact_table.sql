{{
    config(
        materialized='incremental',
        unique_key='fact_sk',
        cluster_by=['REST_ID', 'REST_SK', 'ONLINE_ID']
    )
}}

with fact_table as (
    select 
        REST_ID,
        REST_NAME,
        RESTAURANT_TYPE,
        RATING,
        TOTAL_RATINGS,
        COST_FOR_TWO,
        AREA,
        LOCAL_ADDRESS,
        ONLINE_ORDERS,
        TABLE_BOOKING
    from {{ ref('stg_clean') }}
),

address as (
    select distinct 
        AREA, 
        LOCAL_ADDRESS, 
        LOAL_ID
    from {{ ref('dim_address') }}
),

rest as (
    select distinct 
        REST_NAME, 
        RESTAURANT_TYPE, 
        REST_SK
    from {{ ref('dim_rest') }}
),

onlines as (
    select distinct 
        ONLINE_ORDERS, 
        TABLE_BOOKING, 
        ONLINE_ID
    from {{ ref('dimonline') }}
),

fact as (
    select distinct
        f.REST_ID,
        f.RATING,
        f.TOTAL_RATINGS,
        f.COST_FOR_TWO,
        a.LOAL_ID,
        r.REST_SK,
        o.ONLINE_ID
    from fact_table f
    left join address a
        on f.AREA = a.AREA 
       and f.LOCAL_ADDRESS = a.LOCAL_ADDRESS
    left join rest r
        on f.REST_NAME = r.REST_NAME
       and f.RESTAURANT_TYPE = r.RESTAURANT_TYPE
    left join onlines o
        on f.ONLINE_ORDERS = o.ONLINE_ORDERS
)

select
    {{ dbt_utils.generate_surrogate_key(['REST_ID','REST_SK','ONLINE_ID']) }} as fact_sk,
    REST_ID,
    RATING,
    TOTAL_RATINGS,
    COST_FOR_TWO,
    LOAL_ID,
    REST_SK,
    ONLINE_ID
from fact
