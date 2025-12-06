{{
    config(
        materialized = 'incremental',
        unique_key = 'fact_sk',
        cluster_by = ['REST_ID', 'CUISINE_SK', 'REST_SK', 'ONLINE_ID']
    )
}}

with fact_table as (
    select 
        sc.REST_ID,
        sc.REST_NAME,
        sc.RESTAURANT_TYPE,
        sc.RATING,
        sc.TOTAL_RATINGS,
        sc.COST_FOR_TWO,
        sc.AREA,
        sc.LOCAL_ADDRESS,
        sc.ONLINE_ORDERS,
        sc.TABLE_BOOKING,
        trim(f.value) as cuisine_name
        from {{ ref('stg_clean') }} sc,
        lateral flatten(input => split(sc.CUISINES_TYPE, ',')) f
),
addres as (
    select AREA, LOCAL_ADDRESS, LOAL_ID
    from {{ ref('dim_address') }}
),

cusines as (
    select cuisine_name, CUISINE_SK
    from {{ ref('dim_cusines') }}
),

rest as (
    select REST_NAME, RESTAURANT_TYPE, REST_SK
    from {{ ref('dim_rest') }}
),

onlines as (
    select ONLINE_ORDERS, TABLE_BOOKING, ONLINE_ID
    from {{ ref('dimonline') }}
),

fact as (
    select 
        {{ dbt_utils.generate_surrogate_key([
            'f.REST_ID',
            'c.CUISINE_SK',
            'r.REST_SK',
            'o.ONLINE_ID'
        ]) }} as fact_sk,

        f.REST_ID,
        f.RATING,
        f.TOTAL_RATINGS,
        f.COST_FOR_TWO,
        a.LOAL_ID,
        c.CUISINE_SK,
        r.REST_SK,
        o.ONLINE_ID
    from fact_table f

    left join addres a
        on f.AREA = a.AREA 
       and f.LOCAL_ADDRESS = a.LOCAL_ADDRESS

    left join cusines c
        on c.cuisine_name = f.cuisine_name

    left join rest r
        on r.REST_NAME = f.REST_NAME
       and r.RESTAURANT_TYPE = f.RESTAURANT_TYPE

    left join onlines o
        on o.ONLINE_ORDERS = f.ONLINE_ORDERS
)

select * 
from fact

order by REST_ID, CUISINE_SK, REST_SK, ONLINE_ID
