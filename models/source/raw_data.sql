{{ config(
    materialized = 'table',

) }}

SELECT 
    -- Clean restaurant name (remove symbols + extra spaces)
    REST_ID,
    REST_NAME,

    RESTAURANT_TYPE ,

   (RATE_OUT_OF_5) AS RATING,

    (NUM_OF_RATINGS) AS TOTAL_RATINGS,

    (AVG_COST_TWO_PEOPLE) AS COST_FOR_TWO,

    ONLINE_ORDER AS ONLINE_ORDERS,
    TABLE_BOOKING,
    CUISINES_TYPE,
    AREA,
    LOCAL_ADDRESS

FROM {{ source('zoma_s', 'DBT_TABLE') }}
