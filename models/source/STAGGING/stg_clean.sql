{{
    config(
        materialized='table'
    )
}}

SELECT  REST_ID,
    REGEXP_REPLACE(
    TRIM(
        REGEXP_REPLACE(REST_NAME, '[^a-zA-Z0-9 ]', '')
    ),
    '\\s+', ' '
) AS REST_NAME,
    RESTAURANT_TYPE,

   TO_NUMBER(TO_VARCHAR(RATING)) AS RATING,

    TO_NUMBER(TO_VARCHAR(TOTAL_RATINGS) )AS TOTAL_RATINGS,

    TO_NUMBER(TO_VARCHAR(COST_FOR_TWO)) AS COST_FOR_TWO,
    ONLINE_ORDERS,
    TABLE_BOOKING,
    AREA,
    LOCAL_ADDRESS
    FROM {{ ref('raw_data') }}