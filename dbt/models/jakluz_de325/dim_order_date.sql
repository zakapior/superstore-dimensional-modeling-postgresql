{{
  config(
    materialized = "view"
  )
}}

with order_dates as (

    select
        date_key as order_date_key,
        date as order_date,
        day as order_day_of_month,
        month as order_month,
        year as order_year,
        day_of_week as order_day_of_week,
        quarter as order_quarter

    from {{ ref('dim_date')}}

)

select * from order_dates