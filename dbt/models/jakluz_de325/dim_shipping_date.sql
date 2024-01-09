{{
  config(
    materialized = "view"
  )
}}

with shipping_dates as (

    select
        date_key as shipping_date_key,
        date as shipping_date,
        day as shipping_day_of_month,
        month as shipping_month,
        year as shipping_year,
        day_of_week as shipping_day_of_week,
        quarter as shipping_quarter

    from {{ ref('dim_date')}}

)

select * from shipping_dates