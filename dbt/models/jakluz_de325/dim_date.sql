with combined_dates as (

    select
        to_date("Ship_Date", 'DD/MM/YYYY') as date
    from 
        {{ source('upstream', 'superstore_final_dataset') }}

    union all

    select
        to_date("Order_Date", 'DD/MM/YYYY') as date
    from
        {{ source('upstream', 'superstore_final_dataset') }}

),

dates as (

    select distinct date from combined_dates

),

final as (

    select
        to_char(date, 'yyyymmdd')::char(8) as date_key,
        date as date,
        extract(day from date) as day,
        extract(month from date) as month,
        extract(year from date) as year,
        extract(dow from date) as day_of_week,
        extract(quarter from date) as quarter

    from dates

)

select * from final
