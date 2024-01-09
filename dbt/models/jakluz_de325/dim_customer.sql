with customers as (

    select distinct
        "Customer_ID"::varchar(8) as customer_id,
        "Customer_Name"::varchar(512) as customer_name,
        "Segment"::varchar(11) as segment

    from
        {{ source('upstream', 'superstore_final_dataset') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'customer_name', 'segment']) }}::varchar(32) as customer_key,
        customers.*
    
    from
        customers

)

select * from final