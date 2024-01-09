with

products as (

    select distinct
        "Product_ID"::varchar(15) product_id,
        "Category"::varchar(15) product_category,
        "Sub_Category"::varchar(11) product_sub_category,
        "Product_Name" product_name
    from
        {{ source('upstream', 'superstore_final_dataset') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['product_id', 'product_category', 'product_sub_category', 'product_name']) }}::varchar(32) as product_key,
        products.*
    from
        products


)

select * from final
