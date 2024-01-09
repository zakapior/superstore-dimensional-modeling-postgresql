with

customers as (

    select
        *

    from
        {{ ref('dim_customer') }}

),

products as (

    select
        *

    from
        {{ ref("dim_products") }}

),

shipping_addresses as (

    select
        *

    from
        {{ ref("dim_shipping_addresses") }}

),

order_dates as (

    select
        *

    from
        {{ ref("dim_order_date") }}

),

shipping_dates as (

    select
        *
        
    from
        {{ ref("dim_shipping_date") }}

),

source_orders as (

    select
        "Row_ID",
        "Postal_Code",
        "Sales",
        "Ship_Date",
        "Ship_Mode",
        "Customer_ID",
        "Customer_Name",
        "Segment",
        "Country",
        "City",
        "State",
        "Product_Name",
        "Region",
        "Product_ID",
        "Category",
        "Sub_Category",
        "Order_ID",
        "Order_Date"

    from
        {{ source('upstream', 'superstore_final_dataset') }}

),

final as (
    select
        "Row_ID" as row_id,
        "Order_ID"::varchar(14) as order_id,
        order_dates.order_date_key as order_date_key,
        shipping_dates.shipping_date_key as shipping_date_key,
        "Ship_Mode" as ship_mode,
        customers.customer_key as customer_key,
        shipping_addresses.shipping_address_key as shipping_address_key,
        products.product_key as product_key,
        "Sales"::numeric(8,2) as sales_amount

    from
        source_orders
    join order_dates on
        order_dates.order_date = to_date(source_orders."Order_Date", 'DD/MM/YYYY')
    join shipping_dates on
        shipping_dates.shipping_date = to_date(source_orders."Ship_Date", 'DD/MM/YYYY')
    join customers on
        customers.customer_id = source_orders."Customer_ID" and
        customers.customer_name = source_orders."Customer_Name" and
        customers.segment = source_orders."Segment"
    join shipping_addresses on
        shipping_addresses.shipping_country = source_orders."Country" and
        shipping_addresses.shipping_city = source_orders."City" and
        shipping_addresses.shipping_state = source_orders."State" and
        shipping_addresses.shipping_postal_code = source_orders."Postal_Code" and
        shipping_addresses.shipping_region = source_orders."Region"
    join products on
        products.product_id = source_orders."Product_ID" and
        products.product_category = source_orders."Category" and
        products.product_sub_category = source_orders."Sub_Category" and
        products.product_name = source_orders."Product_Name"
)

select * from final