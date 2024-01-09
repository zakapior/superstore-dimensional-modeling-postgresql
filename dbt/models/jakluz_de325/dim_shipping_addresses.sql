with shipping_addresses as (

    select distinct
        "City"::varchar(32) as shipping_city,
        "Country"::varchar(16) as shipping_country,
        "State"::varchar(32) as shipping_state,
        "Postal_Code" as shipping_postal_code,
        "Region"::varchar(8) as shipping_region
    from
        {{ source('upstream', 'superstore_final_dataset') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['shipping_city', 'shipping_country', 'shipping_state', 'shipping_postal_code', 'shipping_region']) }}::varchar(32) as shipping_address_key,
        *
    from
        shipping_addresses

)

select * from final