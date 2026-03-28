with 

source as (

    select * from {{ source('raw', 'ship') }}

),

cleaned as (

    select
        orders_id,
        shipping_fee,
        logcost,
        CAST(ship_cost AS FLOAT64) AS ship_cost
    from source

)

select * from cleaned