with 

source as (

    select * from {{ source('raw', 'cc_parcel_product') }}

),

renamed as (

    select
        ParCEL_id AS parcel_id,
        Model_mAME AS model_name,
        QUANTITY AS qty,
    from source

)

select * from renamed