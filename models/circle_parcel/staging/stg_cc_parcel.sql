with 

source as (

    select * from {{ source('raw', 'cc_parsel') }}

),

renamed as (

    select
        Parcel_id AS parcel_id,
        Parcel_tracking AS parcel_tracking,
        Transporter AS transporter,
        Priority AS priority,
        PARSE_DATE("%B %d, %Y", Date_purCHase) AS date_purchase,
        PARSE_DATE("%B %d, %Y", Date_sHIpping) AS date_shipping,
        PARSE_DATE("%B %d, %Y", DATE_delivery) AS date_delivery,
        PARSE_DATE("%B %d, %Y", DaTeCANcelled) AS date_cancelled
    from source

)

select * from renamed