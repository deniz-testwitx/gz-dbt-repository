with 

parcel_source as (
    select * from {{ ref('stg_cc_parcel') }}
),

parcel_product_source as (
    select * from {{ ref('stg_cc_parcel_product') }}
),

parcel_product_summary as (
    select
        parcel_id,
        SUM(qty) AS qty,
        COUNT(DISTINCT model_name) AS nb_model
    from {{ ref('stg_cc_parcel_product') }}
    group by parcel_id
),

transformed as (

    select
        s.*,
        p.model_name,
        EXTRACT(MONTH FROM date_purchase) AS month_purchase,
        CASE 
            WHEN date_cancelled IS NOT NULL THEN 'Cancelled'
            WHEN date_shipping IS NULL THEN 'Processing'
            WHEN date_delivery IS NULL THEN 'On the Road'
            WHEN date_delivery IS NOT NULL THEN 'Delivered'
        END AS status,
        DATE_DIFF(date_shipping, date_purchase, DAY) AS expedition_time,
        DATE_DIFF(date_delivery, date_shipping, DAY) AS transport_time,
        DATE_DIFF(date_delivery, date_purchase, DAY) AS delivery_time,
        IF(date_delivery IS NULL, NULL, IF(DATE_DIFF(date_delivery, date_purchase, DAY) > 7, 1, 0)) AS delay,
        ss.qty,
    from parcel_source s
    LEFT JOIN parcel_product_summary ss ON s.parcel_id = ss.parcel_id
    LEFT JOIN parcel_product_source p ON s.parcel_id = p.parcel_id
)

select * from transformed