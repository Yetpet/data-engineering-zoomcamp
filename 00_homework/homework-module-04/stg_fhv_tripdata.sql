with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (

select
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,

    -- Rename fields
    PUlocationID as pickup_location_id,
    DOlocationID as dropoff_location_id,
    SR_Flag as sr_flag

from source
where dispatching_base_num is not null
)
select * from renamed
