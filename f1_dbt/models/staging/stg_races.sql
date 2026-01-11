with source as (

    select
        race_id,
        season,
        round as race_round,
        race_name,
        race_date,
        race_time,
        circuit_id,
        circuit_name,
        locality,
        country
    from {{ source('f1_raw', 'races_raw') }}

)

select * from source