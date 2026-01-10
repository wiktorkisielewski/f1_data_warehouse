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
    from public.races_raw

)

select * from source