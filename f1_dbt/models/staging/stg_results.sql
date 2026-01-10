with source as (

    select
        race_id,
        season,
        round as race_round,
        driver_id,
        constructor_id,
        position as finish_position,
        points,
        status
    from public.results_raw

)

select * from source