with source as (

    select
        constructor_id,
        name as constructor_name,
        nationality,
        url as constructor_url
    from public.constructors_raw

)

select * from source