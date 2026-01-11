with source as (

    select
        constructor_id,
        name as constructor_name,
        nationality,
        url as constructor_url
    from {{ source('f1_raw', 'constructors_raw') }}

)

select * from source