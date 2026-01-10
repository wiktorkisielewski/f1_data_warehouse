with source as (
    select
        driver_id,
        code as driver_code,
        given_name as first_name,
        family_name as last_name,
        nationality,
        dob as date_of_birth
    from public.drivers_raw
)

select * from source