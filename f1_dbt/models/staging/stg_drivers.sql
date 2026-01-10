WITH source AS (
    SELECT
        driver_id,
        code AS driver_code,
        given_name AS first_name,
        family_name AS last_name,
        nationality,
        dob AS date_of_birth
    FROM public.drivers_raw
)

SELECT * FROM source