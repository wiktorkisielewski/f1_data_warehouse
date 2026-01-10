WITH source AS (

    SELECT
        constructor_id,
        name AS constructor_name,
        nationality,
        url AS constructor_url
    FROM public.constructors_raw

)

SELECT * FROM source