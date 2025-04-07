
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table',
         database="stage",
        schema="core",
        post_hook="UPDATE RAW.APPLICATION.TEST_TABLE SET COLUMN1='ABC'"		
    )
}}

with source_data as (

    select '{{var('masterservicer')}}' as service
    union all
    select null as id

)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
