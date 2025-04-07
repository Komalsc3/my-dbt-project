{{
    config(
        materialized="incremental",
        database="stage",
        schema="core",
        post_hook=["delete from {{ this }} trg  where loaddate= current_date();"],
    )
}}

with
    hist_cte as (
        select * exclude(masterservicer, metadataaction), current_date as loaddate
        from {{ ref("bankruptcies") }}
    )
select *
from hist_cte
