{{ config(materialized="ephemeral") }}
with random_table_master_loan_data_cte as (
    {{ random_table_master_loan_data(source('raw_reverse_svcr_phh', 'vw_phh_loan_master')) }}
)

select *
from random_table_master_loan_data_cte

