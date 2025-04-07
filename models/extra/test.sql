{{ config(materialized='table') }}

with source_data as (

    select 1 as id
    union all
    select null as id

)

select * repalce (
  '{{ invocation_id }}' as dbt_invocation_id,
  '{{ model.unique_id }}' as dbt_unique_id,
  '{{ env_var("DBT_CLOUD_PROJECT_ID", "manual") }}' as DBT_CLOUD_PROJECT_ID,
  '{{ env_var("DBT_CLOUD_JOB_ID", "manual") }}' as DBT_CLOUD_JOB_ID,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as DBT_CLOUD_RUN_ID,
  '{{ env_var("DBT_CLOUD_RUN_REASON_CATEGORY", "manual") }}' as DBT_CLOUD_RUN_REASON_CATEGORY,
  '{{ env_var("DBT_CLOUD_RUN_REASON", "manual") }}' as DBT_CLOUD_RUN_REASON,
  sysdate() as updated_at
)
--from your_final_cte
from source_data