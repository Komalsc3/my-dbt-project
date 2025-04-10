{{
    config(
        materialized="table",        
        database="stage",
        schema="core"
"]) 
}}

{{ 
  union_existing_etltarget_tables(
    base_table_name="etltargetbankruptcies",
    servicer=["celink", "phh"]
  ) 
}}
