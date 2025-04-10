{{ config(materialized="table", database="stage", schema="core",
        pre_hook = ["delete from {{ this }} trg where upper(servicer)='{{var('servicer')}}' and upper(masterservicer)='{{var('masterservicer')}}';
"]) }}

{{
    union_existing_etltarget_tables(
        base_table_name="etltargetbankruptcies", servicer=["celink", "phh"]
    )
}}
