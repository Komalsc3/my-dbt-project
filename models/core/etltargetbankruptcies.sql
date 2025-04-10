{{ config(materialized="incremental", database="stage", schema="core",
        pre_hook = ["delete from {{ this }} trg where upper(servicer)='{{var('servicer')}}' and upper(masterservicer)='{{var('masterservicer')}}';
"]) }}

{{ union_existing_etltarget_tables("etltargetbankruptcies", [var("servicer")]) }}

