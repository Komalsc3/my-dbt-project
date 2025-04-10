{{
    config(
        materialized="incremental",        
        database="stage",
        schema="core",
        merge_exclude_columns=["metadataaction"],
        unique_key=["loanid","casenumber","bkfilingstate","bkfilingdistrict","servicer","masterservicer"],
        post_hook = ["delete from {{ this }} trg
where
    UPPER(array_to_string (array_compact (array_construct(loanid,CaseNumber,BKFilingState,BKFilingDistrict)),'|')) NOT IN 
      (SELECT UPPER(array_to_string (array_compact (array_construct(loanid,CaseNumber,BKFilingState,BKFilingDistrict)),'|'))
      from {{ ref('etltargetbankruptcies') }} etltargetbankruptcies 
      where upper(servicer)='{{var('servicer')}}' and upper(masterservicer)='{{var('masterservicer')}}'
    );
"] ) 
}}
with
    final_cte as (
        select
            src.id,
            src.loanid,
            src.servicer,
            src.masterservicer,
            src.servicerloannumber,
            src.servicerworkflowid,
            src.status,
            src.bkcounsel,
            src.borrowercounsel,
            src.bkchapter,
            src.casenumber,
            src.bkfilingstate,
            src.bkfilingdistrict,
            src.petitionfileddate,
            src.postpetitionduedate,
            src.postplanpaymentsource,
            src.prepetitionduedate,
            src.bardate,
            src.pocreferredtocounseldate,
            src.pocfileddate,
            src.amendedpocreferreddate,
            src.amendedpocfileddate,
            src.numberofmfrsfiled,
            src.mfrreferreddate,
            src.mfrfileddate,
            src.mfrentereddate,
            src.mfragreedorderflag,
            src.debtreaffirmeddate,
            src.cramdownflag,
            src.planfileddate,
            src.planconfirmationdate,
            src.planamendmentfileddate,
            src.numberofplanamendmentsfiled,
            src.transferofclaimreferreddate,
            src.transferofclaimfileddate,
            src.noticeoffinalcurereceiveddate,
            src.cureresponsereferreddate,
            src.cureresponsefileddate,
            src.borrowerintent,
            src.midcaseauditreceiveddate,
            src.midcaseauditreferreddate,
            src.midcaseauditfileddate,
            src.primarydebtorname,
            src.codebtorname,
            src.bkdischargedate,
            src.bkdismisseddate,
            src.bkremoveddate,
            src.adversaryproceedingflag,
            src.etllastupdatedat,
            src.etlchangedetectdate,
            src.keyattributehash,
            null as metadataaction
        from  {{ ref("etltargetbankruptcies") }} src
    )
select
    final_cte.id,
    final_cte.loanid,
    final_cte.servicer,
    final_cte.masterservicer,
    final_cte.servicerloannumber,
    final_cte.servicerworkflowid,
    final_cte.status,
    final_cte.bkcounsel,
    final_cte.borrowercounsel,
    final_cte.bkchapter,
    final_cte.casenumber,
    final_cte.bkfilingstate,
    final_cte.bkfilingdistrict,
    final_cte.petitionfileddate,
    final_cte.postpetitionduedate,
    final_cte.postplanpaymentsource,
    final_cte.prepetitionduedate,
    final_cte.bardate,
    final_cte.pocreferredtocounseldate,
    final_cte.pocfileddate,
    final_cte.amendedpocreferreddate,
    final_cte.amendedpocfileddate,
    final_cte.numberofmfrsfiled,
    final_cte.mfrreferreddate,
    final_cte.mfrfileddate,
    final_cte.mfrentereddate,
    final_cte.mfragreedorderflag,
    final_cte.debtreaffirmeddate,
    final_cte.cramdownflag,
    final_cte.planfileddate,
    final_cte.planconfirmationdate,
    final_cte.planamendmentfileddate,
    final_cte.numberofplanamendmentsfiled,
    final_cte.transferofclaimreferreddate,
    final_cte.transferofclaimfileddate,
    final_cte.noticeoffinalcurereceiveddate,
    final_cte.cureresponsereferreddate,
    final_cte.cureresponsefileddate,
    final_cte.borrowerintent,
    final_cte.midcaseauditreceiveddate,
    final_cte.midcaseauditreferreddate,
    final_cte.midcaseauditfileddate,
    final_cte.primarydebtorname,
    final_cte.codebtorname,
    final_cte.bkdischargedate,
    final_cte.bkdismisseddate,
    final_cte.bkremoveddate,
    final_cte.adversaryproceedingflag,
    final_cte.etllastupdatedat,
    final_cte.etlchangedetectdate,
    final_cte.keyattributehash,
    final_cte.metadataaction
from final_cte
{% if is_incremental() %}
    where etllastupdatedat > (select max(etllastupdatedat) from {{ this }})
{% endif %}
