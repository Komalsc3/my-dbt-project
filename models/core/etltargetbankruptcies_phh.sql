{{
    config(
        materialized="table",        
        database="stage",
        schema="core",
        alias="etltargetbankruptcies_phh_"~ var("masterservicer")) 
}}

with
    transient_table as (
        select
            null as id,
            b.loanid,
            b.servicer,
            b.masterservicer,
            b.servicerloannumber,
            b.servicerworkflowid,
            b.bkcounsel,
            b.borrowercounsel,
            b.bkchapter,
            b.casenumber,
            b.bkfilingstate,
            b.petitionfileddate,
            b.postpetitionduedate,
            b.postplanpaymentsource,
            b.prepetitionduedate,
            b.bardate,
            b.pocreferredtocounseldate,
            b.pocfileddate,
            b.amendedpocreferreddate,
            b.amendedpocfileddate,
            b.numberofmfrsfiled,
            b.mfrreferreddate,
            b.mfrfileddate,
            b.mfrentereddate,
            b.mfragreedorderflag,
            b.debtreaffirmeddate,
            b.cramdownflag,
            b.planfileddate,
            b.planconfirmationdate,
            b.planamendmentfileddate,
            b.numberofplanamendmentsfiled,
            b.transferofclaimreferreddate,
            b.transferofclaimfileddate,
            b.noticeoffinalcurereceiveddate,
            b.cureresponsereferreddate,
            b.cureresponsefileddate,
            b.borrowerintent,
            b.midcaseauditreceiveddate,
            b.midcaseauditreferreddate,
            b.midcaseauditfileddate,
            b.primarydebtorname,
            b.codebtorname,
            b.bkdischargedate,
            b.bkdismisseddate,
            b.bkremoveddate,
            b.adversaryproceedingflag,
            b.status,
            b.bkfilingdistrict,
            {{
                hash_key_generate(
                    var("servicer")~"_bankruptcy_stage", "b"
                )
            }},
            current_date() as etllastupdatedat,
            current_date() as etlchangedetectdate
        from {{ ref("phh_bankruptcy_stage") }} b
    )
select
    id,
    loanid,
    servicer,
    masterservicer,
    servicerloannumber,
    servicerworkflowid,
    status,
    bkcounsel,
    borrowercounsel,
    bkchapter,
    casenumber,
    bkfilingstate,
    bkfilingdistrict,
    petitionfileddate,
    postpetitionduedate,
    postplanpaymentsource,
    prepetitionduedate,
    bardate,
    pocreferredtocounseldate,
    pocfileddate,
    amendedpocreferreddate,
    amendedpocfileddate,
    numberofmfrsfiled,
    mfrreferreddate,
    mfrfileddate,
    mfrentereddate,
    mfragreedorderflag,
    debtreaffirmeddate,
    cramdownflag,
    planfileddate,
    planconfirmationdate,
    planamendmentfileddate,
    numberofplanamendmentsfiled,
    transferofclaimreferreddate,
    transferofclaimfileddate,
    noticeoffinalcurereceiveddate,
    cureresponsereferreddate,
    cureresponsefileddate,
    borrowerintent,
    midcaseauditreceiveddate,
    midcaseauditreferreddate,
    midcaseauditfileddate,
    primarydebtorname,
    codebtorname,
    bkdischargedate,
    bkdismisseddate,
    bkremoveddate,
    adversaryproceedingflag,
    etllastupdatedat,
    etlchangedetectdate,
    keyattributehash
from transient_table
