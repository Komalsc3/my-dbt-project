{{
    config(
        materialized="table",        
        database="stage",
        schema="core",
        alias="etltargetbankruptcies_celink_" ~ var("masterservicer")) 
}}

with
    transient_table as (
        select
            stage.core.etltargetbankruptcies_id.nextval as id,
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
            case
                when b.bkdischargedate is not null
                then 'complete '
                when b.bkdismisseddate is not null
                then 'closed '
                when b.mfrentereddate is not null
                then 'complete '
                when b.bkremoveddate is not null
                then 'closed '
                when ld.status_description <> 'bankruptcy ' and b.servicer = 'celink '
                then 'closed '
                else 'active '
            end as status,
            case
                when upper(b.bkfilingdistrict) like ('%s outhern % ')
                then 'southern '
                when upper(b.bkfilingdistrict) like ('% middle % ')
                then 'middle '
                when upper(b.bkfilingdistrict) like ('% northern % ')
                then 'northern '
                when upper(b.bkfilingdistrict) like ('% eastern % ')
                then 'eastern '
                when upper(b.bkfilingdistrict) like ('% middle % ')
                then 'middle '
                when upper(b.bkfilingdistrict) like ('% central ')
                then 'central '
                when upper(b.bkfilingdistrict) like ('% all % ')
                then null
            end as bkfilingdistrict,
            {{
                hash_key_generate(
                    var("servicer")~"_bankruptcy_stage_"~ var("masterservicer"), "b"
                )
            }},
            current_date() as etllastupdatedat,
            current_date() as etlchangedetectdate
        from {{ ref("celink_bankruptcy_stage") }} b
        left outer join
            {{ source("raw_reverse_svcr_celink", "vw_celink_loandata") }} ld
            on ld.loanid = b.loanid
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
