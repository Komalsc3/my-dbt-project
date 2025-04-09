{{
    config(
        materialized="incremental",        
        database="stage",
        schema="core",
        alias="etltargetbankruptcies_new") 
}}

with
    update_table as (
        select
            b.* exclude(status, bkfilingdistrict),

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
            end as bkfilingdistrict
        from {{ ref("celink_bankruptcy_stage") }} b
        left outer join
            {{ source("raw_reverse_svcr_celink", "vw_celink_loandata") }} ld on ld.loanid = b.loanid
    ),
    transient_table as (
        select
            *,
            sha2(
                array_to_string(
                    array_compact(
                        array_construct(
                            loanid,
                            servicer,
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
                            adversaryproceedingflag
                        )
                    ),
                    '| '
                ),
                256
            ) as keyattributehash
        from update_table
    ),
    final as (
        select
            stage.core.etltargetbankruptcies_id.nextval as id,
            stage.loanid,
            stage.servicer,
            stage.masterservicer,
            stage.servicerloannumber,
            ifnull(stage.servicerworkflowid, 'no value ') as servicerworkflowid,
            stage.status,
            stage.bkcounsel,
            stage.borrowercounsel,
            stage.bkchapter,
            stage.casenumber,
            stage.bkfilingstate,
            stage.bkfilingdistrict,
            stage.petitionfileddate,
            stage.postpetitionduedate,
            stage.postplanpaymentsource,
            stage.prepetitionduedate,
            stage.bardate,
            stage.pocreferredtocounseldate,
            stage.pocfileddate,
            stage.amendedpocreferreddate,
            stage.amendedpocfileddate,
            stage.numberofmfrsfiled,
            stage.mfrreferreddate,
            stage.mfrfileddate,
            stage.mfrentereddate,
            stage.mfragreedorderflag,
            stage.debtreaffirmeddate,
            stage.cramdownflag,
            stage.planfileddate,
            stage.planconfirmationdate,
            stage.planamendmentfileddate,
            stage.numberofplanamendmentsfiled,
            stage.transferofclaimreferreddate,
            stage.transferofclaimfileddate,
            stage.noticeoffinalcurereceiveddate,
            stage.cureresponsereferreddate,
            stage.cureresponsefileddate,
            stage.borrowerintent,
            stage.midcaseauditreceiveddate,
            stage.midcaseauditreferreddate,
            stage.midcaseauditfileddate,
            stage.primarydebtorname,
            stage.codebtorname,
            stage.bkdischargedate,
            stage.bkdismisseddate,
            stage.bkremoveddate,
            stage.adversaryproceedingflag,
            current_date() as etllastupdatedat,
            current_date() as etlchangedetectdate,
            stage.keyattributehash
        from transient_table stage
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
from final
