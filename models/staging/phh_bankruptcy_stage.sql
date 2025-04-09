{{
    config(
        materialized="table",
        database="stage",
        schema="staging",
        tags="phh_bankruptcy",
    )
}}
with
    phh_phi as (
        select
            lm.loanid,
            lm.loan_skey,
            li.loannumber,
            li.loaninfoidentifier,
            max(bk.refinfoidentifier) as bkid
        from {{ source("raw_reverse_svcr_phh", "vw_phh_bkfs_loaninfo_loaninfomsp") }} li
        inner join
            {{ source("raw_reverse_svcr_phh", "vw_phh_bkfs_refinfo") }} ri
            on ri.loaninfoidentifier = li.loaninfoidentifier
        inner join
            {{ source("raw_reverse_svcr_phh", "vw_phh_bkfs_bankruptcycaseinfo") }} bk
            on bk.refinfoidentifier = ri.refinfoidentifier
        left outer join
            {{ source("raw_reverse_svcr_phh", "vw_phh_loan_master") }} lm
            on repeat('0', 10 - len(lm.loan_skey)) || lm.loan_skey = li.loannumber
        group by lm.loanid, lm.loan_skey, li.loannumber, li.loaninfoidentifier
    ),
    phh_data_cte as (
        select
            p.loanid as loanid,
            'PHH' as servicer,  -- dbo.ContactPull(l.ServicerId) AS SERVICER
            'PHH' as masterservicer,
            pl.servicerloannumber as servicerloannumber,
            replace(
                concat(
                    p.loanid,
                    bk.bankruptcycasenumber,
                    bk.bankruptcyfilingstate,
                    coalesce(bk.bankruptcydistrict, '')
                ),
                '-',
                ''
            ) as servicerworkflowid,  -- renamed
            case
                when pl.loan_sub_status_description not like ('%bnk%')  /* and dbo.ContactPull(l.ServicerId) = 'PHH'*/
                then 'Closed'
                else 'Active'
            end as status,
            ri.assignedvendor as bkcounsel,
            bk.bankruptcychapter as bkchapter,
            bk.bankruptcycasenumber as casenumber,
            bk.bankruptcyfilingstate as bkfilingstate,
            case
                when bk.bankruptcydistrict like ('%Southern%')
                then 'Southern'
                when bk.bankruptcydistrict like ('%Middle%')
                then 'Middle'
                when bk.bankruptcydistrict like ('%Northern%')
                then 'Northern'
                when bk.bankruptcydistrict like ('%Eastern%')
                then 'Eastern'
                when bk.bankruptcydistrict like ('%Middle%')
                then 'Middle'
                when bk.bankruptcydistrict like ('%Central')
                then 'Central'
                when bk.bankruptcydistrict like ('%all%')
                then null
            end as bkfilingdistrict,
            bk.bankruptcyfilingdate as petitionfileddate,
            bk.bankruptcypocbardate as bardate,
            bks.fileprfclmdate as pocfileddate,  -- renamed
            bks.filemtnforrlfdate as mfrfileddate,
            bks.mtnrlfgrntdate as mfrentereddate,
            bks.bnkrtcyplancnfrmddate as planconfirmationdate,
            bks.bnkrtcydischrgddate as bkdischargedate,
            bks.bnkrtcydsmssddate as bkdismisseddate,
            case
                when pl.loan_sub_status_description not like '%bnk%'
                then
                    (
                        select max(plmh.loaddate)
                        from
                            {{
                                source(
                                    "raw_reverse_svcr_phh", "phh_monthly_loan_summary"
                                )
                            }} plmh
                        inner join
                            {{ source("raw_reverse_svcr_phh", "vw_phh_loan_master") }} lm
                            on lm.loan_skey = plmh.loan_skey
                            and lm.loan_sub_status_description like '%bnk%'
                    )
            end as bkremoveddate
        from {{ ref("ephe_phh_loandata") }} pl
        inner join phh_phi p on p.loanid = pl.loanid
        inner join
            {{ source("raw_reverse_svcr_phh", "vw_phh_bkfs_bankruptcycaseinfo") }} bk
            on bk.refinfoidentifier = p.bkid
        left outer join
            {{ source("raw_reverse_svcr_phh", "vw_phh_bkfs_refinfo") }} ri
            on ri.refinfoidentifier = bk.refinfoidentifier
        left outer join
            (
                select *
                from
                    (
                        select
                            bks.*,
                            row_number() over (
                                partition by bks.loanid
                                order by
                                    abs(
                                        datediff(
                                            day,
                                            bk.bankruptcyfilingdate,
                                            coalesce(
                                                bks.bnkrtcyfilech7date,
                                                bks.bnkrtcyfilech13date,
                                                bks.bnkrtcyfilech11date
                                            )
                                        )
                                    )
                            ) as correct_value
                        from {{ source("raw_reverse_svcr_phh", "phh_bksummary") }} bks
                        inner join
                            {{ source("raw_application", "loans") }} l
                            on l.id = bks.loanid
                        inner join phh_phi p on p.loanid = bks.loanid
                        inner join
                            {{
                                source(
                                    "raw_reverse_svcr_phh",
                                    "vw_phh_bkfs_bankruptcycaseinfo",
                                )
                            }} bk on bk.refinfoidentifier = p.bkid
                    )
                where correct_value = 1
            ) bks
            on pl.id = bks.loanid
    )
select
    loanid,
    servicer,
    masterservicer,
    servicerloannumber,
    servicerworkflowid,
    status,
    bkcounsel,
    null as borrowercounsel,
    bkchapter,
    casenumber,
    bkfilingstate,
    bkfilingdistrict,
    petitionfileddate,
    null as postpetitionduedate,
    null as postplanpaymentsource,
    null as prepetitionduedate,
    bardate,
    null as pocreferredtocounseldate,
    pocfileddate,
    null as amendedpocreferreddate,
    null as amendedpocfileddate,
    null as numberofmfrsfiled,
    null as mfrreferreddate,
    mfrfileddate,
    mfrentereddate,
    null as mfragreedorderflag,
    null as debtreaffirmeddate,
    null as cramdownflag,
    null as planfileddate,
    planconfirmationdate,
    null as planamendmentfileddate,
    null as numberofplanamendmentsfiled,
    null as transferofclaimreferreddate,
    null as transferofclaimfileddate,
    null as noticeoffinalcurereceiveddate,
    null as cureresponsereferreddate,
    null as cureresponsefileddate,
    null as borrowerintent,
    null as midcaseauditreceiveddate,
    null as midcaseauditreferreddate,
    null as midcaseauditfileddate,
    null as primarydebtorname,
    null as codebtorname,
    bkdischargedate,
    bkdismisseddate,
    bkremoveddate,
    null as adversaryproceedingflag
from phh_data_cte
