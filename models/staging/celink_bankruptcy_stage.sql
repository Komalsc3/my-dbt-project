{{
    config(
        materialized="table",
        database="stage",
        schema="staging",
        tags="Celink_bankruptcy",
        alias="celink_bankruptcy_stage_" ~ var("masterservicer")
    )
}}

with
    celink_bkci_cte as (
        select
            ld.loanid,
            bankruptcycasenumber,
            bankruptcyfilingstate,
            max(bk.refinfoidentifier) as bkid
        from
            {{
                source(
                    "raw_reverse_svcr_celink", "vw_celink_bkfs_bankruptcy_case_info"
                )
            }} bk
        inner join
            {{ source("raw_reverse_svcr_celink", "vw_celink_bkfs_ref_info") }} ri
            on ri.refinfoidentifier = bk.refinfoidentifier
        inner join
            {{ source("raw_reverse_svcr_celink", "vw_celink_bkfs_loan_info") }} li
            on li.loaninfoidentifier = ri.loaninfoidentifier
        inner join
            {{ source("raw_reverse_svcr_celink", "vw_celink_loandata") }} ld
            on ld.loannumber = li.loannumber
        where upper(ld.masterservicer) = '{{var('masterservicer')}}'
        group by ld.loanid, bankruptcycasenumber, bankruptcyfilingstate
    ),
    celink_cbk_cte as (
        select
            bk.loannumber,
            chapter13borrowerattorney,
            chapter7borrowerattorney,
            chapter13fileddate,
            chapter7fileddate,
            chapter13proofofclaimdeadline,
            chapter13proofofclaimfiled,
            chapter13motionforrelieffiled,
            chapter13motionforreliefgranted,
            chapter13planconfirmed,
            chapter13discharged,
            chapter13dismissedordischarged,
            chapter7discharged,
            chapter7dismissedordischarged,
            chapter13dismissed,
            chapter7dismissed,
            chapter7dateofreferral,
            chapter13dateofreferral
        from {{ source("raw_reverse_svcr_celink", "vw_celink_loandata") }} ld
        inner join
            (
                select *
                from
                    (
                        select
                            *,
                            row_number() over (
                                partition by loannumber order by loaddate desc
                            ) as row_num
                        from
                            {{
                                source(
                                    "raw_reverse_svcr_celink", "vw_celink_bankruptcy"
                                )
                            }}
                    )
                where row_num = 1
            ) bk
            on bk.loannumber = ld.loannumber
        where upper(ld.masterservicer) = '{{var('masterservicer')}}'
    ),
    celink_bankruptcy_cte as (
        select
            rd.loanid as loanid,
            'CELINK' as servicer,  -- dbo.ContactPull(rd.ServicerId) as Servicer
            '{{var('masterservicer')}}',
            rd.loannumber as servicerloannumber,
            replace(
                concat(
                    rd.loanid,
                    bk.bankruptcycasenumber,
                    bk.bankruptcyfilingstate,
                    bk.bankruptcydistrict
                ),
                '-',
                ''
            ) as servicerworkflowid,
            ri.assignedvendor as bkcounsel,
            coalesce(
                cbk.chapter13borrowerattorney, cbk.chapter7borrowerattorney
            ) as borrowercounsel,
            bk.bankruptcychapter as bkchapter,
            bk.bankruptcycasenumber as casenumber,
            bk.bankruptcyfilingstate as bkfilingstate,
            bk.bankruptcydistrict as bkfilingdistrict,
            case
                when bk.bankruptcychapter = 13
                then cbk.chapter13fileddate
                when bk.bankruptcychapter = 7
                then cbk.chapter7fileddate
                else bk.bankruptcyfilingdate
            end as petitionfileddate,
            cbk.chapter13proofofclaimdeadline as bardate,
            cbk.chapter13proofofclaimfiled as pocfileddate,
            cbk.chapter13motionforrelieffiled as mfrfileddate,
            cbk.chapter13motionforreliefgranted as mfrentereddate,
            cbk.chapter13planconfirmed as planconfirmationdate,
            case
                when cbk.chapter13discharged = 'True'
                then cbk.chapter13dismissedordischarged
                when cbk.chapter7discharged = 'True'
                then cbk.chapter7dismissedordischarged
                else null
            end as bkdishargeddate,
            case
                when cbk.chapter13dismissed = 'True'
                then cbk.chapter13dismissedordischarged
                when cbk.chapter7dismissed = 'True'
                then cbk.chapter7dismissedordischarged
                else null
            end as bkdismisseddate
        from {{ ref("ephe_celink_loandata") }} rd
        inner join
            {{ source("raw_reverse_svcr_celink", "vw_celink_bkfs_loan_info") }} li
            on li.loannumber = rd.loannumber
        inner join
            {{ source("raw_reverse_svcr_celink", "vw_celink_bkfs_ref_info") }} ri
            on ri.loaninfoidentifier = li.loaninfoidentifier
        inner join celink_bkci_cte ci on ci.loanid = rd.loanid
        inner join
            {{
                source(
                    "raw_reverse_svcr_celink", "vw_celink_bkfs_bankruptcy_case_info"
                )
            }} bk
            on bk.refinfoidentifier = ri.refinfoidentifier
            and bk.refinfoidentifier = ci.bkid
        left outer join
            celink_cbk_cte cbk
            on cbk.loannumber = rd.loannumber
            and greatest(
                coalesce(cbk.chapter7dateofreferral, '1990-01-01'),
                coalesce(cbk.chapter13dateofreferral, '1990-01-01')
            )
            > bk.bankruptcyfilingdate
        where upper(rd.masterservicer) = '{{var('masterservicer')}}'
    ),
    celink_cbpop_cte as (
        select
            rd.id as loanid,
            concat(
                rd.id, coalesce(bk.chapter7casenumber, bk.chapter13casenumber, '')
            ) as servicerworkflowid,
            max(bk.id) as bkid
        from {{ ref("ephe_celink_loandata") }} rd
        inner join
            {{ source("raw_reverse_svcr_celink", "vw_celink_bankruptcy") }} bk
            on bk.loannumber = rd.loannumber
        left outer join
            celink_bankruptcy_cte b
            on b.loanid = rd.loanid
            and (
                b.casenumber = bk.chapter7casenumber
                or b.casenumber = bk.chapter13casenumber
            )
        where
            b.loanid is null
            and upper(rd.masterservicer) = '{{var('masterservicer')}}'
        group by 1, 2
    ),
    celink_bankruptcy_stage_cte as (
        select
            rd.id as loanid,
            'CELINK' as servicer,  -- dbo.ContactPull(rd.ServicerId) AS 
            '{{var('masterservicer')}}' as masterservicer,
            rd.loannumber as servicerloannumber,
            bp.servicerworkflowid,
            null as bkfilingstate,
            null as bkfilingdistrict,
            null as planconfirmationdate,
            coalesce(
                fn.firmname, bk.chapter13celinkattorneyname, 'Unassigned'
            ) as bkcounsel,
            coalesce(
                tn.firmname,
                sn.firmname,
                bk.chapter13borrowerattorney,
                bk.chapter7borrowerattorney,
                null
            ) as borrowercounsel,
            case
                when
                    ifnull(bk.chapter7fileddate, '1/1/1900')
                    > ifnull(bk.chapter13fileddate, '1/1/1900')
                then 7
                when
                    ifnull(bk.chapter13fileddate, '1/1/1900')
                    > ifnull(bk.chapter7fileddate, '1/1/1900')
                then 13
            end as bkchapter,
            coalesce(bk.chapter7casenumber, bk.chapter13casenumber) as casenumber,
            coalesce(bk.chapter7fileddate, bk.chapter13fileddate) as petitionfileddate,
            bk.chapter13proofofclaimdeadline as bardate,
            bk.chapter13proofofclaimfiled as pocfileddate,
            bk.chapter13motionforrelieffiled as mfrfileddate,
            bk.chapter13motionforreliefgranted as mfrentereddate,
            case
                when
                    bk.chapter7realtionshiptoborrower in (
                        'Borrower',
                        'Self',
                        'Borrowers',
                        'Both',
                        'Both Borrowers',
                        'Borrower & CoBorrower'
                    )
                then b.fullname
                when
                    bk.chapter13realtionshiptoborrower in (
                        'Borrower',
                        'Self',
                        'Borrowers',
                        'Both',
                        'Both Borrowers',
                        'Borrower & CoBorrower'
                    )
                then b.fullname
                when bk.chapter13realtionshiptoborrower in ('Co-Borrower', 'CoBorrower')
                then c.fullname
                when bk.chapter7realtionshiptoborrower is not null
                then bk.chapter7realtionshiptoborrower
                when bk.chapter13realtionshiptoborrower is not null
                then bk.chapter13realtionshiptoborrower
            end as primarydebtorname,
            case
                when
                    bk.chapter7realtionshiptoborrower
                    in ('Borrowers', 'Both', 'Both Borrowers', 'Borrower & CoBorrower')
                then c.fullname
                when
                    bk.chapter13realtionshiptoborrower
                    in ('Borrowers', 'Both', 'Both Borrowers', 'Borrower & CoBorrower')
                then c.fullname
                when bk.chapter7realtionshiptoborrower is not null
                then bk.chapter7realtionshiptoborrower
                when bk.chapter13realtionshiptoborrower is not null
                then bk.chapter13realtionshiptoborrower
            end as codebtorname,
            case
                when bk.chapter13discharged = 'True'
                then bk.chapter13dismissedordischarged
                when bk.chapter7discharged = 'True'
                then bk.chapter7dismissedordischarged
            end as bkdischargedate,
            case
                when bk.chapter13dismissed = 'True'
                then bk.chapter13dismissedordischarged
                when bk.chapter7dismissed = 'True'
                then bk.chapter7dismissedordischarged
            end as bkdismisseddate

        from {{ ref("ephe_celink_loandata") }} rd
        inner join celink_cbpop_cte bp on bp.loanid = rd.loanid
        inner join
            {{ source("raw_reverse_svcr_celink", "vw_celink_bankruptcy") }} bk
            on bk.id = bp.bkid
            and bk.loannumber = rd.loannumber
        left outer join
            {{ source("stage_reference", "firmname") }} fn
            on fn.rawname = bk.chapter13borrowerattorney
        left outer join
            {{ source("stage_reference", "firmname") }} sn
            on sn.rawname = bk.chapter7borrowerattorney
        left outer join
            {{ source("stage_reference", "firmname") }} tn
            on tn.rawname = bk.chapter13borrowerattorney
        left outer join
            {{ source("raw_application", "parties") }} b on b.id = rd.borrower1id
        left outer join
            {{ source("raw_application", "parties") }} c on c.id = rd.borrower2id
        where upper(rd.masterservicer) = '{{var('masterservicer')}}'
    ),
    union_of_cte as (
        select
            loanid,
            servicer,
            '{{var('masterservicer')}}' masterservicer,
            servicerloannumber,
            servicerworkflowid,
            bkcounsel,
            borrowercounsel,
            bkchapter,
            casenumber,
            bkfilingstate,
            bkfilingdistrict,
            petitionfileddate,
            bardate,
            pocfileddate,
            mfrfileddate,
            mfrentereddate,
            planconfirmationdate,
            null as primarydebtorname,
            null as codebtorname,
            null as bkdischargedate,
            bkdismisseddate
        from celink_bankruptcy_cte
        union
        select
            loanid,
            servicer,
            '{{var('masterservicer')}}' masterservicer,
            servicerloannumber,
            servicerworkflowid,
            bkcounsel,
            borrowercounsel,
            bkchapter,
            casenumber,
            null as bkfilingstate,
            null as bkfilingdistrict,
            petitionfileddate,
            bardate,
            pocfileddate,
            mfrfileddate,
            mfrentereddate,
            null as planconfirmationdate,
            primarydebtorname,
            codebtorname,
            bkdischargedate,
            bkdismisseddate
        from celink_bankruptcy_stage_cte
    )
select
    loanid,
    servicer,
    masterservicer,
    servicerloannumber,
    servicerworkflowid,
    null as status,
    bkcounsel,
    borrowercounsel,
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
    primarydebtorname,
    codebtorname,
    bkdischargedate,
    bkdismisseddate,
    null as bkremoveddate,
    null as adversaryproceedingflag
from union_of_cte
