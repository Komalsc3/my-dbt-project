with
    dm_bankruptcy_temp as (
        select
            bk.loanid,
            bk.servicer,
            bk.servicerloannumber,
            bk.servicerworkflowid,
            bk.status,
            bk.bkcounsel,
            bk.borrowercounsel,
            bk.bkchapter,
            bk.casenumber,
            bk.bkfilingstate,
            bk.bkfilingdistrict,
            bk.petitionfileddate,
            bk.postpetitionduedate,
            bk.postplanpaymentsource,
            bk.prepetitionduedate,
            bk.bardate,
            bk.pocreferredtocounseldate,
            bk.pocfileddate,
            bk.amendedpocreferreddate,
            bk.amendedpocfileddate,
            bk.numberofmfrsfiled,
            bk.mfrreferreddate,
            bk.mfrfileddate,
            bk.mfrentereddate,
            bk.mfragreedorderflag,
            bk.debtreaffirmeddate,
            bk.cramdownflag,
            bk.planfileddate,
            bk.planconfirmationdate,
            bk.planamendmentfileddate,
            bk.numberofplanamendmentsfiled,
            bk.transferofclaimreferreddate,
            bk.transferofclaimfileddate,
            bk.noticeoffinalcurereceiveddate,
            bk.cureresponsereferreddate,
            bk.cureresponsefileddate,
            bk.borrowerintent,
            bk.midcaseauditreceiveddate,
            bk.midcaseauditreferreddate,
            bk.midcaseauditfileddate,
            bk.primarydebtorname,
            bk.codebtorname,
            bk.bkdischargedate,
            bk.bkdismisseddate,
            bk.bkremoveddate,
            bk.adversaryproceedingflag,
            bk.loaddate,
            ifnull(
                me.me_date,
                to_date((select max(snapshotdate) from edw.pbi.loan_master_history))
            ) as snapshotdate,
            least_ignore_nulls(
                mfrentereddate, bkdischargedate, bkdismisseddate, bkremoveddate
            ) as closeddate,
            case
                when mfrentereddate is not null
                then 'mfr granted'
                when bkdismisseddate is not null
                then 'dismissed'
                when bkdischargedate is not null
                then 'discharge granted'
                when bkremoveddate is not null
                then 'bk removed'
                else null
            end as closedreason,
            coalesce(fn.firmname, bk.bkcounsel, 'unassigned') as bkfirmnormalized,
            case
                when bk.postpetitionduedate is not null
                then dateadd(d, 65, bk.postpetitionduedate)
            end as mfrsla,
            dateadd(day, 65, bk.petitionfileddate) as pocsla,
            case
                when bk.pocfileddate is not null
                then
                    case
                        when bk.pocfileddate <= dateadd(d, 65, bk.petitionfileddate)
                        then 'met'
                        else 'missed'
                    end
                when bk.pocfileddate is null
                then
                    case
                        when
                            dateadd(day, 65, bk.petitionfileddate)
                            <= dateadd(day, -1, bk.loaddate)
                        then 'missed'
                        when
                            dateadd(day, 65, bk.petitionfileddate)
                            < dateadd(day, 30, bk.loaddate)
                        then 'due in 1 - 30 days'
                        when
                            dateadd(day, 65, bk.petitionfileddate)
                            < dateadd(day, 60, bk.loaddate)
                        then 'due in 31 - 60 days'
                        else 'due in 60 + days'
                    end
                else 'error'
            end as pocslastatus,
            case
                when bk.postpetitionduedate is null
                then 'no post - petition due date'
                when
                    bk.mfrfileddate is not null
                    and bk.mfrfileddate > bk.postpetitionduedate
                then
                    case
                        when
                            bk.mfrfileddate <= case
                                when bk.postpetitionduedate is not null
                                then dateadd(d, 65, bk.postpetitionduedate)
                            end
                        then 'met'
                        else 'missed'
                    end
                when
                    case
                        when bk.postpetitionduedate is not null
                        then dateadd(d, 65, bk.postpetitionduedate)
                    end
                    < bk.loaddate
                then 'missed'
                when
                    datediff(
                        day,
                        bk.loaddate,
                        case
                            when bk.postpetitionduedate is not null
                            then dateadd(day, 65, bk.postpetitionduedate)
                        end
                    )
                    < 30
                then 'due in 1 - 30 days'
                when
                    datediff(
                        day,
                        bk.loaddate,
                        case
                            when bk.postpetitionduedate is not null
                            then dateadd(day, 65, bk.postpetitionduedate)
                        end
                    )
                    < 60
                then 'due in 31 - 60 days'
                when
                    datediff(
                        day,
                        bk.loaddate,
                        case
                            when bk.postpetitionduedate is not null
                            then dateadd(day, 65, bk.postpetitionduedate)
                        end
                    )
                    < 90
                then 'due in 61 - 90 days'
                when
                    datediff(
                        day,
                        bk.loaddate,
                        case
                            when bk.postpetitionduedate is not null
                            then dateadd(day, 65, bk.postpetitionduedate)
                        end
                    )
                    >= 90
                then 'due in 90 + days'
            end as mfrslastatus,
            concat(
                bk.loanid,
                '-',
                to_char((select max(updatedat) from raw.application.loans), 'yyyymmdd')
            ) as lu_key
        from {{ ref("bankruptcies_history") }} bk
        inner join
        {{ source("stage_reports", "me_dates") }} me
            on bk.loaddate = ifnull(me.data_date, current_date())
            and ifnull(me.me_date, current_date()) >= '12 / 31 / 2023'
        left outer join stage.reference.firmname fn on fn.rawname = bk.bkcounsel
    ),
    update_active as (
        select
            loanid,
            servicer,
            servicerloannumber,
            servicerworkflowid,
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
            loaddate,
            snapshotdate,
            closeddate,
            closedreason,
            bkfirmnormalized,
            mfrsla,
            pocsla,
            pocslastatus,
            mfrslastatus,
            lu_key,
            row_number() over (
                partition by loanid, snapshotdate
                order by loanid, snapshotdate, petitionfileddate desc
            ) as dupcnt,
            case when dupcnt > 1 then 'closed' else status end as status
        from dm_bankruptcy_temp
        where status = 'active'

    ),
    dm_bankruptcy_temp_non_active as (
        select
            loanid,
            servicer,
            servicerloannumber,
            servicerworkflowid,
            status,  -- --exclude
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
            loaddate,
            snapshotdate,
            closeddate,
            closedreason,
            bkfirmnormalized,
            mfrsla,
            pocsla,
            pocslastatus,
            mfrslastatus,
            lu_key
        from dm_bankruptcy_temp
        where status != 'active'
    ),
    union_update_active as (
        select
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
            adversaryproceedingflag,
            loaddate,
            snapshotdate,
            closeddate,
            closedreason,
            bkfirmnormalized,
            mfrsla,
            pocsla,
            pocslastatus,
            mfrslastatus,
            lu_key
        from update_active
        union all
        select
            loanid,
            servicer,
            servicerloannumber,
            servicerworkflowid,
            status,  -- --exclude
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
            loaddate,
            snapshotdate,
            closeddate,
            closedreason,
            bkfirmnormalized,
            mfrsla,
            pocsla,
            pocslastatus,
            mfrslastatus,
            lu_key
        from dm_bankruptcy_temp_non_active

    ),
    dm_bankruptcy_temp1 as (
        select
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
            adversaryproceedingflag,
            loaddate,
            snapshotdate,
            closeddate,
            closedreason,
            bkfirmnormalized,
            mfrsla,
            pocsla,
            pocslastatus,
            mfrslastatus,
            lu_key
        from union_update_active
        qualify
            row_number() over (
                partition by loanid, snapshotdate
                order by loanid, snapshotdate, petitionfileddate desc
            )
            = 1
    ),
    bankruptcies_cte as (
        select
            id,
            isdeleted,
            createdat,
            createdby,
            updatedat,
            updatedby,
            name,
            status,
            chapter,
            filingdate,
            bardate,
            postpetitiondate,
            stateoffiling,
            bkcaseno,
            courtid,
            asset,
            cramdown,
            disposition,
            caseurl,
            dismisseddate,
            dischargeddate,
            converteddate,
            dateopened,
            openedreason,
            dateclosed,
            closedreason,
            reopeneddate,
            confirmationhearingdate,
            transferreddate,
            prepaiddate,
            lienstripflag,
            sourceoffundscode,
            mfrdate,
            mfrgranteddate,
            servicerloanid,
            bkfilingdistrict,
            bkactivationdate,
            bkactivationdatetempo,
            bkdischargedate,
            bkdismisseddate,
            bkremovaldate,
            bkmfrdate,
            mfrreferredtoattorney,
            mfrfilereceivedbyattorney,
            mfrfileddate,
            bkreliefgranted,
            bkreaffirmeddate,
            bkstatus,
            pocreferredtoattorney,
            pocfileddate,
            planconfirmationdate,
            pocplanstatusdate,
            petitionfiledate,
            preplanstartdate,
            preplanenddate,
            preplannextduedate,
            preplanpaymentamount,
            preplanpaymentdue,
            preplanshortfallbalance,
            preplanpaymentfrequency,
            preplanpaymentchangeamount,
            preplanpaymentchangeeffectivedate,
            preplansuspensebalance,
            preuncollectedaccuredlatecharges,
            preplansource,
            postplanstartdate,
            postplanenddate,
            postplanpaymentduedate,
            postdelinquency,
            postplanpaymentamount,
            postplanpaidamount,
            postplanshortfallbalance,
            postplansuspencebalance,
            postplansource,
            postplanpaymentfrequency,
            postplanpaymentduedate90,
            aoduedate,
            tocreferredtoattorney,
            tocattorneyreceivedref,
            tocfiled,
            reaffirmationsenttoda,
            reaffirmationreceived,
            reaffirmationsentforfiling,
            reaffirmationfiled,
            cureresponsereffered,
            cureresponsefiled,
            borrowerintent,
            nofcaging,
            nofcdaystofileresponse,
            reaffirmationaging,
            daystofilereaffirmation,
            bkattorneyname,
            apocstatus,
            apocreferreddate,
            apocreceivedbyatty,
            apocfileddate,
            assignedperson,
            loanid,
            aitno,
            x341id,
            x341meetingdate,
            planpaymenttype
        from {{ source("raw_application", "bankruptcies") }}
        qualify row_number() over (partition by loanid order by id desc) = 1

    ),
    dm_bankruptcy_temp2 as (
        select
            p.loanid,
            p.servicer,
            p.servicerloannumber,
            p.loanid as servicerworkflowid,
            p.snapshotdate,
            'active' as status,
            b.chapter as bkchapter,
            b.bkcaseno as casenumber,
            'unknown' as pocslastatus,
            concat(
                p.loanid,
                '-',
                to_char((select max(updatedat) from raw.application.loans), 'yyyymmdd')
            ) as lu_key,
            t.bkcounsel,
            t.borrowercounsel,
            t.bkfilingstate,
            t.bkfilingdistrict,
            t.petitionfileddate,
            t.postpetitionduedate,
            t.postplanpaymentsource,
            t.prepetitionduedate,
            t.bardate,
            t.pocreferredtocounseldate,
            t.pocfileddate,
            t.amendedpocreferreddate,
            t.amendedpocfileddate,
            t.numberofmfrsfiled,
            t.mfrreferreddate,
            t.mfrfileddate,
            t.mfrentereddate,
            t.mfragreedorderflag,
            t.debtreaffirmeddate,
            t.cramdownflag,
            t.planfileddate,
            t.planconfirmationdate,
            t.planamendmentfileddate,
            t.numberofplanamendmentsfiled,
            t.transferofclaimreferreddate,
            t.transferofclaimfileddate,
            t.noticeoffinalcurereceiveddate,
            t.cureresponsereferreddate,
            t.cureresponsefileddate,
            t.borrowerintent,
            t.midcaseauditreceiveddate,
            t.midcaseauditreferreddate,
            t.midcaseauditfileddate,
            t.primarydebtorname,
            t.codebtorname,
            t.bkdischargedate,
            t.bkdismisseddate,
            t.bkremoveddate,
            t.adversaryproceedingflag,
            t.loaddate,
            t.closeddate,
            t.closedreason,
            t.bkfirmnormalized,
            t.mfrsla,
            t.pocsla,
            t.mfrslastatus
        from {{ source("edw_pbi", "loan_master_history") }} p
        left outer join
            dm_bankruptcy_temp1 t
            on t.loanid = p.loanid
            and t.snapshotdate = p.snapshotdate
        left outer join bankruptcies_cte b on p.loanid = b.loanid
        where
            p.defaultgroup in ('bankruptcy', 'bk')
            and p.isactive = 1
            and t.loanid is null
            and p.snapshotdate >= '12 / 31 / 2023'
        order by snapshotdate desc
    )
select
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
    adversaryproceedingflag,
    loaddate,
    snapshotdate,
    closeddate,
    closedreason,
    bkfirmnormalized,
    mfrsla,
    pocsla,
    pocslastatus,
    mfrslastatus,
    lu_key
from dm_bankruptcy_temp2
where snapshotdate >= dateadd(year, -1, date_trunc('month', current_date()))
