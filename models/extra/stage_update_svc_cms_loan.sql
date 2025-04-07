/******************************************************************************

Description: This process will build the stage_update_svc_cms_loan object in stage.STAGING, 
             to get Liquidated,active from tables vw_svc_cms_loandetail,vw_svc_cms_liquidations.
             

Change History:
Who:  Komal Singh
When: 2025-03-25
What: Initial model creation
Why:  acts as an Staging table for loan table
******************************************************************************/
{{ config(materialized="table") }}

/**********************************************************************************
This cte gets the Liquidated details after doing the some conditions for CMS data
**********************************************************************************/
with
    liquidations_cte as (
        select
            l.id,
            'Liquidated' as loanstatus,
            case
                when t.liqstatus like '%REO%SALE%'
                then 'LIQ - REO Sale'
                when
                    t.liqstatus like '%PAID%IN%FULL%'
                    or t.liqstatus like '%PIF%'
                    or t.liqstatus like '%FULL%PAYOFF%'
                    or t.liqstatus like '%LOAN%PAID%OFF%'
                then 'LIQ - Paid In Full'
                when t.liqstatus like '%SHORT%SALE%' or t.liqstatus like '%Prior SS%'
                then 'LIQ - Short Sale'
                when t.liqstatus like '%SHORT%PAYOFF%'
                then 'LIQ - Short Payoff'
                when
                    t.liqstatus like '%FORECLOSURE THIRD PARTY SALE%'
                    or t.liqstatus like '%FC SALE%'
                then 'LIQ - FC Sale'
                when t.liqstatus like '%FHA%CLAIM%'
                then 'LIQ - FHA Claim'
                when t.liqstatus like '%VA%CLAIM%'
                then 'LIQ - VA Claim'
                when t.liqstatus like '%USDA%CLAIM%'
                then 'LIQ - USDA Claim'
                when t.liqstatus like '%PMI%CLAIM%'
                then 'LIQ - PMI Claim'
                when t.liqstatus like '%Title%Claim%'
                then 'LIQ - Title Claim'
                when t.liqstatus like '%CHARGE%OFF%'
                then 'LIQ - Charge Off'
                when t.liqstatus like '%Hazard%Claim%'
                then 'LIQ - Hazard Claim'
                when t.liqstatus like '%REPURCHASE%'
                then 'LIQ - Repurchase'
                when
                    t.liqstatus like '%THIRD%PARTY%SALE%'
                    or t.liqstatus like '%LOAN%SALE%'
                    or t.liqstatus like '%REPOOL%'
                then 'LIQ - Third Party Sale'
                when t.liqstatus in ('VASP')
                then 'LIQ - VA Servicing Purchase'
                else 'LIQ'
            end as currentassetstatus,
            case
                when t.liqstatus like '%REO%SALE%'
                then 'REO Sale'
                when
                    t.liqstatus like '%PAID%IN%FULL%'
                    or t.liqstatus like '%PIF%'
                    or t.liqstatus like '%FULL%PAYOFF%'
                    or t.liqstatus like '%LOAN%PAID%OFF%'
                then 'Paid In Full'
                when t.liqstatus like '%SHORT%SALE%' or t.liqstatus like '%Prior SS%'
                then 'Short Sale'
                when t.liqstatus like '%SHORT%PAYOFF%'
                then 'Short Payoff'
                when
                    t.liqstatus like '%FORECLOSURE THIRD PARTY SALE%'
                    or t.liqstatus like '%FC SALE%'
                then 'FC Sale'
                when t.liqstatus like '%FHA%CLAIM%'
                then 'FHA Claim'
                when t.liqstatus like '%VA%CLAIM%'
                then 'VA Claim'
                when t.liqstatus like '%USDA%CLAIM%'
                then 'USDA Claim'
                when t.liqstatus like '%PMI%CLAIM%'
                then 'PMI Claim'
                when t.liqstatus like '%Title%Claim%'
                then 'Title Claim'
                when t.liqstatus like '%CHARGE%OFF%'
                then 'Charge Off'
                when t.liqstatus like '%Hazard%Claim%'
                then 'Hazard Claim'
                when t.liqstatus like '%REPURCHASE%'
                then 'Repurchase'
                when
                    t.liqstatus like '%THIRD%PARTY%SALE%'
                    or t.liqstatus like '%LOAN%SALE%'
                    or t.liqstatus like '%REPOOL%'
                then 'Third Party Sale'
                when t.liqstatus in ('VASP')
                then 'VA Servicing Purchase'
                else 'LIQ'
            end as liquidationtype,
            case
                when t.liquidationdate is not null
                then t.liquidationdate
                else to_date(dateadd(dd, -1, t.loaddate))
            end as liquidationdate,
            t.liqprice as liquidationpricegross,
            t.netproceeds as liquidationpricenet,
            dateadd(hh, -6, current_date()) as updatedat,
            'rUpdate_SVC_CMS(Liquidation)' as updatedby
        from {{ source("raw_application", "loans") }} l
        left join
            {{ source("stage_reports", "vw_svc_cms_liquidations") }} t
            on t.loanid = l.id
        where
            t.loaddate = to_date(dateadd(hh, -6, current_date()))
            and (
                ifnull(l.currentassetstatus, 'x') not like 'LIQ%'
                or l.currentassetstatus in ('LIQ')
            )
    ),
    /**********************************************************************************
This cte gets the active details based on above cte using macro fnDays360
**********************************************************************************/
    loandetail1_cte as (
        select
            l.id,
            null as currentassettype,
            case
                when t.currentbalance1 is null
                then 'Pending Servicing Transfer'
                when t.reoflag = 'Y'
                then 'Active - REO'
                when t.currentbalance1 = 0
                then 'LIQ'
                when t.bkflag = 'Y'
                then 'Active - BK'
                when t.bkflag is null and t.fiservdelqstatusmba = 'BK'
                then 'Active - BK'
                when t.fcflag = 'Y'
                then 'Active - FC'
                when t.fcflag is null and t.fiservdelqstatusmba = 'FC'
                then 'Active - FC'
                when
                    {{
                        fnDays360(
                            "t.DatePaymentDue", "to_date(dateadd(dd,0,t.LoadDate))"
                        )
                    }} < 30
                then 'Active - Current'
                when
                    {{
                        fnDays360(
                            "t.DatePaymentDue", "to_date(dateadd(dd,0,t.LoadDate))"
                        )
                    }} < 60
                then 'Active - 30 Days DQ'
                when
                    {{
                        fnDays360(
                            "t.DatePaymentDue", "to_date(dateadd(dd,0,t.LoadDate))"
                        )
                    }} < 90
                then 'Active - 60 Days DQ'
                when
                    {{
                        fnDays360(
                            "t.DatePaymentDue", "to_date(dateadd(dd,0,t.LoadDate))"
                        )
                    }} < 120
                then 'Active - 90 Days DQ'
                else 'Active - 120+ Days DQ'
            end as currentassetstatus,
            'Active' as loanstatus,
            ifnull(t.currentbalance1, l.upbamount) as upbamount,
            ifnull(t.currentbalance1, l.currentunpaidbalance) as currentunpaidbalance,
            ifnull(t.deferredprincipalamt1, 0)
            + ifnull(t.deferredinterestamt1, 0) as currentdeferredbalance,
            ifnull(t.datepaymentdue, l.nextpaymentdue) as nextpaymentdue,
            ifnull(t.interestrate1 / 100.00, l.interestrate) as interestrate,
            ifnull(t.payemntpicurrent, l.paymentamount) as paymentamount,
            case
                when ifnull(t.payemntpicurrent, 0) + ifnull(t.paymentticurrent, 0) = 0
                then l.currentpaymentpiti
                else ifnull(t.payemntpicurrent, 0) + ifnull(t.paymentticurrent, 0)
            end as currentpaymentpiti,
            to_date(ifnull(t.datepaymentlast, l.lastpaymentdate)) as lastpaymentdate,
            case
                when ifnull(t.escrowbalance, l.currentescrowadvancebalance) > 0
                then ifnull(t.escrowbalance, l.currentescrowadvancebalance)
                else 0
            end as currentescrowbalance,
            case
                when ifnull(t.escrowbalance, l.currentescrowadvancebalance) < 0
                then ifnull(t.escrowbalance, l.currentescrowadvancebalance)
                else 0
            end as currentescrowadvancebalance,
            ifnull(
                t.totalcorpadvanced, l.currentcorporateadvancebalance
            ) as currentcorporateadvancebalance,
            ifnull(
                t.latefeeuncollectedamount, l.currentlatefeebalance
            ) as currentlatefeebalance,
            left(ifnull(t.paystring, l.paymenthistory), 12) as paymenthistory,
            ifnull(t.amorttype, l.amortizationtype) as amortizationtype,
            t.mipercentage as mipercentage,
            coalesce(t.dateballoon, t.datematurity, l.maturitydate) as maturitydate,
            case
                when ifnull(t.mipercentage, 0) > 0
                then
                    case
                        when t.micompany = 'FHA'
                        then 'FHA'
                        when t.micompany = 'USDA'
                        then 'USDA'
                        when t.micompany = 'VA'
                        then 'VA'
                        else 'PMI'
                    end
                else
                    ifnull(
                        case
                            when t.loan_typecode = 1
                            then 'CONV'
                            when t.loan_typecode = 2
                            then 'FHA'
                            when t.loan_typecode = 3
                            then 'VA'
                            when t.loan_typecode = 4
                            then 'HELOC'
                            when t.loan_typecode = 8 and t.micompany = 'USDA'
                            then 'USDA'
                            when t.loan_typecode = 8
                            then 'PMI'
                            else null
                        end,
                        l.loantype
                    )
            end as loantype,
            t.dispositionpath as investmentstrategy,
            ifnull(t.lsmit_status, l.lossmitstatus) as lossmitstatus,
            ifnull(t.lsmit_statusdate, l.lossmitstatusdate) as lossmitstatusdate,
            case
                when l.lossmitflag is not null and t.loss_mit_flag is null
                then 'Removed'
                else t.loss_mit_flag
            end as lossmitflag,
            t.nexttaxpaymentdate as nexttaxpaymentdate,
            t.nexttaxpaymentamt as nexttaxpaymentamount,
            t.nextinsurancepaymentdate as nextinsurancepaymentdate,
            t.nextinsurancepaymentamt as nextinsurancepaymentamount,
            t.breach_letter_expire_date as breachletterexpiredate,
            t.breach_letter_sent_date as breachlettersentdate,
            t.breach_sent_next_due_date as breachletternextduedate,
            ifnull(t.servicingtransferdate, l.transferasofdate) as transferasofdate,
            ifnull(t.servicingtransferdate, l.transferdate) as transferdate,
            dateadd(hh, -6, current_date()) as updatedat,
            'rUpdate_SVC_CMS(LoanDetail1)' as updatedby
        from {{ source("raw_application", "loans") }} l
        left join
            {{ source("stage_reports", "vw_svc_cms_loandetail") }} t on t.loanid = l.id
        where
            t.loaddate = to_date(dateadd(hh, -6, current_date()))
            and (
                ifnull(l.currentassetstatus, 'x') not like 'LIQ%'
                and (
                    ifnull(l.currentassetstatus, 'x') <> case
                        when t.currentbalance1 is null
                        then 'Pending Servicing Transfer'
                        when t.reoflag = 'Y'
                        then 'Active - REO'
                        when t.currentbalance1 = 0
                        then 'LIQ'
                        when t.bkflag = 'Y'
                        then 'Active - BK'
                        when t.fcflag = 'Y'
                        then 'Active - FC'
                        when
                            {{
                                fnDays360(
                                    "t.DatePaymentDue",
                                    "to_date(dateadd(dd,0,t.LoadDate))",
                                )
                            }} < 30
                        then 'Active - Current'
                        when
                            {{
                                fnDays360(
                                    "t.DatePaymentDue",
                                    "to_date(dateadd(dd,0,t.LoadDate))",
                                )
                            }} < 60
                        then 'Active - 30 Days DQ'
                        when
                            {{
                                fnDays360(
                                    "t.DatePaymentDue",
                                    "to_date(dateadd(dd,0,t.LoadDate))",
                                )
                            }} < 90
                        then 'Active - 60 Days DQ'
                        when
                            {{
                                fnDays360(
                                    "t.DatePaymentDue",
                                    "to_date(dateadd(dd,0,t.LoadDate))",
                                )
                            }} < 120
                        then 'Active - 90 Days DQ'
                        else 'Active - 120+ Days DQ'
                    end
                    or ifnull(l.upbamount, 0)
                    <> ifnull(t.currentbalance1, ifnull(l.upbamount, 0))
                    or ifnull(l.currentdeferredbalance, 0)
                    <> ifnull(t.deferredprincipalamt1, 0)
                    + ifnull(t.deferredinterestamt1, 0)
                    or to_date(ifnull(l.nextpaymentdue, '1/1/1980')) <> to_date(
                        ifnull(ifnull(t.datepaymentdue, l.nextpaymentdue), '1/1/1980')
                    )
                    or ifnull(round(l.interestrate, 6), 0) <> coalesce(
                        round(t.interestrate1 / 100.00, 6), round(l.interestrate, 6), 0
                    )
                    or ifnull(l.paymentamount, 0)
                    <> ifnull(t.payemntpicurrent, ifnull(l.paymentamount, 0))
                    or ifnull(l.currentpaymentpiti, 0) <> case
                        when
                            ifnull(t.payemntpicurrent, 0)
                            + ifnull(t.paymentticurrent, 0)
                            = 0
                        then ifnull(l.currentpaymentpiti, 0)
                        else
                            ifnull(t.payemntpicurrent, 0)
                            + ifnull(t.paymentticurrent, 0)
                    end
                    or to_date(ifnull(l.lastpaymentdate, '1/1/1980')) <> to_date(
                        ifnull(t.datepaymentlast, ifnull(l.lastpaymentdate, '1/1/1980'))
                    )
                    or ifnull(l.currentescrowadvancebalance, 0)
                    <> ifnull(t.escrowbalance, ifnull(l.currentescrowadvancebalance, 0))
                    or ifnull(l.currentcorporateadvancebalance, 0) <> ifnull(
                        t.totalcorpadvanced, ifnull(l.currentcorporateadvancebalance, 0)
                    )
                    or ifnull(l.currentlatefeebalance, 0) <> ifnull(
                        t.latefeeuncollectedamount, ifnull(l.currentlatefeebalance, 0)
                    )
                    or ifnull(l.paymenthistory, 'x')
                    <> ifnull(left(t.paystring, 12), ifnull(l.paymenthistory, 'x'))
                    or ifnull(l.amortizationtype, 'x')
                    <> ifnull(t.amorttype, ifnull(l.amortizationtype, 'x'))
                    or ifnull(l.mipercentage, 0) <> ifnull(t.mipercentage, 0)
                    or ifnull(l.maturitydate, '1/1/1980') <> ifnull(
                        to_date(
                            coalesce(t.dateballoon, t.datematurity, l.maturitydate)
                        ),
                        '1/1/1980'
                    )
                    or ifnull(l.loantype, 'x') <> ifnull(
                        ifnull(
                            case
                                when t.loan_typecode = 1
                                then 'CONV'
                                when t.loan_typecode = 2
                                then 'FHA'
                                when t.loan_typecode = 3
                                then 'VA'
                                when t.loan_typecode = 4
                                then 'HELOC'
                                when t.loan_typecode = 8
                                then 'PMI'
                                else null
                            end,
                            l.loantype
                        ),
                        'x'
                    )
                    or ifnull(l.investmentstrategy, 'x')
                    <> ifnull(t.dispositionpath, 'x')
                    or ifnull(l.lossmitstatus, 'x')
                    <> ifnull(ifnull(t.lsmit_status, l.lossmitstatus), 'x')
                    or ifnull(l.lossmitstatusdate, '1/1/1980')
                    <> coalesce(t.lsmit_statusdate, l.lossmitstatusdate, '1/1/1980')
                    or ifnull(l.lossmitflag, 'x') <> case
                        when l.lossmitflag is not null and t.loss_mit_flag is null
                        then 'Removed'
                        else ifnull(t.loss_mit_flag, 'x')
                    end
                    or ifnull(l.lastpaymentdate, '1/1/1980')
                    <> ifnull(t.datepaymentlast, ifnull(l.lastpaymentdate, '1/1/1980'))
                    or ifnull(l.nextpaymentdue, '1/1/1980') <> ifnull(
                        ifnull(t.datepaymentdue, l.nextpaymentdue),
                        ifnull(l.nextpaymentdue, '1/1/1980')
                    )
                    or ifnull(l.nexttaxpaymentdate, '1/1/1980')
                    <> ifnull(t.nexttaxpaymentdate, '1/1/1980')
                    or ifnull(l.nexttaxpaymentamount, 0)
                    <> ifnull(t.nexttaxpaymentamt, 0)
                    or ifnull(l.nextinsurancepaymentdate, '1/1/1980')
                    <> ifnull(t.nextinsurancepaymentdate, '1/1/1980')
                    or ifnull(l.nextinsurancepaymentamount, 0)
                    <> ifnull(t.nextinsurancepaymentamt, 0)
                    or ifnull(l.breachletterexpiredate, '1/1/1980')
                    <> ifnull(t.breach_letter_expire_date, '1/1/1980')
                    or ifnull(l.breachlettersentdate, '1/1/1980')
                    <> ifnull(t.breach_letter_sent_date, '1/1/1980')
                    or ifnull(l.breachletternextduedate, '1/1/1980')
                    <> ifnull(t.breach_sent_next_due_date, '1/1/1980')
                )
                or ifnull(l.transferasofdate, '1/1/1980') <> ifnull(
                    t.servicingtransferdate, ifnull(l.transferasofdate, '1/1/1980')
                )
            )
    ),
    /**********************************************************************************
This cte gets the active MICompanyId and CurrentAssetType details based on above cte using macro fnDays360
**********************************************************************************/
    loandetail2_cte as (
        select
            loandetail1_cte.id,
            case
                when loandetail1_cte.currentassetstatus = 'Active - REO'
                then 'REO'
                when
                    loandetail1_cte.currentassetstatus like 'Active%'
                    and {{
                        fnDays360(
                            "LoanDetail1_cte.NextPaymentDue",
                            "to_date(dateadd(dd,0,t.LoadDate))",
                        )
                    }} < 60
                then 'PL'
                when
                    loandetail1_cte.currentassetstatus like 'Active%'
                    and {{
                        fnDays360(
                            "LoanDetail1_cte.NextPaymentDue",
                            "to_date(dateadd(dd,0,t.LoadDate))",
                        )
                    }} >= 60
                then 'NPL'
                else l.currentassettype
            end as currentassettype,
            case
                when
                    (
                        t.loaddate = to_date(dateadd(hh, -6, current_date()))
                        and ifnull(l.currentassetstatus, 'x') not like 'LIQ%'
                        and ifnull(i.company, 'x') <> ifnull(t.micompany, 'x')
                    )
                then o.id
            end as micompanyid,
            dateadd(hh, -6, current_date()) as updatedat,
            'rupdate_SVC_CMS(LoanDetail2)' as updatedby
        from {{ source("raw_application", "loans") }} l
        left join loandetail1_cte on loandetail1_cte.id = l.id
        left join
            {{ source("stage_reports", "vw_svc_cms_loandetail") }} t
            on loandetail1_cte.id = t.loanid
        left outer join
            {{ source("raw_application", "actors") }} i on l.micompanyid = i.id
        left outer join
            {{ source("raw_application", "actors") }} o
            on o.id = (
                select top 1 o.id
                from {{ source("raw_application", "actors") }} o
                where t.micompany = o.company
            )
        where
            t.loaddate = to_date(dateadd(hh, -6, current_date()))
            and l.currentassetstatus not like 'LIQ%'
            and l.currentassettype <> case
                when l.currentassetstatus = 'Active - REO'
                then 'REO'
                when
                    l.currentassetstatus like 'Active%'
                    and {{
                        fnDays360(
                            "l.NextPaymentDue", "to_date(dateadd(dd,0,t.LoadDate))"
                        )
                    }} < 60
                then 'PL'
                when
                    l.currentassetstatus like 'Active%'
                    and {{
                        fnDays360(
                            "l.NextPaymentDue", "to_date(dateadd(dd,0,t.LoadDate))"
                        )
                    }} >= 60
                then 'NPL'
                else l.currentassettype
            end
    ),
    /**********************************************************************************
This cte gets the below details based on above cte using union all
**********************************************************************************/
    union_liquidation_active_data as (
        select
            liq.id,
            liq.currentassetstatus,
            liq.loanstatus,
            liq.liquidationtype,
            liq.liquidationdate,
            liq.liquidationpricegross,
            liq.liquidationpricenet,
            null as currentassettype,
            null as upbamount,
            null as currentunpaidbalance,
            null as currentdeferredbalance,
            null as nextpaymentdue,
            null as interestrate,
            null as paymentamount,
            null as currentpaymentpiti,
            null as lastpaymentdate,
            null as currentescrowbalance,
            null as currentescrowadvancebalance,
            null as currentcorporateadvancebalance,
            null as currentlatefeebalance,
            null as paymenthistory,
            null as amortizationtype,
            null as mipercentage,
            null as maturitydate,
            null as loantype,
            null as investmentstrategy,
            null as lossmitstatus,
            null as lossmitstatusdate,
            null as lossmitflag,
            null as nexttaxpaymentdate,
            null as nexttaxpaymentamount,
            null as nextinsurancepaymentdate,
            null as nextinsurancepaymentamount,
            null as breachletterexpiredate,
            null as breachlettersentdate,
            null as breachletternextduedate,
            null as transferasofdate,
            null as transferdate,
            null as micompanyid,
            liq.updatedat,
            liq.updatedby
        from liquidations_cte liq
        union all
        select
            actv1.id,
            actv1.currentassetstatus,
            actv1.loanstatus,
            null as liquidationtype,
            null as liquidationdate,
            null as liquidationpricegross,
            null as liquidationpricenet,
            actv1.currentassettype,
            actv1.upbamount,
            actv1.currentunpaidbalance,
            actv1.currentdeferredbalance,
            actv1.nextpaymentdue,
            actv1.interestrate,
            actv1.paymentamount,
            actv1.currentpaymentpiti,
            actv1.lastpaymentdate,
            actv1.currentescrowbalance,
            actv1.currentescrowadvancebalance,
            actv1.currentcorporateadvancebalance,
            actv1.currentlatefeebalance,
            actv1.paymenthistory,
            actv1.amortizationtype,
            actv1.mipercentage,
            actv1.maturitydate,
            actv1.loantype,
            actv1.investmentstrategy,
            actv1.lossmitstatus,
            actv1.lossmitstatusdate,
            actv1.lossmitflag,
            actv1.nexttaxpaymentdate,
            actv1.nexttaxpaymentamount,
            actv1.nextinsurancepaymentdate,
            actv1.nextinsurancepaymentamount,
            actv1.breachletterexpiredate,
            actv1.breachlettersentdate,
            actv1.breachletternextduedate,
            actv1.transferasofdate,
            actv1.transferdate,
            null as micompanyid,
            actv1.updatedat,
            actv1.updatedby
        from loandetail1_cte actv1
        union all
        select
            actv2.id,
            null as currentassetstatus,
            null as loanstatus,
            null as liquidationtype,
            null as liquidationdate,
            null as liquidationpricegross,
            null as liquidationpricenet,
            actv2.currentassettype,
            null as upbamount,
            null as currentunpaidbalance,
            null as currentdeferredbalance,
            null as nextpaymentdue,
            null as interestrate,
            null as paymentamount,
            null as currentpaymentpiti,
            null as lastpaymentdate,
            null as currentescrowbalance,
            null as currentescrowadvancebalance,
            null as currentcorporateadvancebalance,
            null as currentlatefeebalance,
            null as paymenthistory,
            null as amortizationtype,
            null as mipercentage,
            null as maturitydate,
            null as loantype,
            null as investmentstrategy,
            null as lossmitstatus,
            null as lossmitstatusdate,
            null as lossmitflag,
            null as nexttaxpaymentdate,
            null as nexttaxpaymentamount,
            null as nextinsurancepaymentdate,
            null as nextinsurancepaymentamount,
            null as breachletterexpiredate,
            null as breachlettersentdate,
            null as breachletternextduedate,
            null as transferasofdate,
            null as transferdate,
            actv2.micompanyid,
            actv2.updatedat,
            actv2.updatedby
        from loandetail2_cte actv2
    ),
    not_on_servicer_file_status_cte as (
        select
            l.id,
            'Not on Servicer File' as currentassetstatus,
            dateadd(hh, -6, current_date()) as updatedat,
            'rupdate_SVC_CMS' as updatedby
        from {{ source("raw_application", "loans") }} l
        left join
            union_liquidation_active_data on union_liquidation_active_data.id = l.id
        left outer join
            {{ source("stage_reports", "vw_svc_cms_loandetail") }} t
            on l.id = t.loanid
            and t.loaddate = to_date(dateadd(hh, -6, current_date()))
        left outer join
            {{ source("stage_reports", "vw_svc_cms_loandetail") }} t1
            on t1.id = (
                select t1.id
                from {{ source("stage_reports", "vw_svc_cms_loandetail") }} t1
                where l.id = t1.loanid
            )
        where
            l.servicerid = 78859
            and l.currentassetstatus not like 'LIQ%'
            and t.loanid is null
            and t1.loanid is not null
    ),
    /**********************************************************************************
This cte gets the below details based on above cte using union all
**********************************************************************************/
    final as (
        select
            union_liquidation_active_data.id,
            union_liquidation_active_data.currentassetstatus,
            union_liquidation_active_data.loanstatus,
            union_liquidation_active_data.liquidationtype,
            union_liquidation_active_data.liquidationdate,
            union_liquidation_active_data.liquidationpricegross,
            union_liquidation_active_data.liquidationpricenet,
            union_liquidation_active_data.currentassettype,
            union_liquidation_active_data.upbamount,
            union_liquidation_active_data.currentunpaidbalance,
            union_liquidation_active_data.currentdeferredbalance,
            union_liquidation_active_data.nextpaymentdue,
            union_liquidation_active_data.interestrate,
            union_liquidation_active_data.paymentamount,
            union_liquidation_active_data.currentpaymentpiti,
            union_liquidation_active_data.lastpaymentdate,
            union_liquidation_active_data.currentescrowbalance,
            union_liquidation_active_data.currentescrowadvancebalance,
            union_liquidation_active_data.currentcorporateadvancebalance,
            union_liquidation_active_data.currentlatefeebalance,
            union_liquidation_active_data.paymenthistory,
            union_liquidation_active_data.amortizationtype,
            union_liquidation_active_data.mipercentage,
            union_liquidation_active_data.maturitydate,
            union_liquidation_active_data.loantype,
            union_liquidation_active_data.investmentstrategy,
            union_liquidation_active_data.lossmitstatus,
            union_liquidation_active_data.lossmitstatusdate,
            union_liquidation_active_data.lossmitflag,
            union_liquidation_active_data.nexttaxpaymentdate,
            union_liquidation_active_data.nexttaxpaymentamount,
            union_liquidation_active_data.nextinsurancepaymentdate,
            union_liquidation_active_data.nextinsurancepaymentamount,
            union_liquidation_active_data.breachletterexpiredate,
            union_liquidation_active_data.breachlettersentdate,
            union_liquidation_active_data.breachletternextduedate,
            union_liquidation_active_data.transferasofdate,
            union_liquidation_active_data.transferdate,
            union_liquidation_active_data.micompanyid,
            union_liquidation_active_data.updatedat,
            union_liquidation_active_data.updatedby
        from union_liquidation_active_data
        union all
        select
            not_on_servicer_file_status_cte.id,
            not_on_servicer_file_status_cte.currentassetstatus,
            null as loanstatus,
            null as liquidationtype,
            null as liquidationdate,
            null as liquidationpricegross,
            null as liquidationpricenet,
            null as currentassettype,
            null as upbamount,
            null as currentunpaidbalance,
            null as currentdeferredbalance,
            null as nextpaymentdue,
            null as interestrate,
            null as paymentamount,
            null as currentpaymentpiti,
            null as lastpaymentdate,
            null as currentescrowbalance,
            null as currentescrowadvancebalance,
            null as currentcorporateadvancebalance,
            null as currentlatefeebalance,
            null as paymenthistory,
            null as amortizationtype,
            null as mipercentage,
            null as maturitydate,
            null as loantype,
            null as investmentstrategy,
            null as lossmitstatus,
            null as lossmitstatusdate,
            null as lossmitflag,
            null as nexttaxpaymentdate,
            null as nexttaxpaymentamount,
            null as nextinsurancepaymentdate,
            null as nextinsurancepaymentamount,
            null as breachletterexpiredate,
            null as breachlettersentdate,
            null as breachletternextduedate,
            null as transferasofdate,
            null as transferdate,
            null as micompanyid,
            not_on_servicer_file_status_cte.updatedat,
            not_on_servicer_file_status_cte.updatedby
        from not_on_servicer_file_status_cte
    )
/**********************************************************************************
Final select
**********************************************************************************/
select
    final.id,
    final.currentassetstatus,
    final.loanstatus,
    final.liquidationtype,
    final.liquidationdate,
    final.liquidationpricegross,
    final.liquidationpricenet,
    final.currentassettype,
    final.upbamount,
    final.currentunpaidbalance,
    final.currentdeferredbalance,
    final.nextpaymentdue,
    final.interestrate,
    final.paymentamount,
    final.currentpaymentpiti,
    final.lastpaymentdate,
    final.currentescrowbalance,
    final.currentescrowadvancebalance,
    final.currentcorporateadvancebalance,
    final.currentlatefeebalance,
    final.paymenthistory,
    final.amortizationtype,
    final.mipercentage,
    final.maturitydate,
    final.loantype,
    final.investmentstrategy,
    final.lossmitstatus,
    final.lossmitstatusdate,
    final.lossmitflag,
    final.nexttaxpaymentdate,
    final.nexttaxpaymentamount,
    final.nextinsurancepaymentdate,
    final.nextinsurancepaymentamount,
    final.breachletterexpiredate,
    final.breachlettersentdate,
    final.breachletternextduedate,
    final.transferasofdate,
    final.transferdate,
    final.micompanyid,
    final.updatedat,
    final.updatedby
from final
