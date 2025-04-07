/******************************************************************************

Description: This process will build the stage_update_svc_lh_loan object in stage.STAGING, 
             to get Liquidated,active from tables SVC_LH_Inventory.
             

Change History:
Who:  Komal Singh
When: 2025-03-25
What: Initial model creation
Why:  acts as an Staging table for loan table
******************************************************************************/
{{ config(materialized="table") }}

/**********************************************************************************
This cte gets the Liquidated details after doing the some conditions for LH data
**********************************************************************************/
with
    svc_lh_loan_max_load_date_cte as (
        select loanid, max(loaddate) as max_loaddate
        from {{ source("stage_reports", "vw_svc_lh_inventory") }}
        group by loanid
    ),
    svc_lh_loan_servicer_cte as (
        select id
        from {{ source("raw_application", "actors") }}
        where contacttype = 'Servicer' and company in ('LandHome', 'Land Home')
    ),
    svc_lh_loan_liquidated_cte as (
        select
            l.id,
            'Liquidated' loanstatus,
            case
                when t.reoflag = 'TRUE'
                then 'LIQ - REO Sale'
                when t.saleheld = 'Yes' and ifnull(t.amountsuccessbid, 0) > 0
                then 'LIQ - FC Sale'
                when t.executeddeals like '%SHORT SALE%' or t.executeddeals like '%SS%'
                then 'LIQ - Short Sale'
                else 'LIQ'
            end as currentassetstatus,
            case
                when t.reoflag = 'TRUE'
                then 'REO Sale'
                when t.saleheld = 'Yes' and ifnull(t.amountsuccessbid, 0) > 0
                then 'FC Sale'
                when t.executeddeals like '%SHORT SALE%' or t.executeddeals like '%SS%'
                then 'Short Sale'
                else 'LIQ'
            end as liquidationtype,
            to_date(
                ifnull(t.salehelddate, dateadd(dd, -1, t.loaddate))
            ) as liquidationdate,
            t.amountsuccessbid as liquidationpricegross,
            t.amountsuccessbid as liquidationpricenet  -- ,
        -- dateadd(hh,-6,getdate()) as UpdatedAt,
        -- 'rUpdate_SVC_LH' as UpdatedBy
        from {{ source("raw_application", "loans") }} l
        left join
            {{ source("stage_reports", "vw_svc_lh_inventory") }} t on t.loanid = l.id
        left join
            svc_lh_loan_max_load_date_cte
            on l.id = svc_lh_loan_max_load_date_cte.loanid
            and t.loaddate = svc_lh_loan_max_load_date_cte.max_loaddate
        where
            ifnull(t.prinbal, 0) = 0
            and l.servicerid in (select id from svc_lh_loan_servicer_cte)
    ),
    /**********************************************************************************
This cte gets the active details after doing the some conditions using macro fnDays360
**********************************************************************************/
    svc_lh_loan_active_data_cte as (
        select
            l.id,
            case
                when t.reoflag = 'TRUE'
                then 'Active - REO'
                when t.bkactive = 'TRUE'
                then 'Active - BK'
                when t.fcactive = 'TRUE'
                then 'Active - FC'
                when
                    {{
                        fnDays360(
                            "t.NextPymtDue", "to_date(dateadd(dd,0,t.LoadDate))"
                        )
                    }} < 30
                then 'Active - Current'
                when
                    {{
                        fnDays360(
                            "t.NextPymtDue", "to_date(dateadd(dd,0,t.LoadDate))"
                        )
                    }} < 60
                then 'Active - 30 Days DQ'
                when
                    {{
                        fnDays360(
                            "t.NextPymtDue", "to_date(dateadd(dd,0,t.LoadDate))"
                        )
                    }} < 90
                then 'Active - 60 Days DQ'
                when
                    {{
                        fnDays360(
                            "t.NextPymtDue", "to_date(dateadd(dd,0,t.LoadDate))"
                        )
                    }} < 120
                then 'Active - 90 Days DQ'
                else 'Active - 120+ Days DQ'
            end as currentassetstatus,
            'Active' as loanstatus,
            ifnull(t.prinbal, l.upbamount) as upbamount,
            ifnull(t.prinbal, l.currentbalance) as currentbalance,
            ifnull(t.prinbal, l.currentunpaidbalance) as currentunpaidbalance,
            ifnull(t.deferredprincipal, 0)
            + ifnull(t.deferredinterest, 0) as currentdeferredbalance,
            ifnull(t.nextpymtdue, l.nextpaymentdue) as nextpaymentdue,
            ifnull(t.intrate, l.interestrate) as interestrate,
            ifnull(t.pipmt, l.paymentamount) as paymentamount,
            ifnull(t.pitiamount, l.currentpaymentpiti) as currentpaymentpiti,
            to_date(ifnull(t.lastpymtreceived, l.lastpaymentdate)) as lastpaymentdate,
            case
                when ifnull(t.escrowbal, l.currentescrowadvancebalance) > 0
                then ifnull(t.escrowbal, l.currentescrowadvancebalance)
                else 0
            end currentescrowbalance,
            case
                when ifnull(t.escrowbal, l.currentescrowadvancebalance) < 0
                then ifnull(t.escrowbal, l.currentescrowadvancebalance)
                else 0
            end currentescrowadvancebalance,
            ifnull(
                - t.corporateadvancebalance, l.currentcorporateadvancebalance
            ) as currentcorporateadvancebalance,
            ifnull(
                t.latechargebalance, l.currentlatefeebalance
            ) as currentlatefeebalance,
            ifnull(
                case when t.armloan = 'No' then 'FIXED' else 'ARM' end,
                l.amortizationtype
            ) as amortizationtype,
            coalesce(t.maturitydate, l.maturitydate) as maturitydate,
            ifnull(
                case when t.loantype = 'Conventional' then 'CONV' end, l.loantype
            ) as loantype  -- ,
        -- dateadd(hh, -6, getdate()) as UpdatedAt,
        -- 'rupdate_SVC_LH' as UpdatedBy
        from {{ source("raw_application", "loans") }} l
        left join
            {{ source("stage_reports", "vw_svc_lh_inventory") }} t on t.loanid = l.id
        left join
            svc_lh_loan_max_load_date_cte
            on l.id = svc_lh_loan_max_load_date_cte.loanid
            and t.loaddate = svc_lh_loan_max_load_date_cte.max_loaddate
        where
            ifnull(t.prinbal, 0) > 0
            and l.servicerid in (select id from svc_lh_loan_servicer_cte)
    ),
    /**********************************************************************************
This cte gets the active details based on above cte using macro fnDays360
**********************************************************************************/
    svc_lh_loan_active_cte as (
        select
            l.id,
            ad.currentassetstatus,
            ad.loanstatus,
            ad.upbamount,
            ad.currentbalance,
            ad.currentunpaidbalance,
            ad.currentdeferredbalance,
            ad.nextpaymentdue,
            ad.interestrate,
            ad.paymentamount,
            ad.currentpaymentpiti,
            ad.lastpaymentdate,
            ad.currentescrowbalance,
            ad.currentescrowadvancebalance,
            ad.currentcorporateadvancebalance,
            ad.currentlatefeebalance,
            ad.amortizationtype,
            ad.maturitydate,
            ad.loantype,
            case
                when ad.currentassetstatus = 'Active - REO'
                then 'REO'
                when
                    ad.currentassetstatus like 'Active%'
                    and {{
                        fnDays360(
                            "ad.NextPaymentDue",
                            "to_date(dateadd(dd,0,t.LoadDate))",
                        )
                    }} < 60
                then 'PL'
                when
                    ad.currentassetstatus like 'Active%'
                    and {{
                        fnDays360(
                            "ad.NextPaymentDue",
                            "to_date(dateadd(dd,0,t.LoadDate))",
                        )
                    }} >= 60
                then 'NPL'
                else l.currentassettype
            end as currentassettype
        from {{ source("raw_application", "loans") }} l
        left join svc_lh_loan_active_data_cte ad on ad.id = l.id
        left join
            {{ source("stage_reports", "vw_svc_lh_inventory") }} t on t.loanid = l.id
        left join
            svc_lh_loan_max_load_date_cte
            on l.id = svc_lh_loan_max_load_date_cte.loanid
            and t.loaddate = svc_lh_loan_max_load_date_cte.max_loaddate
        where
            l.currentassetstatus not like 'LIQ%'
            and l.servicerid in (select id from svc_lh_loan_servicer_cte)
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
            null as upbamount,
            null as currentbalance,
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
            null as amortizationtype,
            null as maturitydate,
            null as loantype,
            null as currentassettype,
            null as paymenthistory,
            null as mipercentage,
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
            null as micompanyid
        from svc_lh_loan_liquidated_cte liq
        union all
        select
            actv.id,
            actv.currentassetstatus,
            actv.loanstatus,
            null as liquidationtype,
            null as liquidationdate,
            null as liquidationpricegross,
            null as liquidationpricenet,
            actv.upbamount,
            actv.currentbalance,
            actv.currentunpaidbalance,
            actv.currentdeferredbalance,
            actv.nextpaymentdue,
            actv.interestrate,
            actv.paymentamount,
            actv.currentpaymentpiti,
            actv.lastpaymentdate,
            actv.currentescrowbalance,
            actv.currentescrowadvancebalance,
            actv.currentcorporateadvancebalance,
            actv.currentlatefeebalance,
            actv.amortizationtype,
            actv.maturitydate,
            actv.loantype,
            actv.currentassettype,
            null as paymenthistory,
            null as mipercentage,
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
            null as micompanyid
        from svc_lh_loan_active_cte actv
    )
/**********************************************************************************
Final select
**********************************************************************************/
select
    union_liquidation_active_data.id,
    union_liquidation_active_data.currentassetstatus,
    union_liquidation_active_data.loanstatus,
    union_liquidation_active_data.liquidationtype,
    union_liquidation_active_data.liquidationdate,
    union_liquidation_active_data.liquidationpricegross,
    union_liquidation_active_data.liquidationpricenet,
    union_liquidation_active_data.upbamount,
    union_liquidation_active_data.currentbalance,
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
    union_liquidation_active_data.amortizationtype,
    union_liquidation_active_data.maturitydate,
    union_liquidation_active_data.currentassettype,
    union_liquidation_active_data.paymenthistory,
    union_liquidation_active_data.mipercentage,
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
    union_liquidation_active_data.loantype,
    dateadd(hh, -6, getdate()) as updatedat,
    'rupdate_SVC_LH' as updatedby
from union_liquidation_active_data
