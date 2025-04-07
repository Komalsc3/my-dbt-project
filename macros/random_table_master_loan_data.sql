{%- macro random_table_master_loan_data(table_name) -%}

select
          l.id
          ,l.ServicerLoanNumber
        , ld.* exclude(id)
		, l.Borrower1Id
		, l.Borrower2Id 
FROM {{table_name}} ld
left join
{{ source("raw_application", "loans") }} l
on ld.LoanId = l.id

{%- endmacro -%}