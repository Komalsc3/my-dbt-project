{%- macro fnDays360(startDate,endDate) -%}

(CASE WHEN Day({{endDate}})=31 THEN 30 ELSE Day({{endDate}}) END) -
       (CASE WHEN Day({{startDate}})=31 THEN 30 ELSE Day({{startDate}}) END)
    + ((DATE_PART(month, {{endDate}}) + (DATE_PART(year, {{endDate}}) * 12))
        -(DATE_PART(month, {{startDate}}) + (DATE_PART(year, {{startDate}}) * 12))) * 30
{%- endmacro -%}