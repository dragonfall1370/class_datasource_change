SELECT
o.opportunity_ref AS job_id,
pv.lower_income::INT salary_from,
pv.upper_income::INT salary_to

FROM opportunity o
LEFT JOIN permanent_vac pv ON o.opportunity_ref = pv.opportunity_ref
LEFT JOIN temporary_vac tv ON o.opportunity_ref = tv.opportunity_ref
WHERE COALESCE(pv.lower_income, pv.upper_income) IS NOT NULL