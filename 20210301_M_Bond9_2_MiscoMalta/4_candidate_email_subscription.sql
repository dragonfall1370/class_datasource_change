with 
--EMAIL ADDRESS
email as (select uniqueid
	, "4 ref no numeric" ref_id
	, "33 e-mail alphanumeric" org_email
	, a.email
	, case when position('--' in email) > 0 or position('?' in email) > 0 or email = '' then NULL
		else trim('''' from trim(email)) end as new_email
	, a.splitrn
	from f01, unnest(string_to_array("33 e-mail alphanumeric", '~')) with ordinality as a(email, splitrn)
	where "33 e-mail alphanumeric" ilike '%_@_%.__%')
	
, person_email as (select uniqueid, ref_id, org_email, new_email, splitrn
	, row_number() over(partition by new_email order by ref_id) as rn --distinct email if more than once
	, row_number() over(partition by uniqueid order by splitrn, new_email desc) as person_rn --distinct if candidates may have more than 1 email
	from email
	where new_email is not NULL
	) --select * from person_email


--MAIN SCRIPT
select c.uniqueid as cand_ext_id
--CAND INFO
, case when pe.rn > 1 then pe.rn || '_' || pe.new_email
		else coalesce(pe.new_email, c."4 ref no numeric" || '_candidate@noemail.com') end as "candidate-email"
, "74 maillist yn"
, case when c."74 maillist yn" = 'Y' then 1 else 0 end as email_subscription
from f01 c
left join (select * from person_email where person_rn=1) pe on pe.uniqueid = c.uniqueid --get only 1 email for candidate
where 1=1
and c."101 candidate codegroup  23" = 'Y'
--and c."74 maillist yn" = 'Y'
--and "1 name alphanumeric" ilike '%Eduardo%Cano%' --test candidate
--and c."4 ref no numeric" = '2891'