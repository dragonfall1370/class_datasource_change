--RUN [dbo].[udf_StripHTML] FUNCTION BEFORE RUNNING THIS SCRIPT

--MAIN SCRIPT
select coalesce('PRTR' + nullif(convert(varchar(max),h.company_id),''),NULL) as CompExtID
, coalesce('PRTR' + nullif(convert(varchar(max),h.contact_id),''),NULL) as ContactExtID
, coalesce('PRTR' + nullif(convert(varchar(max),h.vac_id),''),NULL) as JobExtID
, coalesce('PRTR' + nullif(convert(varchar(max),h.can_id),''),NULL) as CandidateExtID
, concat_ws(char(10)
	, concat('[History activities] '
		, coalesce('Added by: ' + nullif(ltrim(rtrim(u.usr_fullname)) + ' - ' + trim(' ' from u.usr_email),''),NULL)) --updated on 20181101
	, coalesce('Action: ' + nullif(h.his_action,''),NULL)
	, coalesce('*** Details: ' + nullif([dbo].[udf_StripHTML](ltrim(h.his_full_details)),''),'')
	, coalesce('Activity ID: ' + convert(varchar(max),h.his_id),'')
	, coalesce('Added on: ' + nullif(convert(varchar(max),h.his_date),''),NULL) --changed order on 20181106
	) as Comment_activities
, h.his_date as Insert_timestamp
, 'comment' as category
, -10 as User_account_id
from common.History h --3153434
left join users.Users u on h.usr_id = u.usr_id
left join company.Companies com on com.company_id = h.company_id
left join candidate.Candidates c on c.cn_id = h.can_id
left join candidate.Candidates c2 on c2.cn_id = h.contact_id
left join vacancies.Vacancies v on v.vac_id = h.vac_id
--3593202 rows