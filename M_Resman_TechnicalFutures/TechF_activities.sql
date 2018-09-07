select case when ClientID > 0 then concat('TF',ClientID) 
	else NULL end as CompExtID
, case when ContactID > 0 then concat('TF',ContactID) 
	else NULL end as ContactExtID
, case when CandidateID > 0 then concat('TF',CandidateID)
	else NULL end as CandidateExtID
, case when JobID > 0 then concat('TF',JobID) 
	else NULL end as JobExtID
, concat_ws(char(10)
	, coalesce('Action Code: ' + nullif(ActionCode,''),'')
	, coalesce('Business Process: ' + nullif(BusinessProcess,''),'')
	, coalesce('Consultant: ' + nullif(u.Firstname,'') + ' ' + nullif(u.Surname,''),'')
	, coalesce('Consultant Email: ' + nullif(u.Email,''),'')
	, coalesce('Regarding: ' + nullif(Regarding,''),'')
	, coalesce('InvoiceAmount: ' + nullif(InvoiceAmount,''),'')
	, coalesce('GST: ' + nullif(GST,''),'')
	, coalesce('InvoiceTotal: ' + nullif(InvoiceTotal,''),'')
	, coalesce('EmailAddress: ' + nullif(EmailAddress,''),'')
	, coalesce('Subject: ' + nullif(Subject,''),'')
	, coalesce('Comments: ' + nullif(Comments,''),'')
	) as TF_comment_activities
, case when DateOfAction is not NULL then convert(datetime,DateOfAction,112) + TimeOfAction
	else getdate() end as TF_insert_timestamp
, -10 as TF_user_account_id
, 'comment' as TF_category
from EditedActionHistoryPart1 a
left join Users u on u.ConsultantID = a.ConsultantID

UNION ALL

select case when ClientID > 0 then concat('TF',ClientID) 
	else NULL end as CompExtID
, case when ContactID > 0 then concat('TF',ContactID) 
	else NULL end as ContactExtID
, case when CandidateID > 0 then concat('TF',CandidateID)
	else NULL end as CandidateExtID
, case when JobID > 0 then concat('TF',JobID) 
	else NULL end as JobExtID
, concat_ws(char(10)
	, coalesce('Action Code: ' + nullif(ActionCode,''),'')
	, coalesce('Business Process: ' + nullif(BusinessProcess,''),'')
	, coalesce('Consultant: ' + nullif(u.Firstname,'') + ' ' + nullif(u.Surname,''),'')
	, coalesce('Consultant Email: ' + nullif(u.Email,''),'')
	, coalesce('Regarding: ' + nullif(Regarding,''),'')
	, coalesce('InvoiceAmount: ' + nullif(InvoiceAmount,''),'')
	, coalesce('GST: ' + nullif(GST,''),'')
	, coalesce('InvoiceTotal: ' + nullif(InvoiceTotal,''),'')
	, coalesce('EmailAddress: ' + nullif(EmailAddress,''),'')
	, coalesce('Subject: ' + nullif(Subject,''),'')
	, coalesce('Comments: ' + nullif(Comments,''),'')
	) as TF_comment_activities
, case when DateOfAction is not NULL then convert(datetime,DateOfAction,112) + TimeOfAction
	else getdate() end as TF_insert_timestamp
, -10 as TF_user_account_id
, 'comment' as TF_category
from EditedActionHistoryPart2 a
left join Users u on u.ConsultantID = a.ConsultantID

--total: 122993