-->> OFFER INFO AS JOB ACTIVITIES
select o.Match_Number, o.Stage_Id
	, concat('AFR',m.Job_Order_Number) as AFR_JobExtID
	, concat_ws(char(10),'[Offer Info]'
		, coalesce('Stage ID: ' + convert(nvarchar(max),o.Stage_Id),NULL)
		, coalesce('Offer Date: ' + convert(nvarchar(10),o.Offer_DT,120),NULL)
		, coalesce('Offer Amount: ' + convert(nvarchar(max),o.Offer_Amt),NULL)
		, coalesce('Candidate Number: ' + convert(varchar(10),m.Candidate_Number),NULL)
		, coalesce('Candidate Name: ' + coalesce(p.Name_First,'') + ' ' + coalesce(p.Name_Middle,'') + ' ' + coalesce(p.Name_Last,''),NULL)
		, coalesce('Job Order Number: ' + convert(varchar(10),m.Job_Order_Number),NULL)
		, coalesce('Job Title: ' + nullif(j.Position_Title,''),NULL)
		, coalesce('Company Number: ' + convert(varchar(10), m.Company_Number),NULL)
		, coalesce('Company Name: ' + nullif(c.Company_Name,''),NULL)
		) as AFR_comment_activities
	, -10 as AFR_user_account_id
	, convert(varchar(20),o.Offer_DT,120) as AFR_insert_timestamp
	, 'comment' as AFR_category
	, 'job' as AFR_type
from Offer o
left join Match m on m.Match_Number = o.Match_Number
left join Person p on p.Person_Number = m.Candidate_Number
left join Job_Order j on j.Job_Order_Number = m.Job_Order_Number
left join Company c on c.Company_Number = m.Company_Number
--where o.Match_Number = 113971
order by o.Match_Number


--OFFER DETAILS (MAIN CUSTOM SCRIPT) Status = 200
with LatestOffer as (select Match_Number, Stage_Id, Offer_Amt, Offer_DT, row_number() over(partition by Match_Number order by Stage_Id desc) as rn
	from Offer)

select o.Match_Number
	, o.Stage_Id
	, m.Candidate_Number as AFR_CandidateExtID
	, m.Job_Order_Number as AFR_JobExtID
	, o.Offer_Amt
	, left(o.Offer_Amt,charindex('.',o.Offer_Amt)-1)*12 as AFR_gross_annual_salary
	, convert(datetime,o.Offer_DT,120) as AFR_OfferDate
	, concat_ws(char(10),'The offer details were migrated from AFR'
		, coalesce('Stage: ' + convert(nvarchar(max),o.Stage_Id),'')
		, coalesce('Offer Date: ' + convert(nvarchar(20),o.Offer_DT,120),'')
		, coalesce('Offer Amount: ' + convert(nvarchar(max),o.Offer_Amt),'')
		, coalesce('Candidate Number: ' + convert(varchar(10),m.Candidate_Number),NULL)
		, coalesce('Candidate Name: ' + coalesce(p.Name_First,'') + ' ' + coalesce(p.Name_Middle,'') + ' ' + coalesce(p.Name_Last,''),NULL)
		, coalesce('Job Order Number: ' + convert(varchar(10),m.Job_Order_Number),NULL)
		, coalesce('Job Title: ' + nullif(j.Position_Title,''),NULL)
		, coalesce('Company Number: ' + convert(varchar(10), m.Company_Number),NULL)
		, coalesce('Company Name: ' + nullif(c.Company_Name,''),NULL)
		) as AFR_note
	, -10 as AFR_latest_user_id
	, getdate() as AFR_latest_update_date
	, 200 as AFR_status
	, 0 as AFR_tax_rate
	, 'other' as AFR_export_data_to
	, 0 as AFR_net_total
	, 0 as AFR_other_invoice_items_total
	, 0 as AFR_invoice_total
from LatestOffer o
left join Match m on m.Match_Number = o.Match_Number
left join Person p on p.Person_Number = m.Candidate_Number
left join Job_Order j on j.Job_Order_Number = m.Job_Order_Number
left join Company c on c.Company_Number = m.Company_Number
where o.rn = 1
--and o.Match_Number = 113971
order by o.Match_Number



-->> PLACEMENT INFO AS JOB ACTIVITIES
select p.Match_Number, p.Stage_Id
	, concat('AFR',m.Job_Order_Number) as AFR_JobExtID
	, concat_ws(char(10),'[Placement Info]'
		, coalesce('Stage: ' + convert(nvarchar(max),p.Stage_Id),'')
		, coalesce('Placement Date: ' + convert(nvarchar(10),p.Placement_DT,120),NULL)
		, coalesce('Start Date: ' + convert(nvarchar(20),p.Start_DT,120),'')
		, coalesce('Recruiter Fee Amount: ' + convert(nvarchar(max),p.Recruiter_Fee_Amt),'')
		, coalesce('Candidate Number: ' + convert(varchar(10),m.Candidate_Number),NULL)
		, coalesce('Candidate Name: ' + coalesce(pp.Name_First,'') + ' ' + coalesce(pp.Name_Middle,'') + ' ' + coalesce(pp.Name_Last,''),NULL)
		, coalesce('Job Order Number: ' + convert(varchar(10),m.Job_Order_Number),NULL)
		, coalesce('Job Title: ' + nullif(j.Position_Title,''),NULL)
		, coalesce('Company Number: ' + convert(varchar(10), m.Company_Number),NULL)
		, coalesce('Company Name: ' + nullif(c.Company_Name,''),NULL)
		) as AFR_comment_activities
	, -10 as AFR_user_account_id
	, convert(varchar(20),p.Placement_DT,120) as AFR_insert_timestamp
	, 'comment' as AFR_category
	, 'job' as AFR_type
from Placement p
left join Match m on m.Match_Number = p.Match_Number
left join Person pp on pp.Person_Number = m.Candidate_Number
left join Job_Order j on j.Job_Order_Number = m.Job_Order_Number
left join Company c on c.Company_Number = m.Company_Number
order by p.Match_Number


--PLACEMENT DETAILS (MAIN CUSTOM SCRIPT) Status >= 300
with LatestPlacement as (select Match_Number, Stage_Id, Placement_DT, Start_DT, Recruiter_Fee_Amt, row_number() over(partition by Match_Number order by Stage_Id desc) as rn
	from Placement)

select p.Match_Number
	, p.Stage_Id
	, p.Placement_DT as AFR_placed_date
	, p.Start_DT as AFR_start_date
	, m.Candidate_Number as AFR_CandidateExtID
	, m.Job_Order_Number as AFR_JobExtID
	, concat_ws(char(10),'The placement details were migrated from AFR'
		, coalesce('Stage: ' + convert(nvarchar(max),p.Stage_Id),'')
		, coalesce('Recruiter Fee Amount: ' + convert(nvarchar(max),p.Recruiter_Fee_Amt),'')
		, coalesce('Placed Date: ' + convert(nvarchar(20),p.Placement_DT,120),'')
		, coalesce('Start Date: ' + convert(nvarchar(20),p.Start_DT,120),'')
		, coalesce('Candidate Number: ' + convert(varchar(10),m.Candidate_Number),NULL)
		, coalesce('Candidate Name: ' + coalesce(pp.Name_First,'') + ' ' + coalesce(pp.Name_Middle,'') + ' ' + coalesce(pp.Name_Last,''),NULL)
		, coalesce('Job Order Number: ' + convert(varchar(10),m.Job_Order_Number),NULL)
		, coalesce('Job Title: ' + nullif(j.Position_Title,''),NULL)
		, coalesce('Company Number: ' + convert(varchar(10), m.Company_Number),NULL)
		, coalesce('Company Name: ' + nullif(c.Company_Name,''),NULL)
		) as AFR_note
	, -10 as AFR_latest_user_id
	, getdate() as AFR_latest_update_date
	, 300 as AFR_status
	, 0 as AFR_tax_rate
	, 'other' as AFR_export_data_to
	, 0 as AFR_net_total
	, 0 as AFR_other_invoice_items_total
	, 0 as AFR_invoice_total
from LatestPlacement p
left join Match m on m.Match_Number = p.Match_Number
left join Person pp on pp.Person_Number = m.Candidate_Number
left join Job_Order j on j.Job_Order_Number = m.Job_Order_Number
left join Company c on c.Company_Number = m.Company_Number
where p.rn = 1
order by p.Match_Number