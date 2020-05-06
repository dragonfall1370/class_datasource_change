--JOB links with same contacts but new companies
with dif_job as (select j.JobId
				, j.ClientId
				, j.JobRefNo
				, j.ClientContactId
				, cc.ClientId as Contact_ClientId
				, j.JobTitle
				from jobs j
				left join ClientContacts cc on cc.ClientContactId = j.ClientContactId
				where j.ClientId not in (select ObjectId from SectorObjects where SectorId = 49) --due to deleted companies
				and (j.ClientId <> cc.ClientId or j.ClientContactId is NULL)
				)
, dif_contact as (select JobId, ClientId
				--, concat_ws('_',ClientId,JobId) as dif_contact
				, ClientId as dif_contact
				, 'Default contact' as contact_lname
				, 'Default contact for this company' as contact_note
				from dif_job
				)
, consultant as (select j.JobConsultantId
				, j.JobId
				, j.userId
				, u.EmailAddress
				, u.UserFullName
				, j.UserRelationshipId
				, ur.Description
				, j.CommissionPerc
				, g.GroupName
				from JobConsultants j
				left join Users u on u.UserId = j.UserId
				left join UserRelationships ur on ur.UserRelationshipId = j.UserRelationshipId
				left join Groups g on g.GroupId = j.UserGroupId
				)
, owners as (select JobId
				, string_agg(EmailAddress, ',') within group (order by UserRelationshipId) as owners --9 is Primary owner
				from consultant
				group by JobId
				)
, consultant_info as (select JobId
				, string_agg(
					concat_ws(' - '
						, nullif(UserFullName,'')
						, coalesce('Relationship: ' + nullif(Description, ''), NULL)
						, coalesce('Group name: ' + nullif(GroupName, ''), NULL)
						, coalesce('[Commission(%): ' + convert(varchar(max),CommissionPerc, 1)+ ']',NULL))
					, ', ') as consultant_info
				from consultant
				group by JobId
				)
, userinfo as (select u.UserId,u.LoginName,u.UserName,u.UserFullName,u.JobTitle,u.Inactive
				from Users u
				)
, job_documents as (select jd.JobId
				, STRING_AGG(concat_ws('_','NP_JD',jd.DocumentId,concat(jd.jobId,dc.FileExtension)),',') 
					within group (order by jd.DocumentID desc) as job_documents
				from dbo.JobDocuments jd
				left join dbo.DocumentContent dc on dc.DocumentID=jd.DocumentId
				where dc.FileExtension in ('.pdf','.doc','.rtf','.xls','.xlsx','.docx','.png','.jpg','.jpeg','.gif','.bmp','.msg','.txt','.htm','.html')
				group by jd.JobId
				)
--Placement Documents
, placement_doc as (select p.JobId
			, string_agg(concat_ws('_','NP_P', d.DocumentID, concat(p.JobId, dc.FileExtension)),',') 
				within group (order by d.DocumentID desc) as placement_doc
			from PlacementDocuments pd
			left join Placements p on p.PlacementID = pd.PlacementID
			left join Documents d on d.DocumentID = pd.DocumentId
			left join DocumentContent dc on d.DocumentId = dc.DocumentId
			where 1=1
			and dc.FileExtension in ('.pdf','.doc','.rtf','.xls','.xlsx','.docx','.png','.jpg','.jpeg','.gif','.bmp','.msg','.txt','.htm','.html')
			and p.JobId is not NULL
			group by p.JobId
			)
--CVSend documents
, job_cvsend as (select aa.JobId--, d.DocumentID
		, string_agg(cast(concat_ws('_','NP_CVSend', d.DocumentID, concat(aa.JobId, dc.FileExtension)) as varchar(max)),',') 
			within group (order by d.DocumentID desc) as job_cvsend
		from CVSendDocuments cvs
		left join ApplicantActions aa on aa.ApplicantActionId = cvs.ApplicantActionId
		left join Documents d on d.DocumentID = cvs.DocumentId
		left join DocumentContent dc on d.DocumentId = dc.DocumentId
		where 1=1
		--and cvs.CVId is NULL --10321 --CV already parsed for candidates
		and dc.FileExtension in ('.pdf','.doc','.rtf','.xls','.xlsx','.docx','.png','.jpg','.jpeg','.gif','.bmp','.msg','.txt','.htm','.html')
		and aa.JobId is not NULL --16852 rows
		group by aa.JobId --4021 rows
		)
--Interview Documents
, job_interview as (select nl.JobId
		, string_agg(cast(concat_ws('_','NP_IV', d.DocumentID, concat(nl.JobId, dc.FileExtension)) as varchar(max)),',') 
			within group (order by d.DocumentID desc) as job_interview
		from InterviewDocuments id --5602
		left join NotebookLinks nl on nl.NotebookItemId = id.NotebookItemId
		left join Documents d on d.NotebookItemId = nl.NotebookItemId
		left join DocumentContent dc on dc.DocumentId = d.DocumentID
		where 1=1
		and dc.FileExtension in ('.pdf','.doc','.rtf','.xls','.xlsx','.docx','.png','.jpg','.jpeg','.gif','.bmp','.msg','.txt','.htm','.html')
		and nl.JobId is not NULL --7605 rows
		group by nl.JobId --1201 rows
		)
, dup as (select JobId
			, case when nullif(JobTitle,'') is null then concat('No job title - ', JobRefNo) else JobTitle end as JobTitle
			, row_number() over(partition by lower(JobTitle) order by JobId) as rn
			from Jobs)
, placement_enddate as (select JobId, max(CreatedOn) as EndDate --Latest placement date as EndDate
			from Placements
			where JobID is not NULL
			group by JobId
			)
--MAIN SCRIPT
select concat('NP',j.JobId) as [position-externalId]
        , case when dup.rn > 1 then concat_ws('_', j.JobTitle, j.JobRefNo)
				else dup.JobTitle end as [position-title]
		, j.ClientContactId
		, j.ClientId
        , case when j.jobId in (select jobId from dif_contact) then concat('NP_DEF', dc.dif_contact) --default contact
				else concat('NP', j.ClientContactId) end as [position-contactId]
        , coalesce(nullif(j.NoOfPlaces,0),1) as [position-headcount]
        , case when j.StartDate is NULL then convert(date,j.CreatedOn,120)
			else convert(date,j.StartDate,120) end as [position-startDate]
		, pe.EndDate as [position-endDate] --get placement date as EndDate
        , case 
                when j.CurrencyId =10 then 'GBP'
                when j.CurrencyId =11 then 'USD'
                --when j.CurrencyId =12 then 'SEK' --not available
				when j.CurrencyId =13 then 'CHF'
				when j.CurrencyId =14 then 'EUR'
                else 'GBP' end as [position-currency]
        , o.owners as [position-owners]
        , case 
                when j.EmploymentTypeId in (5, 6, 7, 8, 10, 11, 13) then 'CONTRACT'
                when j.EmploymentTypeId in (4, 9, 12) then 'PERMANENT'
                else 'CONTRACT' end as [position-type] --having contract rate type
        , convert(money,j.MaxBasic) as [position-actualSalary]
		, convert(money,j.MinBasic) as salaryFrom --#Inject
        , convert(money,j.MaxBasic) as salaryTo --#Inject
		--, nullif(j.Notes,'') as [position-internalDescription]
		, nullif(j.PublishedJobDescription,'') as [position-publicDescription]
        --NOTE
        , concat_ws(
                char(10)
                , coalesce('External ID: ' + convert(varchar(max), j.JobId),NULL)
                , coalesce('Created by: ' + u.UserFullName,NULL)
                , coalesce('Created On: ' + convert(varchar(max),j.CreatedOn,120),NULL)
                --, coalesce('Position: ' + nullif(a.Notes, ''), NULL) --j.PositionAttributeId
                --, coalesce('Sector Name: ' + st.SectorName, NULL)
                , coalesce('Job Ref No: ' + j.JobRefNo, NULL)
				, coalesce('Parent Company: ' + nullif(cl.Company, ''), NULL)
                , coalesce('Status: ' + nullif(js.Description, ''),NULL)
                --, coalesce('Job Location: ' + nullif(concat_ws(' - ', l.Description, l.Code),''),NULL)     
				, coalesce('Work Address: ' + j.WorkAddress,NULL)				
                --, coalesce('Min Package: ' + convert(varchar(max),j.MinPackage),NULL)
                --, coalesce('Max Package: ' + convert(varchar(max),j.MaxPackage),NULL)
                --, coalesce('Retainer: ' + convert(varchar(max),j.Retainer),NULL)
                --, coalesce('Salary: ' + convert(varchar(max),j.Salary),NULL)
                --, coalesce('Max Age: ' + convert(varchar(max),j.MaxAge),NULL)
                --, coalesce('Actual Salary: ' + convert(varchar(max),j.MinBasic),NULL)
                --, coalesce('Salary To: ' + convert(varchar(max),j.MaxBasic),NULL)
                , coalesce('Commission Perc: ' + convert(varchar(max),j.CommissionPerc),NULL)
                --, coalesce('Placement Fee: ' + convert(varchar(max),j.PlacementFee),NULL)
				--, coalesce(char(10) + '--Interview Address--' + char(10) + nullif(j.InterviewAddress, ''),NULL)
                --, coalesce('Published: ' + j.Published, NULL)
                --, coalesce('Published Job Category: ' + nullif(jc.Description, ''),NULL)
                --, coalesce('Published Job Location: ' + nullif(a2.Notes, ''),NULL)
                --, coalesce(char(10) + '--Consultant Info--' + char(10) + nullif(ci.consultant_info,''), NULL)
                , coalesce(char(10) + '--Notes-- ' + char(10) + coalesce(nullif(j.Notes,''),'NONE'), NULL)
        ) as [position-note]
        , concat_ws(',', jd.job_documents, jc.job_cvsend, ji.job_interview) as [position-document]
from Jobs j
left join dup on dup.JobId = j.JobId
left join dif_contact dc on dc.JobId = j.JobId
--left join Attributes a on a.AttributeId = j.PositionAttributeId --links with AttributeMaster: Position
--left join Attributes a2 on a2.AttributeId = j.AreaAttributeId --links with AttributeMaster: Area
left join userinfo u on u.UserId=j.CreatedUserId
--left join Locations l on l.LocationId = j.LocationId --locations
--left join Sectors st on st.SectorId=j.SectorId
--left join EmploymentTypes et on et.EmploymentTypeId=j.EmploymentTypeId
left join owners o on o.JobId = j.JobId
--left join consultant_info ci on ci.JobId = j.JobId
left join Clients cl on cl.ClientId = j.ClientHirerLegalEntityId --Parent company
left join JobStatus js on js.JobStatusId=j.StatusId
--left join JobCategories jc on jc.JobCategoryId=j.PublishedJobCategoryId
--left join placement_doc pd on pd.JobId = j.JobId
left join placement_enddate pe on pe.JobId = j.JobId
left join job_documents jd on jd.JobId = j.JobId
left join job_cvsend jc on jc.JobId = j.JobId
left join job_interview ji on ji.JobId = j.JobId
where 1=1
and j.ClientId not in (select ObjectId from SectorObjects where SectorId = 49) --Deleted Clients