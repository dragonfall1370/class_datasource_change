------------Company activities-1,048,680 records-----------------------

select a.UniqueID as activity_external_id
    , 'BB - ' + cast(b.[8 Reference Numeric] as varchar(20)) as company_externalid
    , 'BB - ' + cast(con.[4 RefNumber Numeric] as varchar(20)) as contact_externalid
	, 'BB - ' + cast(job.[1 Job Ref Numeric] as varchar(20)) as job_externalId
	, -10 as user_id
	, 'comment' as category
	--, b.[1 Name Alphanumeric] as company
	--, con.[1 Name Alphanumeric]  as contact
	, [Create Date] as insert_timestamp
	,  c.code 
	--,  c.Description as actiondesc
	--, u.[1 Name Alphanumeric] as consultant
	--, job.[3 Job Title Alphanumeric] as job
	, coalesce('Action:[ ' + c.code + '-' + c.Description + ']' + char(10), '') 
	+ coalesce('Created By: ' + u.[1 Name Alphanumeric] + char(10), '')
	/*+ coalesce('Site: ' + b.[1 Name Alphanumeric] + char(10), '')
	+ coalesce('Contact: ' + con.[1 Name Alphanumeric] + char(10), '')
	+ coalesce('Job: ' + job.[3 Job Title Alphanumeric] + char(10), '')*/
    + case when [Notes 1] is NULL and [Notes 2] is null and [Notes 3] is null and [Notes 4] is null and [Notes 5] is null and [Notes 6] is null and [Notes 7] is null then '' 
	  else 'Notes: ' end
	+ coalesce(a.[Notes 1], '')
	+ coalesce(' ' + a.[Notes 2], '')
	+ coalesce(' ' + a.[Notes 3], '')
	+ coalesce(' ' + a.[Notes 4], '')
	+ coalesce(' ' + a.[Notes 5], '')
	+ coalesce(' ' + a.[Notes 6], '')
	+ coalesce(' ' + a.[Notes 7], '')
	+ case when a.[Field 5] = 'CC' then
	   coalesce(char(10) + 'Next Call Date: ' + a1.[7 Next call (Date)], '') 
	 + coalesce(char(10) + 'CC Note: ' + [8 Note (Note)], '') else '' end
    + case when a.[Field 5] = 'FN' then
	  coalesce(char(10) + 'Action time: ' + convert(varchar(50), [7 Actn time (Xref)], 103), '')
	+ coalesce(char(10) + 'FN Note: ' + [8 File note (Note)], '') else '' end
	+ case when a.[Field 5] = 'I1' then
	  coalesce(char(10) + 'Interview Time: ' + convert(varchar(50), [7 Int Time (Xref)], 103), '')
	+ coalesce(char(10) + 'Job Title: ' + [11 Job Title (Note)], '')
	+ coalesce(char(10) + 'Company Address1: ' + [15 Compadd1 (Note)], '')
	+ coalesce(char(10) + 'Consultant Title: ' + [25 constitle (Note)], '')
	+ coalesce(char(10) + 'Candidate Address1: ' + [29 Candadd1 (Note)], '')
	+ coalesce(char(10) + 'Email: ' + [35 email w (Note)], '')
	+ coalesce(char(10) + 'Subject: ' + [38 subject (Note)], '')
	+ coalesce(char(10) + 'Email Signature: ' + [42 EM Sig (Note)], '')
	+ coalesce(char(10) + 'Phone: ' + [43 Phone (Note)], '')
	+ coalesce(char(10) + 'Mobile: ' + [44 Mobile (Note)], '')
	  else '' end
	as content

from act as a
LEFT JOIN F02 as b on b.UniqueID = a.[Field 2]
LEFT JOIN(SELECT * FROM CODES WHERE Codegroup = 94) as c on c.Code = a.[Field 5]
LEFT JOIN F17 as u on u.UniqueID = a.[Field 4]
LEFT JOIN(SELECT UniqueID, [1 Name Alphanumeric],[4 RefNumber Numeric]  FROM F01) as con on con.UniqueID = a.[Field 1]
LEFT JOIN F03 as job on job.UniqueID = a.[Field 3]
LEFT JOIN ACT_CC as a1 on a1.[ CC UniqueID] = a.UniqueID --and ([7 Next call (Date)] is not null or [8 Note (Note)] is not null)
left join act_fn as a2 on a2.[ FN UniqueID] = a.UniqueID
left join ACT_I1 as a3 on a3.[ I1 UniqueID] = a.UniqueID
where 1=1
--and [Field 2] is not NULL
 and [Field 5] in('CCC', 'CC', 'FN', 'PH', 'PC1', 'PC', 'I1')
--and [Field 2] in( '80810201E4D28080', '8081020186828080')





