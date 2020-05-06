/* CREATE TEMP TABLE
insert into tmp_notebook
select notebookItemId
, case when FileExtension like '%rtf' then dbo.RTF2TXT(Memo)
	when FileExtension like '%html' then dbo.udf_StripHTML(Memo)
	else Memo end as Memo
, FileExtension
, CreatedUserId
, CreatedOn
from NotebookItemContent
where notebookItemId between 500000 and 600000

select max(NotebookItemId)
from NotebookItemContent
where NotebookItemId = 1
*/

with phone as (
        select p.ObjectID,p.Num,p.CommunicationTypeId
        from dbo.Phones p
        where p.CommunicationTypeId in (78,85,86) --Email, Email (Office), Email (Personal)
		and p.Num like '%_@_%.__%'
)
, companyID as (
        select DISTINCT o.ObjectID
        from dbo.Objects o
        where ObjectTypeId=2
)
, contactIDlist as (
        select cc.ClientContactId,o.ObjectID,row_number() over (partition by o.ObjectID order by cc.ClientContactId desc) as rn
        from dbo.ClientContacts cc
        left join dbo.Objects o on cc.ContactPersonId=o.ObjectID
        where o.ObjectTypeId=3
)
, contactID as (
        select ClientContactId,ObjectID
        from contactIDlist
        where rn=1
)
, candidateID as (
        select DISTINCT o.ObjectID
        from dbo.Objects o
        where ObjectTypeId=1
)
, contact as (      
        select cc.ClientContactId,o.ObjectID,COALESCE(p1.Num,p3.Num,p2.Num) as Email
        from dbo.ClientContacts cc 
        inner join dbo.Objects o on cc.ContactPersonId=o.ObjectID
        left join (select ObjectID, STRING_AGG(Num,',') as Num from Phone where CommunicationTypeId=78 group by ObjectID) p1 on p1.ObjectID=o.ObjectID
        left join (select ObjectID, STRING_AGG(Num,',') as Num from Phone where CommunicationTypeId=85 group by ObjectID) p3 on p3.ObjectID=o.ObjectID
        left join (select ObjectID, STRING_AGG(Num,',') as Num from Phone where CommunicationTypeId=86 group by ObjectID) p2 on p2.ObjectID=o.ObjectID
		where o.ObjectTypeId=3
)
, candidate as (
        select a.ApplicantId,o.ObjectID,COALESCE(p1.Num,p2.Num,p3.Num) as Email
        from dbo.Applicants a
        inner join dbo.Objects o on a.ApplicantId=o.ObjectID
        left join (select ObjectID, STRING_AGG(Num,',') as Num from Phone where CommunicationTypeId=78 group by ObjectID) p1 on p1.ObjectID=o.ObjectID
        left join (select ObjectID, STRING_AGG(Num,',') as Num from Phone where CommunicationTypeId=85 group by ObjectID) p3 on p3.ObjectID=o.ObjectID
        left join (select ObjectID, STRING_AGG(Num,',') as Num from Phone where CommunicationTypeId=86 group by ObjectID) p2 on p2.ObjectID=o.ObjectID
		where o.ObjectTypeId=1​
)
, emailtolist as (
        select distinct nl.NotebookItemId
		, coalesce(nullif(co.Email,''), nullif(ca.Email,',')) as AddressMail
        from NotebookLinks nl
        left join contact co on nl.ObjectId = co.ObjectId
        left join candidate ca on nl.ObjectId = ca.ObjectId
        where nl.NotebookLinkTypeId=18 --Email TO
UNION
        select distinct nl.NotebookItemId
		, coalesce(nullif(co.Email,''),nullif(ca.Email,',')) as AddressMail
        from NotebookLinks nl
        left join contact co on nl.ObjectId=co.ObjectId
        left join candidate ca on nl.ObjectId=ca.ObjectId
        where nl.NotebookLinkTypeId=19 --Email CC
UNION
        select distinct nl.NotebookItemId
		, coalesce(nullif(co.Email,''),nullif(ca.Email,',')) as AddressMail
        from NotebookLinks nl
        left join contact co on nl.ObjectId=co.ObjectId
        left join candidate ca on nl.ObjectId=ca.ObjectId
        where nl.NotebookLinkTypeId=20 --Email BCC
)
, emailto as (
        select distinct NotebookItemId, STRING_AGG(convert(varchar(max),nullif(AddressMail,'')),',') as AddressMail
        from emailtolist
        group by NotebookItemId
) 
, emailfromlist as (select distinct nl.NotebookItemId
		, coalesce(nullif(co.Email,''),nullif(ca.Email,',')) as AddressMail
        from NotebookLinks nl
        left join contact co on nl.ObjectId = co.ObjectId
        left join candidate ca on nl.ObjectId = ca.ObjectId
        where nl.NotebookLinkTypeId=28 --Email From
) 
/*
select NotebookItemId, count(*)
from emailfromlist
group by NotebookItemId
having count(*) > 1 --check: select * from emailfromlist where NotebookItemId = 431117
*/
, emailfrom as (
        select NotebookItemId, STRING_AGG(convert(varchar(max),nullif(AddressMail,'')),',') as AddressMail
        from emailfromlist
        group by NotebookItemId
)
, emailReference as (
        select nl.NotebookItemId,STRING_AGG(COALESCE(co.Email,ca.Email),',') as AddressMail
        from NotebookLinks nl
        left join contact co on nl.ObjectId=co.ObjectId
        left join candidate ca on nl.ObjectId=ca.ObjectId
        where nl.NotebookLinkTypeId=21 --Reference
        group by nl.NotebookItemId        
), emailSMS as (
        select nl.NotebookItemId,STRING_AGG(COALESCE(co.Email,ca.Email),',') as AddressMail
        from NotebookLinks nl
        left join contact co on nl.ObjectId=co.ObjectId
        left join candidate ca on nl.ObjectId=ca.ObjectId
        where nl.NotebookLinkTypeId=24 --SMS
        group by nl.NotebookItemId        
), emailPrimary as (
        select nl.NotebookItemId,STRING_AGG(COALESCE(co.Email,ca.Email),',') as AddressMail
        from NotebookLinks nl
        left join contact co on nl.ObjectId=co.ObjectId
        left join candidate ca on nl.ObjectId=ca.ObjectId
        where nl.NotebookLinkTypeId=27 --Primary
        group by nl.NotebookItemId        
), notebookdetailcompany as (
        select DISTINCT nl.NotebookItemId,nl.ClientId--,coo.Email as contactEmail,caa.Email as candidateemail
        from dbo.NotebookLinks nl
        where 1=1
        and nl.ClientId is not null  
        )
, notebookdetailcontact as (
        select DISTINCT nl.NotebookItemId,co.ClientContactId--,coo.Email as contactEmail,caa.Email as candidateemail
        from dbo.NotebookLinks nl
        left join dbo.NotebookLinkTypes nlt on nlt.NotebookLinkTypeId=nl.NotebookLinkTypeId
        inner join contactID co on nl.ObjectId=co.ObjectId
        where 1=1
), notebookdetailcandidate as (
        select DISTINCT nl.NotebookItemId,co.ObjectId--,coo.Email as contactEmail,caa.Email as candidateemail
        from dbo.NotebookLinks nl
        left join dbo.NotebookLinkTypes nlt on nlt.NotebookLinkTypeId=nl.NotebookLinkTypeId
        inner join candidateID co on nl.ObjectId=co.ObjectId
        where 1=1
), notebookdetailjob as (
        select DISTINCT nl.NotebookItemId,nl.JobId--,coo.Email as contactEmail,caa.Email as candidateemail
        from dbo.NotebookLinks nl
        where 1=1
        and nl.JobId is not null 
)
, notebooklist as (
        select ni.NotebookItemId
                , ncom.ClientId as CompanyID
                , nco.ClientContactId as ContactID
                , nca.ObjectId as CandidateID
                , nj.JobId as JobId
                , et.AddressMail as 'emailto'
                , ef.AddressMail as 'emailfrom'--ef.ClientId,ef.JobId,
        from dbo.NotebookItems ni
        left join emailto et on ni.NotebookItemId=et.NotebookItemId
        left join emailfrom ef on ni.NotebookItemId=ef.NotebookItemId
        left join notebookdetailcompany ncom on ni.NotebookItemId=ncom.NotebookItemId 
        left join notebookdetailcontact nco on ni.NotebookItemId=nco.NotebookItemId 
        left join notebookdetailcandidate nca on ni.NotebookItemId=nca.NotebookItemId 
        left join notebookdetailjob nj on ni.NotebookItemId=nj.NotebookItemId 
)
, userinfo as (
        select u.UserId,u.LoginName,u.UserName,u.UserFullName,u.JobTitle,u.Inactive
        from Users u)

, tasklist as (select NotebookItemId
		, string_agg(concat_ws(char(10)
			, COALESCE('Subject: '+ Subject,NULL)
			, COALESCE('Start date: '+ convert(varchar(max),StartDate,120),NULL)
			, COALESCE('Due Date: '+ convert(varchar(max),DueDate,120),NULL)
		), char(10)) as tasklist
		from tasks
		where NotebookItemId is not NULL
		group by NotebookItemId)
--MAIN SCRIPT
select ni.NotebookItemId
        , COALESCE('NP'+ convert(varchar(max), nullif(nli.CompanyID,'')),NULL) as com_ext_id
        , COALESCE('NP' + convert(varchar(max),nullif(nli.ContactID,'')),NULL) as con_ext_id
        , COALESCE('NP' + convert(varchar(max),nullif(nli.JobId,'')),NULL) as job_ext_id
        , COALESCE('NP' + convert(varchar(max),nullif(nli.CandidateID,'')),NULL) as cand_ext_id
        , -10 as user_account_id
        , ni.CreatedOn as insert_timestamp
        , 'comment' as category
        , concat_ws(char(10), '[Notebook]'
			, coalesce('Notebook Type: '+ nt.NotebookType,NULL)
			, coalesce('Subject: '+ ni.Subject,NULL)
			, coalesce('From: '+ nli.emailfrom,NULL)
			, coalesce('Recipients: '+ nli.emailto,NULL)
			, coalesce('Folder Name: '+ nf.FolderName,NULL)
			, coalesce('Protected: ' + ni.Protected,NULL)
			, coalesce('Hot Item: ' + ni.HotItem,NULL)
			, coalesce('Created by: ' + uic.UserFullName,NULL)
			, coalesce('Created On: ' + convert(nvarchar(max),ni.CreatedOn,120),NULL)
			, coalesce('Updated by: '  +uiu.UserFullName,NULL)
			, coalesce('Updated On: ' + convert(nvarchar(max),ni.UpdatedOn,120),NULL)
			, coalesce('--Task--' + char(10) + uiu.UserFullName,NULL)
			, coalesce('--Memo--' + char(10) + nullif(tn.Memo,''),NULL)
        ) as comment_activities
from notebooklist nli
left join dbo.NotebookItems ni on ni.NotebookItemId=nli.NotebookItemId
left join NotebookFolders nf on nf.NotebookFolderId=ni.NotebookFolderId
left join dbo.NotebookTypes nt on nt.NotebookTypeId=ni.NotebookTypeId
--left join dbo.NotebookItemContent nic on nic.NotebookItemId=ni.NotebookItemId
left join dbo.tmp_notebook tn on tn.NotebookItemId = ni.NotebookItemId
left join userinfo uic on ni.CreatedUserId=uic.UserId
left join userinfo uiu on ni.UpdatedOn=uiu.UserId
left join tasklist tl on tl.NotebookItemId = ni.NotebookItemId
where 1=1
--and ni.NotebookItemId=383801
--and nli.CompanyID=21
--and tn.Createdon >= '2016-08-16 09:08:52.980'
--and tn.Createdon between '2008-08-16 09:08:52.980' and '2016-08-16 09:08:52.980' --2014-05-09 12:34:38
and tn.Createdon between '2008-08-16 09:08:52.980' and '2014-05-09 12:34:38'
and (nli.CompanyID is not NULL or nli.ContactID is not NULL or nli.JobId is not NULL or nli.CandidateID is not NULL)