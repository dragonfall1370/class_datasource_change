
with
mail1 (ID,userID,email,rn) as (
       select Cl.clientID, Cl.userID
              , replace(translate(value, '!'':"<>[]();,+/\&?•*|‘','                     '),char(9),'') as email --to translate special characters
              , row_number() over(partition by Cl.clientID order by Cl.clientID) as rn
	from bullhorn1.View_ClientContact Cl left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
	cross apply string_split( concat_ws(' ', nullif(convert(nvarchar(max),trim(UC.email)), '') , nullif(convert(nvarchar(max),trim(UC.email2)), ''), nullif(convert(nvarchar(max),trim(UC.email3)), '') ),' ')
	where (UC.email like '%_@_%.__%' or UC.email2 like '%_@_%.__%' or UC.email3 like '%_@_%.__%') and Cl.isdeleted <> 1 --and Cl.status <> 'Archive'
--	and UC.userID in (4840)
	)
--select top 10 * from mail1 where email <> ''

, mail2 (ID,userID,email,rn,ID_rn) as (
       select ID, userID
              , trim(' ' from email) as email
--              , row_number() over(partition by trim(' ' from email) order by ID asc) as rn --distinct email if emails exist more than once
--              , row_number() over(partition by ID order by trim(' ' from email)) as ID_rn --distinct if contacts may have more than 1 email
              , row_number() over(partition by trim(' ' from email) order by ID asc) as rn --distinct email if emails exist more than once
              , row_number() over(partition by ID order by rn asc) as ID_rn --distinct if contacts may have more than 1 email              
	from mail1
	where email like '%_@_%.__%'
	)
--select * from mail2 where userID in (4840)

, ed (ID,email) as (
       select ID
	      , case when rn > 1 then concat(email,'_',rn)
	else email end as email
	from mail2
	where email is not NULL and email <> ''
	and ID_rn = 1
	)
, e2 (ID,email) as (select ID, email from mail2 where ID_rn = 2)
, e3 (ID,email) as (select ID, email from mail2 where ID_rn = 3)	
--select * from ed where id = 1000 --email like '%lburlovich@challenger.com.au%'


, dup_con as (
       select t.email from (
              select replace(translate(uc.email , '!'':"<>[]();,+/\&?•*|‘','                     '),char(9),'') as email
              from bullhorn1.View_ClientContact Cl left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
              where Cl.isdeleted <> 1
       ) t group by t.email having count(*) > 1
)

--, dup_con as (
--       select t.userid from (
--              select Cl.userID 
--              from bullhorn1.View_ClientContact Cl
--              left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
--              where Cl.isdeleted <> 1
--       ) t group by t.userID having count(*) > 1
--)
--select * from dup_con where userid in (19658)

,
--JOB DUPLICATION REGCONITION
job0 as (
       select
                a.jobPostingID
              , a.dateadded
              , iif(a.title is not null and a.title <> '', trim(a.title), 'No JobTitle') as title
              , convert(varchar(10),a.startDate,120) as startDate
              , iif(a.clientcorporationid is null or a.clientcorporationid = '', 'default', a.clientcorporationid) as 'Company_externalID', cc.name as 'CompanyName' --COMPANY
              , iif(company.contact_externalId is null or convert(varchar(500),company.contact_externalId) = '', 'default', convert(varchar(500),company.contact_externalId)) as 'contact_externalId', a.clientUserID as 'ContactUserID', uc.firstname as 'Contact_firstname', uc.lastname as 'Contact_lastname'--, uc.isdeleted as '#Contact_isdeleted', uc.status as '#Contact_status'              

              , company.[contact_companyId] as 'Company_externalID_of_ContactUserID'
              , company.company_name as 'CompanyName_of_ContactUserID'
       -- select count(*)                            
       from bullhorn1.BH_JobPosting a
       left join bullhorn1.BH_UserContact UC on a.clientUserID = UC.userID
       left JOIN ( select clientCorporationID, name, status from bullhorn1.BH_ClientCorporation CC /*where CC.status <> 'Archive'*/) cc ON cc.clientCorporationID = a.clientcorporationid
       left join (
              select * 
              from (
                     select
                              Cl.clientID as 'contact_externalId', Cl.userID as 'UserID'
                            , UC.clientCorporationID as 'contact_companyId'
                            , com.name as 'company_name'
                            , ROW_NUMBER() OVER(PARTITION BY Cl.UserID, UC.clientCorporationID, com.name ORDER BY Cl.clientID desc) AS rn 
                     -- select *
                     from bullhorn1.View_ClientContact Cl
                     left join (select distinct userID,clientCorporationID from bullhorn1.BH_UserContact) UC ON Cl.userID = UC.userID
                     left join (select clientCorporationID, name from bullhorn1.BH_ClientCorporation /*where status <> 'Archive'*/) com on com.clientCorporationID = UC.clientCorporationID
                     where (Cl.isdeleted <> 1 /*and Cl.status <> 'Archive'*/) --and Cl.userID in (579,605)
              ) n where rn = 1
              ) company on company.userID = a.clientUserID
       where (a.isdeleted <> 1 /*and a.status <> 'Archive'*/) --and company.rn = 1
       --and a.clientcorporationid in (51)       
       --and a.jobPostingID in (18,19,20)
)
, job (jobPostingID, dateadded, title, starDate, Company_externalID, CompanyName, contact_externalId, final_contact_externalId, ContactUserID, Contact_firstname, Contact_lastname, Company_externalID_of_ContactUserID, CompanyName_of_ContactUserID, rn) as (
	SELECT  jobPostingID
	       , dateadded
		, title
		, startDate
       --COMPANY
		, Company_externalID, CompanyName
       --CONTACT
		, contact_externalId
		, case
		     when Company_externalID = Company_externalID_of_ContactUserID  then iif(contact_externalId is null or contact_externalId = '', concat('default',convert(varchar(5000),Company_externalID)), convert(varchar(5000),contact_externalId) )
		     --when Company_externalID <> Company_externalID_of_ContactUserID then iif(contact_externalId is null or contact_externalId = '', concat('default',convert(varchar(5000),Company_externalID)), concat('default',convert(varchar(5000),contact_externalId)) )
		     when Company_externalID <> Company_externalID_of_ContactUserID then iif(contact_externalId is null or contact_externalId = '', concat('default',convert(varchar(5000),Company_externalID)), concat('default',convert(varchar(5000),Company_externalID),'.',convert(varchar(5000),contact_externalId)) )
		     end as 'final_contact'
		, ContactUserID, Contact_firstname, Contact_lastname
              , Company_externalID_of_ContactUserID
              , CompanyName_of_ContactUserID
		, ROW_NUMBER() OVER(PARTITION BY contact_externalId,title,startDate ORDER BY jobPostingID) AS rn 
	from job0 
	)
--select * from job where jobPostingID in (952)


-- >>> CREATE DEFAULT CONTACT LIST FOR JOB <<< ---
--select distinct Company_externalID as 'contact-companyId', final_contact_externalId as 'contact-externalId', Contact_firstname as 'contact-firstname', Contact_lastname as 'contact-lastname' from job where final_contact_externalId like 'default%' order by Company_externalID desc

, num_job as (
select position_contactId, count(*) as amount
from (
       select
                a.jobPostingID as 'position_externalId'
              , iif(job.final_contact_externalId is null, 'default',job.final_contact_externalId) as 'position_contactId'
              --, job.Company_externalID as '#CompanyId', job.CompanyName as '#CompanyName', job.ContactUserID as '#ContactUserID', job.Contact_firstname as '#Contact_firstname', job.Contact_lastname as '#Contact_lastname'
              , a.dateadded
              , case when job.rn > 1 then concat(job.title,' ',rn) else job.title end as 'position_title'
       from bullhorn1.BH_JobPosting a
       left join job on a.jobPostingID = job.jobPostingID
       ) t group by position_contactId
) 
--select * from num_job


select
         Cl.clientID as 'contact-externalId'
       , Cl.userID as 'UserID'
       , Cl.status as '#status'
       --, iif(UC.clientCorporationID in (select clientCorporationID from bullhorn1.BH_ClientCorporation where status <> 'Archive'), convert(varchar(max),UC.clientCorporationID), 'default' ) as 'contact-companyId' --, UC.clientCorporationID as 'contact-companyId'
       , case when com.clientCorporationID is null then 'default' else com.name end as 'company-name'
       , UC.firstName as 'contact-firstName'
       , UC.lastName as 'contact-Lastname'
       , ed.email as 'contact-email'
       , UC.occupation as 'contact-jobTitle'
       , num_job.amount as 'Number of Jobs linked'

-- select count(*) --19302 -- select distinct convert(varchar(max),desiredSkills) as skills -- select top 10 * -- select distinct status
from bullhorn1.View_ClientContact Cl left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
left join ed ON ed.ID = Cl.clientID
left join (select clientCorporationID, name from bullhorn1.BH_ClientCorporation /*where status <> 'Archive'*/) com on com.clientCorporationID = UC.clientCorporationID
--left join dup_con on dup_con.userID = cl.userID
left join num_job on num_job.position_contactId = convert(varchar(2000),Cl.clientID)
where Cl.isdeleted <> 1
--and cl.userID in ( select userid from dup_con)e
and uc.email in ( select email from dup_con) and ed.ID is not null
--and dup_con.userID is not null
--and (UC.firstName like '%Laura%' and UC.lastName like '%Auchterlonie%')
