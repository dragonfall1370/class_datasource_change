
with
job0 as (
       select
                a.jobPostingID
              , a.dateadded
              , iif(a.title is not null and a.title <> '', trim(a.title), 'No JobTitle') as title
              , convert(varchar(10),a.startDate,120) as startDate
              , iif(a.clientcorporationid is null or a.clientcorporationid = '', 'default', a.clientcorporationid) as 'Company_externalID', cc.name as 'CompanyName' --COMPANY
              , iif(company.contact_externalId is null or convert(varchar(500),company.contact_externalId) = '', 'default', convert(varchar(500),company.contact_externalId)) as 'contact_externalId', a.clientUserID as 'ContactUserID', uc.firstname as 'Contact_firstname', uc.lastname as 'Contact_lastname'--, uc.isdeleted as '#Contact_isdeleted', uc.status as '#Contact_status'
              
              , company.[contact_companyId] as 'COMPANY_externalID_of_ContactUserID'
              , company.company_name as 'COMPANYNAME_of_ContactUserID'
       -- select count(*)                            
       from bullhorn1.BH_JobPosting a
       left join bullhorn1.BH_UserContact UC on a.clientUserID = UC.userID
       left JOIN ( select clientCorporationID, name, status from bullhorn1.BH_ClientCorporation CC) cc ON cc.clientCorporationID = a.clientcorporationid
       left join (
              select * 
              from (
                     select
                              Cl.clientID as 'contact_externalId', Cl.userID as 'UserID'
                            , UC.clientCorporationID as 'contact_companyId'
                            , com.name as 'company_name'
                            , ROW_NUMBER() OVER(PARTITION BY Cl.UserID, UC.clientCorporationID, com.name ORDER BY Cl.clientID desc) AS rn 
                     -- select *
                     from bullhorn1.BH_Client Cl
                     left join (select distinct userID,clientCorporationID from bullhorn1.BH_UserContact) UC ON Cl.userID = UC.userID
                     left join (select clientCorporationID, name from bullhorn1.BH_ClientCorporation) com on com.clientCorporationID = UC.clientCorporationID
                     where (Cl.isdeleted <> 1) --and Cl.userID in (579,605)
              ) n where rn = 1
              ) company on company.userID = a.clientUserID
       where (a.isdeleted <> 1)
)
select * from job0 where contact_externalId like '%default%'
select * from job0 where Company_externalID <> COMPANY_externalID_of_ContactUserID

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
		     when Company_externalID <> Company_externalID_of_ContactUserID then iif(contact_externalId is null or contact_externalId = '', concat('default',convert(varchar(5000),Company_externalID)), concat('default',convert(varchar(5000),contact_externalId)) )
		     end as 'final_contact'
		, ContactUserID, Contact_firstname, Contact_lastname
              , Company_externalID_of_ContactUserID
              , CompanyName_of_ContactUserID
		, ROW_NUMBER() OVER(PARTITION BY contact_externalId,title,startDate ORDER BY jobPostingID) AS rn 
	from job0
	)
select * from job where jobPostingID in (5224)

-- >>> CREATE DEFAULT CONTACT LIST FOR JOB <<< ---
--select distinct Company_externalID as 'contact-companyId', final_contact_externalId as 'contact-externalId', Contact_firstname as 'contact-firstname', Contact_lastname as 'contact-lastname' from job where final_contact_externalId like 'default%' order by Company_externalID desc





select
         a.jobPostingID
       , a.dateadded
       , iif(a.title is not null and a.title <> '', trim(a.title), 'No JobTitle') as title
       , convert(varchar(10),a.startDate,120) as startDate
       , a.clientUserID
       , a.clientcorporationid
from bullhorn1.BH_JobPosting a where jobPostingID in (18,19,20)              

