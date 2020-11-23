
with
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
                     where (Cl.isdeleted <> 1/* and Cl.status <> 'Archive'*/)
              ) n where rn = 1
              ) company on company.userID = a.clientUserID
       where (a.isdeleted <> 1 /*and a.status <> 'Archive'*/)
       --and a.clientcorporationid in (51)
       --and a.jobPostingID in (18,19,20)
)
--select * from job0 where CompanyName = 'Concept Group (bust??)' --contact_externalId like '%default%'
--select * from job0 where Company_externalID <> COMPANY_externalID_of_ContactUserID
select distinct Company_externalID as 'OLD_Company_externalID', CompanyName as 'OLD_CompanyName', contact_externalId, ContactUserID, Contact_firstname, Contact_lastname, Company_externalID_of_ContactUserID, CompanyName_of_ContactUserID from job0 where Company_externalID <> COMPANY_externalID_of_ContactUserID


