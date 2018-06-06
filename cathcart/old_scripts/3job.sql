
with
--JOB DUPLICATION REGCONITION
 job (UniqueID,SiteUniqID,RoleDescription,CreationDate,rn) as (
	SELECT  a.UniqueID as UniqueID
		, a.SiteUniqID as SiteUniqID
		, a.RoleDescription as RoleDescription
		, CONVERT(varchar(10), CONVERT(date, a.CreationDate, 103), 120) as CreationDate
		, ROW_NUMBER() OVER(PARTITION BY a.SiteUniqID,a.RoleDescription,CONVERT(varchar(10), CONVERT(date, a.CreationDate, 103), 120) ORDER BY a.UniqueID) AS rn 
	from vacancies a )
--select * from job

select
         v.UniqueID as 'position-externalId' 
        ,am1.email as 'position-owners' --v.MainConsultant
        ,coalesce( nullif(con.ContactUniqueID,''),concat(v.SiteUniqID,'_','defaultcontact') ) as 'position-contactId'
        ,case when job.rn > 1 then concat(job.RoleDescription,' ',rn) else coalesce(nullif(job.RoleDescription,''),'No Job Title') end as 'position-title' --,v.RoleDescription as 'position-title'
        ,case v.VacancyType when 'PERM' then 'PERMANENT' when 'CONT' then 'CONTRACT' end as 'position-type'
        ,v.SiteUniqID as 'CompanyID#', cl.AccountName as 'company-name#'
        ,v.NoOfPositions as 'position-headcount'
        --,v.CreationDate as 'position-startDate'
        ,CONVERT(varchar(10), CONVERT(date, v.CreationDate, 103), 120) as 'position-startDate'
        ,nullif(CONVERT(varchar(10), CONVERT(date, v.DatePlaced, 103), 120),'1900-01-01') as 'position-endDate'
        ,case when v.Currency = 'B' then 'THB' when v.Currency = '£' then 'GBP' end as 'position-currency'
        , Stuff( 
                  Coalesce('Creating User: ' + NULLIF(cast(am2.fullname as varchar(max)), '') + char(10), '') --v.CreatingUser
                + Coalesce('Date Placed: ' + NULLIF(cast(v.DatePlaced as varchar(max)), '') + char(10), '')
                + Coalesce('From Salary: ' + NULLIF(cast(v.FromSalary as varchar(max)), '') + char(10), '')
                + Coalesce('To Salary: ' + NULLIF(cast(v.ToSalary as varchar(max)), '') + char(10), '')
                + Coalesce('Work Location: ' + NULLIF(cast(v.WorkLocation as varchar(max)), '') + char(10), '')
                + Coalesce('Business Type: ' + NULLIF(cast(v.BusinessType as varchar(max)), '') + char(10), '')
                + Coalesce('Status: ' + NULLIF(cast(ct.description as varchar(max)), '') + char(10), '') --v.Status
                + Coalesce('Report To UniqueID: ' + NULLIF(cast(con.name as varchar(max)), '') + char(10), '') --v.ReportToUniqueID
                + Coalesce('Stage Reached: ' + NULLIF(cast(v.StageReached as varchar(max)), '') + char(10), '')
                , 1, 0, '') as 'contact-note'        
-- select count(*) --2362 -- select distinct v.VacancyType --select distinct v.Currency -- select top 100 * 
from vacancies v
left join (select distinct am.[user],am.fullname,am.email from AccountManager am left join vacancies v on am.[user] = v.MainConsultant) am1 on am1.[user] = v.MainConsultant
left join (select distinct am.[user],am.fullname,am.email from AccountManager am left join vacancies v on am.[user] = v.CreatingUser) am2 on am2.[user] = v.CreatingUser
--left join (select distinct am.[user],am.fullname,am.email from AccountManager am left join vacancies v on am.[user] = v.ReportToUniqueID) am3 on am3.[user] = v.ReportToUniqueID
left join Clients cl on cl.UniqueID = v.SiteUniqID
left join job on v.UniqueID = job.UniqueID
left join (select ContactUniqueID, concat(Forename,' ',Surname) as name from contacts) con on con.ContactUniqueID = v.ReportToUniqueID
left join (select code,description from codetables where TabName = 'Vac Status Code') ct on ct.code = v.status

/*
select v.UniqueID,v.ReportToUniqueID,concat(con.Forename,' ',con.Surname) as name,v.SiteUniqID, cl.AccountName
from vacancies v
left join contacts con on con.ContactUniqueID = v.ReportToUniqueID
left join Clients cl on cl.UniqueID = v.SiteUniqID
where con.ContactUniqueID is null
*/

/*
with t as (
select 
         distinct v.SiteUniqID as 'CompanyID', cl.AccountName as 'company-name#'
        ,con.ContactUniqueID as 'contact-owner'
from vacancies v
left join Clients cl on cl.UniqueID = v.SiteUniqID
left join (select ContactUniqueID, concat(Forename,' ',Surname) as name from contacts) con on con.ContactUniqueID = v.ReportToUniqueID
where con.ContactUniqueID is null )

select t.[CompanyID] as 'contact-companyId'
        ,concat(t.[CompanyID],'_','defaultcontact') as 'contact-externalid'
        --,concat(t.[CompanyID],'_','defaultcontact','@noemail.com') as 'contact-email'
        ,'Default Contact' as 'contact-firstName'
        ,'Default Contact' as 'contact-lastName'
from t
--left join contacts c on c.SiteUnique = t.[CompanyID#] where c.SiteUnique is null
*/
