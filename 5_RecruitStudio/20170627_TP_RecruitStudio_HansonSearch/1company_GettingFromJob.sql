-----------

with
 job (JobNumber,ContactId,JobTitle,startdate,rn) as (
	SELECT  a.JobNumber as JobNumber
		, a.ContactId as ContactId
		, Coalesce(NULLIF(a.JobTitle, ''), 'Job Title') as JobTitle
		, CONVERT(VARCHAR(10),a.startdate,120) as startdate
		, ROW_NUMBER() OVER(PARTITION BY a.ContactId,a.JobTitle,CONVERT(VARCHAR(10),a.startdate,120) ORDER BY a.ContactId) AS rn 
	from vacancies a )
, c as (
        select
                CC.CompanyId as 'company-externalId'
                , case when CC.CompanyName in (select CC.CompanyName from dbo.Companies CC group by CC.CompanyName having count(*) > 1) then concat (dup.CompanyName,' ',dup.rn)
                        when (CC.CompanyName = '' or CC.CompanyName is null) then 'No CompanyName'
                        else CC.CompanyName end as 'companyname'
        -- select count(*) --17293
        from Companies CC
        left join (SELECT CompanyId,CompanyName,ROW_NUMBER() OVER(PARTITION BY CC.CompanyName ORDER BY CC.CompanyId DESC) AS rn FROM Companies CC) dup on CC.CompanyId = dup.CompanyId
        )

, nc as (select   j.JobNumber as 'position-externalId'
                , case when job.rn > 1 then concat(job.JobTitle,' ',rn) else job.JobTitle end as 'position-title'
                , j.VacancyId
                , iif(j.company = '','No CompanyName', ltrim(rtrim(j.company)) ) as 'company-name'
                , cast('61187' as int) as wid
        from c
        --left join vacancies j on c.companyname = j.company
        right join vacancies j on c.companyname = j.company --(USE THIS STATEMENT & COMMENT OUT ABOVE LINE TO FIND COMPANIES ARE NOT EXIT IN COMPANY TABLE)
        left join job on job.JobNumber = j.JobNumber
        where j.JobNumber is not null
        )

--select count(*) from nc
select * from nc where [position-externalId] in ('163239-6035-17117','851856-7893-1780') --('102826-2798-7151','116318-9169-174','118860-8879-12227','135227-6745-16258','151929-5996-1513','153383-2272-12227','157877-3606-1684','163239-6035-17117','165025-4415-1684','168969-1517-12276','169990-4635-17144','174042-1154-661','212882-5292-7283','231586-8200-16140','23195/198','256056-7524-6179','258859-7494-9187','265924-2781-1083','284630-9513-11313','291312-3946-1740','294724-3371-12334','296133-9720-16195','326589-6313-5285','346162-4983-616','355304-5651-1282','356865-5379-14289','358470-6116-6173','359931-7314-13248','362291-8115-7311','364623-4682-9274','375558-4081-1335','383773-8956-11229','383781-9715-16336','404482-6738-12276','431380-7959-15183','440576-9240-16173','447847-5705-1224','463970-8650-17153','467221-7676-14140','474300-5323-1374','497654-4403-1083','503521-2101-10181','51741/219','521809-4406-9188','535140-4146-12275','563126-2267-17116','584415-1407-14280','607681-2268-14308','617996-3151-9257','620816-6723-7354','622448-3168-1629','627569-2015-7319','645257-2609-9232','645681-7427-13262','714834-3315-7354','718738-7220-1732','729272-4873-1692','741495-1312-16308','751497-1337-7310','756719-7649-13262','761440-2406-16224','767301-6794-16243','772559-3626-14217','772893-5127-1323','778631-6357-15238','792894-3864-9275','801472-6493-13284','802406-9426-15183','831662-1350-12276','841855-4624-14275','845235-5890-9188','851856-7893-1780','854432-3735-7144','863672-2512-1692','864217-9011-7354','866574-6217-12116','871028-1261-1690','871361-2824-16314','879819-3136-16186','888678-1960-1019','905754-5095-12296','906684-1227-682','909119-7921-15205','921196-5699-9253','926394-5122-1691','932540-8423-16153','935127-5293-16216','946508-3377-14289','954863-4399-10249','963376-3429-16341','966717-3508-16259','977045-2675-16312','991523-2781-15238','TB/R/000093','V/Alic/95947/112','V/Ta/79670/93') --VacancyId = '1650' --6674
--select top 100 * from nc
--select distinct [company-name] from nc
--select distinct company,companyid from nc




/*
select
          nc.JobNumber
        , nc.ContactId
        , nc.UserName
        , nc.DisplayName
        , nc.Company
from nc --1540
left join (select contactid from Contacts where descriptor = 1) CO on co.contactid = nc.contactid
--where
--nc.VacancyId = '1650'
--co.contactid is null
--select top 100 * from Contacts
*/