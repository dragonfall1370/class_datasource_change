/*

with 
--JOB DUPLICATION REGCONITION
  job (jobPostingID,clientID,title,starDate,rn) as (
	SELECT  a.jobPostingID as jobPostingID
		, b.clientID as clientID
		, a.title as title
		, CONVERT(VARCHAR(10),a.startDate,120) as starDate
		, ROW_NUMBER() OVER(PARTITION BY a.clientUserID,a.title,CONVERT(VARCHAR(10),a.startDate,120) ORDER BY a.jobPostingID) AS rn 
	from bullhorn1.BH_JobPosting a
	left join bullhorn1.BH_Client b on a.clientUserID = b.userID
	where b.isPrimaryOwner = 1) --> add isPrimaryOwner = 1 to remove 1 userID having more than 1 clientID
--select * from job where title = ''


,placementnote as (
       select jobPostingID
	, Stuff((select  char(10)
	        + Coalesce('Date Added: ' + cast(PL.dateAdded as varchar(max)) + char(10), '')
	        + Coalesce('Status: ' + NULLIF(PL.status, '') + char(10), '')
	        + Coalesce('Base Pay Rate: ' + NULLIF(cast(PL.payRate as varchar(max)), '') + char(10), '')
	        + Coalesce('Charge Rate: ' + NULLIF(cast(PL.clientBillRate as varchar(max)), '') + char(10), '')
	        + Coalesce('Custom Text 3: ' + NULLIF(cast(PL.customText3 as varchar(max)), '') + char(10), '')
	        + Coalesce('Report to: ' + NULLIF(cast(PL.reportTo as varchar(max)), '') + char(10), '')
	        + Coalesce('Comments: ' + NULLIF(cast(PL.comments as varchar(max)), '') + char(10), '')
	        -- select count(*) --1559 -- select *
	        from bullhorn1.BH_Placement PL
                left join bullhorn1.BH_UserContact UC on PL.billingUserID = UC.userID
                WHERE PL.jobPostingID = a.jobPostingID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 0, '') as note
        from bullhorn1.BH_Placement a group by a.jobPostingID
)
--select count(*) from placementnote where jobPostingID in (17,28,30,50,92,115)
--select jobPostingID from placementnote group by jobPostingID having count(*) > 1



select --top 100 
         concat(a.jobPostingID,,) as 'position-externalId' 
	, a.clientUserID as '#UserID'
	, b.clientID as 'position-contactId', uc.firstname as '#ContactFirstName',uc.lastname as '#ContactLastName'
	, b.clientCorporationID as '#CompanyID', cc.name as '#CompanyName'
	, case when job.rn > 1 then concat(job.title,' ',rn) else job.title end as 'position-title'
	, a.startDate, a.status, a.dateEnd, a.payRate
	, placementnote.note as 'position-note' --left(,32000)
from bullhorn1.BH_JobPosting a --where a.jobPostingID in (544,843,725,964,1109,1225,1323,1409,1444,1471,1540) --(76938, 100453, 120112)
left join bullhorn1.BH_Client b on a.clientUserID = b.userID --where b.isPrimaryOwner = 1
left JOIN bullhorn1.BH_ClientCorporation CC ON b.clientCorporationID = CC.clientCorporationID
left join bullhorn1.BH_UserContact UC on a.clientUserID = UC.userID
left join job on a.jobPostingID = job.jobPostingID
left join placementnote  on a.jobPostingID = placementnote.jobPostingID
where b.isPrimaryOwner = 1 --> add isPrimaryOwner = 1 to remove 1 userID having more than 1 clientID
and job.title <> ''
--and a.jobPostingID in (1832) --(544,843,725,964,1109,1225,1323,1409,1444,1471,1540)
--and a.jobPostingID in (185,164,178,36)
order by a.jobPostingID asc

*/



with 
-- MAIL
  mail1 (ID,email) as (select userID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(ltrim(rtrim(email)),',',ltrim(rtrim(email2)),',',ltrim(rtrim(email3))),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' ') as email from bullhorn1.BH_UserContact )
, mail2 (ID,email) as (SELECT ID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT ID, CAST ('<M>' + REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data FROM mail1) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail5 (ID,email) as (SELECT ID, STUFF((SELECT DISTINCT ', ' + email from mail3 WHERE ID = a.ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '')  AS email FROM mail3 as a GROUP BY a.ID)
--select * from mail5 where id in (39188,14248,30223)

select --top 100 
         concat(PL.jobPostingID,'_',PL.ID) as 'position-externalId'
	, b.clientID as 'position-contactId'
	, a.clientUserID as '#UserID', uc.firstname as '#ContactFirstName', uc.lastname as '#ContactLastName', CC.clientCorporationID as '#CompanyID', cc.name as '#CompanyName'
       , concat(a.title,' - ',PL.jobPostingID, ' - ',PL.ID) as 'position-title'
       --, a.title as 'position-title'
       --, concat(a.title,'.') as 'position-title'
	, PL.E_mailaddressconsultants as 'position-owners'
       , case
	       when PL.employmentType = 'Contract' then 'CONTRACT'
	       when PL.employmentType = 'Extension' then 'CONTRACT'
              when PL.employmentType = ' Extension Contract' then 'CONTRACT'
	       when PL.employmentType = 'Extension Contract' then 'CONTRACT'
	       when PL.employmentType = 'Extension Margin only' then 'CONTRACT'
	       when PL.employmentType = 'Margin only' then 'CONTRACT'
	       when PL.employmentType = 'Perm' then 'PERMANENT'
	       when PL.employmentType = 'Rolling' then 'TEMPORARY'
	       else '' end as 'position-type'
	       
	, Stuff(
	            Coalesce('JobID: ' + cast(PL.jobPostingID as varchar(max)) + char(10), '')
	        +  Coalesce('PlacementID: ' + cast(PL.ID as varchar(max)) + char(10), '')
	        + Coalesce('Date Added: ' + cast(PL.dateAdded as varchar(max)) + char(10), '')
	        + Coalesce('Employment Type: ' + NULLIF(PL.employmentType, '') + char(10), '')
	        + Coalesce('Status: ' + NULLIF(PL.status, '') + char(10), '')
	        + Coalesce('Base Pay Rate: ' + NULLIF(cast(PL.payRate as varchar(max)), '') + char(10), '')
	        + Coalesce('Charge Rate: ' + NULLIF(cast(PL.clientBillRate as varchar(max)), '') + char(10), '')
	        + Coalesce('Custom Text 3: ' + NULLIF(cast(PL.customText3 as varchar(max)), '') + char(10), '')
	        + Coalesce('Report to: ' + NULLIF(cast(PL.reportTo as varchar(max)), '') + char(10), '')
	        + Coalesce('Work Start Date: ' + NULLIF(cast(PL.dateBegin as varchar(max)), '') + char(10), '')
	        + Coalesce('Work End Date: ' + NULLIF(cast(PL.dateEnd as varchar(max)), '') + char(10), '')
	        + Coalesce('Comments: ' + NULLIF(cast(PL.comments as varchar(max)), '') + char(10), '')
	        , 1, 0, '') as 'position-note'
       , pl.customText1 as 'job-Currency'
       , PL.payRate as 'Base Pay Rate'
       , PL.clientBillRate as 'Charge Rate'
       , PL.dateBegin as 'Work Start Date'
       , PL.dateEnd as 'Work End Date'
	        -- select count(*) --1559 -- select * -- select distinct pl.employmentType
from bullhorn1.BH_JobPosting a --where a.jobPostingID in (544,843,725,964,1109,1225,1323,1409,1444,1471,1540) --(76938, 100453, 120112)
left join (select PL.jobPostingID,PL.comments, PL0.*  from BullhornPlacements PL0 left join bullhorn1.BH_Placement PL on PL0.id = PL.placementID ) PL  on PL.jobPostingID = a.jobPostingID
left join bullhorn1.BH_Client b on a.clientUserID = b.userID --where b.isPrimaryOwner = 1
left JOIN bullhorn1.BH_ClientCorporation CC ON b.clientCorporationID = CC.clientCorporationID
left join bullhorn1.BH_UserContact UC on a.clientUserID = UC.userID
--left join mail5 ON a.userID = mail5.ID
where b.isPrimaryOwner = 1
--where b.isPrimaryOwner = 1 --> add isPrimaryOwner = 1 to remove 1 userID having more than 1 clientID
--and a.jobPostingID in (1,164,178,36)
and PL.jobPostingID is not null
--order by a.jobPostingID asc
