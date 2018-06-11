 create table Temp_WorkHistory
(ApplicantId int PRIMARY KEY,
WorkHistory nvarchar(max)
)
go

with TempWorkHistory as (select a.ApplicantId, wh.WorkHistoryId, wh.PersonName, wh.ClientID, wh.Company, wh.PlacementID,
 wh.FromDate, wh.ToDate, wh.Description JobTitle, wh.EmploymentTypeId, et.Description JobType, wh.PositionAttributeId, att.Description Position,
 p.WorkAddress, p.Salary, p.CommissionPerc, p.PlacementFee, 
 ROW_NUMBER() OVER(PARTITION BY a.ApplicantId ORDER BY wh.WorkHistoryId ASC) AS rn
from Applicants a
 left join WorkHistory wh on a.ApplicantId = wh.ApplicantID
 left join Placements p on wh.placementID = p.placementId
 left join EmploymentTypes et on wh.EmploymentTypeId = et.EmploymentTypeId
 left join Attributes att on wh.PositionAttributeId = att.AttributeId
 where wh.WorkHistoryId is not null)
, CombinedWorkHistory as(select ApplicantId, 
	ltrim(rtrim(concat(
	  iif(WorkHistoryId is NULL,'',concat('------Employer ',rn,': '))
	, iif(Company = '' or Company is NULL,'',concat(char(10),'Company Name: ',Company))
	, iif(JobTitle = '' or JobTitle is NULL,'',concat(char(10),'Job Title: ',JobTitle))
	, iif(FromDate = '' or FromDate is NULL,'',concat(char(10),'Start Date: ',convert(varchar(10),FromDate, 120)))
	, iif(ToDate = '' or ToDate is NULL,'',concat(char(10),'End Date: ',convert(varchar(10),ToDate, 120)))
	, iif(JobType = '' or JobType is NULL,'',concat(char(10),'Job Type: ',JobType))
	, iif(Position = '' or Position is NULL,'',concat(char(10),'Position: ',Position))
	, iif(WorkAddress = '' or WorkAddress is NULL,'',concat(char(10),'Work Address: ',WorkAddress))
	, iif(Salary is NULL,'',concat(char(10),'Salary: ',Salary))
	, iif(CommissionPerc is NULL,'',concat(char(10),'Commission %: ',CommissionPerc))
	, iif(PlacementFee is NULL,'',concat(char(10),'Placement Fee: ',PlacementFee,char(10)))
	))) as WorkHistory
	, rn
	from TempWorkHistory)
--select* from CombinedWorkHistory

insert into Temp_WorkHistory SELECT ApplicantId, 
     STUFF(
         (SELECT char(10) + WorkHistory
          from  CombinedWorkHistory
          WHERE ApplicantId =cwh.ApplicantId
    order by ApplicantId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          ,1,1, '')  AS WorkHistory
FROM CombinedWorkHistory as cwh
GROUP BY cwh.ApplicantId