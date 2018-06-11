 create table Temp_WorkHistory_Production
(ApplicantId int PRIMARY KEY,
WorkHistory nvarchar(max)
)
go

with TempWorkHistory as (select a.ApplicantId, wh.WorkHistoryId, wh.PersonName, wh.ClientID, wh.Company, wh.PlacementID,
 wh.FromDate, wh.ToDate, wh.Description JobTitle, wh.EmploymentTypeId, et.Description JobType, wh.PositionAttributeId, att.Description Position,
 p.WorkAddress, p.Salary, p.CommissionPerc, p.PlacementFee,p.Description as PlacementDesc,p.InvoiceAddress, wh.Notes,
 ROW_NUMBER() OVER(PARTITION BY a.ApplicantId ORDER BY wh.WorkHistoryId ASC) AS rn
from Applicants a
 left join WorkHistory wh on a.ApplicantId = wh.ApplicantID
 left join Placements p on wh.placementID = p.placementId
 left join EmploymentTypes et on wh.EmploymentTypeId = et.EmploymentTypeId
 left join Attributes att on wh.PositionAttributeId = att.AttributeId
 where wh.WorkHistoryId is not null)
, CombinedWorkHistory as(select ApplicantId, 
	ltrim(rtrim(concat(
	  iif(WorkHistoryId is NULL,'',concat('------Employer ',rn,': ',Company,char(10)))
	--, iif(Company = '' or Company is NULL,'',concat('Company Name: ',Company,char(10)))
	, iif(JobTitle = '' or JobTitle is NULL,'',concat('Job Title: ',JobTitle,char(10)))
	, iif(FromDate = '' or FromDate is NULL,'',concat('Start Date: ',convert(varchar(10),FromDate, 120),char(10)))
	, iif(ToDate = '' or ToDate is NULL,'',concat('End Date: ',convert(varchar(10),ToDate, 120),char(10)))
	, iif(JobType = '' or JobType is NULL,'',concat('Job Type: ',JobType,char(10)))
	, iif(Position = '' or Position is NULL,'',concat('Position Attribute: ',Position,char(10)))
	, iif(PlacementDesc = '' or PlacementDesc is NULL,'',concat('Placement Description: ',PlacementDesc,char(10)))
	, iif(InvoiceAddress = '' or InvoiceAddress is NULL,'',concat('Invoice Address: ',InvoiceAddress,char(10)))
	, iif(WorkAddress = '' or WorkAddress is NULL,'',concat('Work Address: ',WorkAddress,char(10)))
	, iif(Salary is NULL,'',concat('Salary: ',Salary,char(10)))
	, iif(CommissionPerc is NULL,'',concat('Commission %: ',CommissionPerc,char(10)))
	, iif(PlacementFee is NULL,'',concat('Placement Fee: ',PlacementFee,char(10)))
	, iif(Notes = '' or Notes is NULL,'',concat('Notes: ',Notes,char(10)))
	))) as WorkHistory
	, rn
	from TempWorkHistory)
--select * from CombinedWorkHistory
insert into Temp_WorkHistory_Production SELECT ApplicantId, 
     STUFF(
         (SELECT char(10) + WorkHistory
          from  CombinedWorkHistory
          WHERE ApplicantId =cwh.ApplicantId
    order by ApplicantId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          ,1,1, '')  AS WorkHistory
FROM CombinedWorkHistory as cwh
GROUP BY cwh.ApplicantId
--select * from Temp_WorkHistory