create table Morpheus_CVInfo_production
(ApplicantId int PRIMARY KEY,
CVInfo nvarchar(max)
)
go

with TempCV as (select a.ApplicantId, CV.CVId, CV.CVRefNo, CV.SectorId, s.SectorName, CV.Publish, CV.Description, CV.CreatedOn, CV.UpdatedOn,
	ROW_NUMBER() OVER(PARTITION BY CV.ApplicantId ORDER BY CV.CVId ASC) AS rn
 from Applicants a left join CV on a.ApplicantId = CV.ApplicantId
					left join Sectors s on CV.SectorId = s.SectorId
					where CVId is not null)

, TempCV1 as(select ApplicantId, 
	ltrim(rtrim(concat(
	  iif(CVId is NULL,'',concat('---CV',CVId,': '))
	, iif(CVRefNo = '' or CVRefNo is NULL,'',concat(char(10),'CV Ref No. ',CVRefNo))
	, iif(SectorName = '' or SectorName is NULL,'',concat(char(10),'Sector: ',SectorName))
	, iif(Publish = '' or Publish is NULL,'',iif(Publish = 'N',concat(char(10),'Published: No'), concat(char(10),'Published: Yes')))
	, iif(Description = '' or Description is NULL,'',concat(char(10),'Description: ',Description))
	, iif(CreatedOn = '' or CreatedOn is NULL,'',concat(char(10),'Created on: ',CreatedOn))
	, iif(UpdatedOn = '' or UpdatedOn is NULL,'',concat(char(10),'Updated on: ',UpdatedOn))))) as CVInfo
	, rn
	from TempCV)
--	SELECT ApplicantId, 
--     STUFF(
--         (SELECT char(10) + CVInfo
--          from  TempCV1
--          WHERE ApplicantId = tcv.ApplicantId
--    order by ApplicantId asc
--          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
--          ,1,1, '')  AS CVInfo
--FROM TempCV1 as tcv
--GROUP BY tcv.ApplicantId
--order by tcv.ApplicantId
--select * from TempCV1
insert into Morpheus_CVInfo_production SELECT ApplicantId, 
     STUFF(
         (SELECT char(10) + CVInfo
          from  TempCV1
          WHERE ApplicantId = tcv.ApplicantId
    order by ApplicantId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          ,1,1, '')  AS CVInfo
FROM TempCV1 as tcv
GROUP BY tcv.ApplicantId