--------------------------------------------------Candidate's Attribute Master and Attribute
with temp_CanAttributeMaster as(
select oa.ObjectID, can.ApplicantId, oa.ObjectAttributeId, am.AttributeMasterId, am.Description as AttributeMaster, a.Description, a.Notes,
	oa.AttributeId, iif(a.Notes = a.Description or a.Notes = '' or a.Notes is NULL,a.Description,concat(a.Description,' (',a.Notes,')')) as Attribute,
	ROW_NUMBER() OVER(PARTITION BY can.ApplicantId ORDER BY am.Description ASC) AS rn 
from ObjectAttributes oa left join Attributes a on oa.AttributeId = a.AttributeId
left join Applicants can on oa.ObjectID = can.ApplicantId
left join AttributeMaster am on a.AttributeMasterId = am.AttributeMasterId
where can.ApplicantId is not null)-- and a.AttributeMasterId is not null
--oa.ObjectID in (select ApplicantId from Applicants)
, CanAttributeMaster as (SELECT ApplicantId, 
     STUFF(
         (SELECT '; ' + AttributeMaster
          from  temp_CanAttributeMaster
          WHERE ApplicantId = tcam.ApplicantId
    order by ApplicantId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS 'AttributeMaster'
FROM temp_CanAttributeMaster as tcam
GROUP BY tcam.ApplicantId)
--select * from CanAttributeMaster
, CanAttribute as (SELECT ApplicantId, 
     STUFF(
         (SELECT '; ' + Attribute
          from  temp_CanAttributeMaster
          WHERE ApplicantId = tcam.ApplicantId
    order by ApplicantId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS 'Attribute'
FROM temp_CanAttributeMaster as tcam
GROUP BY tcam.ApplicantId)

select * from temp_CanAttributeMaster 
where AttributeMasterId in (494,412,493,592,413,601,600,596,590,405,599,498,487,597,477,440,442,473,595,476,475,588,575,602,496)
