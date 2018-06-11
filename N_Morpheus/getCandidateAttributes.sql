select * from VW_APPLICANT_INFO
select * from VW_APPLICANT_GRID_VIEW
select * from Applicants
select * from Person
select * from Attributes
select * from INFORMATION_SCHEMA.COLUMNS 
where COLUMN_NAME like '%Attribute%' 
order by TABLE_NAME

select --oa.ObjectID, oa.ObjectAttributeId, oa.AttributeId, 
distinct (a.Description)
from ObjectAttributes oa left join Attributes a on oa.AttributeId = a.AttributeId
left join Applicants can on oa.ObjectID = can.ApplicantId
where can.ApplicantId is not null
select * from ApplicantExcludedAreas--no
select * from ApplicantRates--no
select * from CVSendDocDefinitionMap--no
select * from Placements

select --c.ClientId, oa.ObjectID, oa.ObjectAttributeId, oa.AttributeId, a.Description, a.Notes
distinct (oa.ObjectID)
from ObjectAttributes oa left join Attributes a on oa.AttributeId = a.AttributeId
left join Clients c on oa.ObjectID = c.ClientId
where c.ClientId is not null
order by ClientID

select * from Attributes