select * from NotebookLinks
select * from DocumentStatus
select * from VW_APPLICANT_ACTION_GRID
select * from NotebookTypeSectors
------------------------
select * from templateDocument
select * from Templates
select * from TemplateTypes
--select * from templateusers--no use
--select * from TemplateSectors--no use
--select * from tempIds-- no use
--select * from templategroups--no use
-------------------Stored Documents of Company
select tpl.TemplateId, TemplateName,Description,ObjectId,tpl.ClientId,FileExtension, tpl.TemplateTypeId, tplt.TemplateTypeName
from Templates tpl left join TemplateTypes tplt on tpl.templateTypeId = tplt.templatetypeId
left join TemplateDocument td on tpl.templateId = td.TemplateId
where tpl.ClientId is not null
-- and ObjectiD is null--) and
-- tpl.TemplatetypeID in (48,59,62,66,34)
--left join ClientContacts cc on tpl.ObjectId = cc.ContactPersonId
--Where (ObjectId is not null or tpl.ClientId is not null)
--and tpl.TemplatetypeID in (48,59,62,66,34)
--and ObjectId in (select ApplicantID from Applicants)-- or ObjectId in (select contactpersonid from ClientContacts))
----------------------------Contact
select tpl.TemplateId, TemplateName,Description,ObjectId,tpl.ClientId,FileExtension, tpl.TemplateTypeId, tplt.TemplateTypeName
from Templates tpl left join TemplateTypes tplt on tpl.templateTypeId = tplt.templatetypeId
left join TemplateDocument td on tpl.templateId = td.TemplateId
left join ClientContacts cc on tpl.ObjectId = cc.ContactPersonId
where cc.ContactPersonId is not null
----------------------------Candidate
select tpl.TemplateId, TemplateName,Description,ObjectId,tpl.ClientId,FileExtension, tpl.TemplateTypeId, tplt.TemplateTypeName
from Templates tpl left join TemplateTypes tplt on tpl.templateTypeId = tplt.templatetypeId
left join TemplateDocument td on tpl.templateId = td.TemplateId
left join Applicants a on tpl.ObjectId = a.ApplicantId
where a.ApplicantId is not null
--and tpl.TemplatetypeID in (48,59,62,66,34)
select distinct(FileExtension)
from Templates tpl left join TemplateTypes tplt on tpl.templateTypeId = tplt.templatetypeId
left join TemplateDocument td on tpl.templateId = td.TemplateId
left join Applicants a on tpl.ObjectId = a.ApplicantId
where a.ApplicantId is not null