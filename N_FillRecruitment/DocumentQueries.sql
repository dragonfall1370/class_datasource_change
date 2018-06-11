with DocumentName as (select doc.DocumentId, doc.NotebookItemId, concat('DC',doc.DocumentID,dc.FileExtension) as DocumentFullName
from Documents doc left join DocumentContent dc on doc.DocumentID = dc.DocumentId)
, tempItems as(SELECT NotebookItemId, 
     STUFF(
         (SELECT ',' + DocumentFullName
          from  DocumentName
          WHERE NotebookItemId = dn.NotebookItemId
    order by DocumentID asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS Document
FROM DocumentName as dn
GROUP BY dn.NotebookItemId)
--select * from tempItems
, tempObjects as (select ti.NotebookItemId, ti.Document, nl.ObjectId, nl.ClientId, nl.JobId
from tempItems ti left join NotebookLinks nl on ti.NotebookItemId = nl.NotebookItemId) 
--select * from tempObjects
--select NotebookItemId, Count(NotebookItemId) from tempObjects group by NotebookItemId having count(NotebookItemId) >1--where Objectid is not null order by ObjectId
, ObjectDocs as (SELECT ObjectId, 
     STUFF(( SELECT ',' + Document
          from  TempObjects
          WHERE ObjectID = toj.ObjectId
    order by ObjectId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS ObjectDocs
FROM TempObjects as toj
GROUP BY toj.Objectid)

, ObjectDocsTypes as (select od.ObjectId, od.ObjectDocs, o.ObjectTypeId
 from ObjectDocs od left join Objects o on od.ObjectId = o.ObjectID)
 select * from ObjectDocsTypes where ObjectId in (select ContactPersonId from ClientContacts)

-- select * from ObjectTypes
--select * from Person where PersonID = 3816
--select * from VW_CONTACT_GRID_VIEW where ContactPersonID = 12458

select nl.Objectid, nl.NotebookItemId, doc.DocumentID
from NotebookLinks nl
	 left join Documents doc on nl.NotebookItemId = doc.NotebookItemId