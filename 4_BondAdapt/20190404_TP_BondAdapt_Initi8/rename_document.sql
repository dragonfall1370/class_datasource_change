, doc(OWNER_ID, DOC_ID) as ( SELECT OWNER_ID, STRING_AGG( concat(cast(DOC_ID as varchar(max)),'.',FILE_EXTENSION) ,',') WITHIN GROUP (ORDER BY DOC_ID) DOC FROM DOCUMENTS where DOC_CATEGORY <> 6532843 GROUP BY OWNER_ID)
, doc(OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STRING_AGG( concat(cast(DOC_ID as varchar(max)),'.',FILE_EXTENSION),',') WITHIN GROUP (ORDER BY DOC_ID) DOC FROM DOCUMENTS GROUP BY OWNER_ID)
, doc(OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STRING_AGG( concat(cast(DOC_ID as varchar(max)),'.',FILE_EXTENSION),',') WITHIN GROUP (ORDER BY DOC_ID) DOC FROM DOCUMENTS GROUP BY OWNER_ID)
, resume (OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ', ' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE OWNER_ID = a.OWNER_ID and DOC_CATEGORY not in (6532841/*for contact*/, 6532839 /*for profile*/, 6532840 /*for experience*/  ) FOR XML PATH ('')), 1, 2, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)


select 
       concat(cast(DOC_ID as varchar(max)),'.',FILE_EXTENSION) as oldname
       , doc_name as new_name
       , created_date
       , *
from DOCUMENTS


