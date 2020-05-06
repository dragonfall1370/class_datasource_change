


with
phone (ID,phone) as (
       select top 100 C.candidateID
	      , replace(translate(value, '!'':"<>[]();,+/\&?•*|‘','                     '),char(9),'') as email --to translate special characters
	from bullhorn1.Candidate C
	cross apply string_split(phone,' ')
--	where (UC.email like '%_@_%.__%' or UC.email2 like '%_@_%.__%' or UC.email3 like '%_@_%.__%') and C.isdeleted <> 1 and C.status <> 'Archive'
	)
select * from phone


select c.phone
from bullhorn1.Candidate C
where phone is not null and phone <> ''
and phone 
and phone like '%[a-Z0-9]%'


select --top 200 
         phone --, LEFT(phone, 1) as _left
       ,      replace(replace(replace(replace(replace(replace(phone,' ',''),'-',''),'.',''),'(',''),')',''),'+','') as clear_format
       , len( replace(replace(replace(replace(replace(replace(phone,' ',''),'-',''),'.',''),'(',''),')',''),'+','') ) as length
       , [dbo].[ufn_FormatPhone](phone) as phone_number
       --, [dbo].[ufn_FormatPhone]( replace(replace(phone,' ',''),'-','') ) as phone_number
from bullhorn1.Candidate C
--where phone like '0%'
--where phone like '%[a-Z0-9]%'
where len( replace(replace(replace(replace(replace(replace(phone,' ',''),'-',''),'.',''),'(',''),')',''),'+','') ) = 11
--where len( replace(replace(replace(replace(replace(phone,' ',''),'-',''),'.',''),'(',''),')','') ) < 10 and phone <> ''
where phone like '+1416%' or phone like '1416%'
