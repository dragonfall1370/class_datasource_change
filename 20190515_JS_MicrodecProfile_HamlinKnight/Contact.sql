--with test as (select *, row_number() over (partition by object_ref order by object_ref) as rn from consent where object_name = 'contact'),
--with document1 as (select b.parent_object_ref,
--concat(b.displayname,'.',file_extension) as 'file',
--row_number() over( partition by b.displayname order by b.displayname) as rn
--from person a left join linkfile b on a.person_ref = b.parent_object_ref
--where b.parent_object_name = 'person')

--,document2 as (select *,iif(rn = 1,[file],concat(rn,'_',[file])) as filename from document1)

--SELECT parent_object_ref as 'ContactID',
--    STUFF((SELECT DISTINCT ', ' + filename
--           FROM document2 a 
--           WHERE a.parent_object_ref = b.parent_object_ref
--          FOR XML PATH('')), 1, 2, '') as 'DocumentName'
--FROM document2 b
--GROUP BY parent_object_ref



with test as (select *, row_number() over (partition by person_ref order by person_ref) as rn from position where contact_status in (
1,2,3,4,5,6,62)),

owners2 as (select a.displayname,a.person_ref,a.responsible_user, b.email_address from test a left join person b on a.responsible_user = b.person_ref)

--linkedin as (select * from linksite where parent_object_name = 'person'),

--document as (select b.parent_object_ref,
--concat(b.displayname,'.',file_extension) as 'file',

--row_number() over( partition by b.displayname order by b.displayname) as rn
--from person a left join linkfile b on a.person_ref = b.parent_object_ref
--where b.parent_object_name = 'person')

, contactlookup as (select * from lookup where code_type = 1010)

, main_department as (select * From lookup where code_type = 299)

--, contacttype1 as (select a.person_ref,b.description from search_code a left join contactlookup b on a.code = b.code
--where a.person_ref is not null and a.code_type = '1010' and a.search_type = 4)

--, contracttype2 as (
--SELECT person_ref as 'Contact_ID',
--    STUFF((SELECT DISTINCT ', ' + a.description
--           FROM contacttype1 a 
--           WHERE a.person_ref = b.person_ref
--          FOR XML PATH('')), 1, 2, '') as 'description'
--FROM contacttype1 b
--GROUP BY person_ref)
,codetype2 as (select * from lookup where code_type = '135'),
test2 as (select row_number() over (partition by person_ref order by person_ref) as rn,* from position where type = 'C')
,contactstatus as (select a.person_ref,b.description from test2 a left join codetype2 b on a.contact_status = b.code)

,temp_locationcode as (
select b.description,a.* from search_code a
left join lookup b on a.code = b.code
where b.code_type = 1020)

,locationcode as (select person_ref,string_agg(description,', ') as description from temp_locationcode
where person_ref is not null
group by person_ref)

,temp_language as (
select b.description,a.* from search_code a
left join lookup b on a.code = b.code
where b.code_type = 1030)

,languagecode as (select person_ref,string_agg(description,', ') as description from temp_locationcode
where person_ref is not null
group by person_ref)

,temp_client_rate as (
select b.description,a.* from search_code a
left join lookup b on a.code = b.code
where b.code_type = 1040)

,client_rate as (select person_ref,string_agg(description,', ') as description from temp_locationcode
where person_ref is not null
group by person_ref)



,dupcheck1 as (select a.person_ref as 'contact-externalId',
iif(a.organisation_ref is null or a.organisation_ref ='','0',a.organisation_ref) as 'company-externalId',
iif(b.last_name is null or b.last_name = '','No Last Name',b.last_name) as 'contact-lastName',
iif(b.first_name is null or b.first_name = '','No First Name',b.first_name) as 'contact-firstName',
iif(b.gender is null or b.gender ='','',iif(b.gender = 'F','FEMALE','MALE')) as 'contact-gender',
case when b.title in ('MR','Mr','MRS','Ms','MISS','MS','Mrs') then b.title
else '' end
as 'contact-Title',
case when (b.title = '') then a.displayname
when b.title in ('MR','Mr','MRS','Ms','MISS','MS','Mrs') then a.displayname
when b.title not in ('MR','Mr','MRS','Ms','MISS','MS','Mrs') and b.title <> '' then b.title
when b.title is null and a.displayname is null then ''
else iif(a.displayname is null or a.displayname = '','',a.displayname) end
as 'contact-jobTitle',
iif(b.salutation is null or b.salutation = '','',b.salutation) as 'salutation',
iif(b.email_address is null or b.email_address ='','',b.email_address) as 'contact-email',
iif(b.mobile_telno is null or b.mobile_telno = '','',dbo.udf_GetNumeric(b.mobile_telno)) as 'contact-phone',
--iif(c.telephone_number is null or c.telephone_number = '','',c.telephone_number) as 'contact-workphone',
--iif(c.zc_telephone_number is null or c.zc_telephone_number = '','',c.zc_telephone_number) as 'contact-mobilephone',
iif(b.mobile_telno is null or b.mobile_telno = '','',dbo.udf_GetNumeric(b.mobile_telno)) as 'contact-mobilephone',

concat(
concat('ExternalID: ',a.person_ref,(char(13)+char(10))),
nullif(concat('Contact Type: ',f.description,(char(13)+char(10))),concat('Contact Type: ',(char(13)+char(10)))),
nullif(concat('Department: ',g.description,(char(13)+char(10))),concat('Department: ',(char(13)+char(10)))),
nullif(concat('Location: ',h.description,(char(13)+char(10))),concat('Location: ',(char(13)+char(10)))),
nullif(concat('Language: ',i.description,(char(13)+char(10))),concat('Language: ',(char(13)+char(10)))),
--nullif(concat('Client Rating: ',j.description,(char(13)+char(10))),concat('Client Rating: ',(char(13)+char(10)))),
nullif(concat('Note: ',replace(a.notes,'\x00\x0d\x00\x0a',(char(13)+char(10)))),'Note: ')

)as 'contact-note'

--,iif(g.email_address is null or g.email_address ='','',g.email_address) as 'contact-owners'
--,iif(e.website_url is null or e.website_url = '','',e.website_url) as 'contact-linkedin'

from test a
left join person b on a.person_ref = b.person_ref
left join address c on a.person_ref = c.person_ref
--left join owners2 g on a.person_ref = g.person_ref
--left join linkedin e on a.person_ref = e.parent_object_ref
--left join document d on d.parent_object_ref = a.person_ref
--left join contracttype2 d on a.person_ref = d.Contact_ID
left join contactstatus f on a.person_ref = f.person_ref
left join main_department g on a.department = g.code
left join locationcode h on a.person_ref = h.person_ref
left join languagecode i on a.person_ref = i.person_ref
--left join client_rate j on a.person_ref = j.person_ref
where a.rn = 1 and f.description is not null)

, dupcheck2 as (select *,row_number() over (partition by [contact-externalId] order by [contact-externalId]) as rn1 from dupcheck1)
, dupcheck3 as (select *,row_number() over (partition by [contact-email] order by [contact-email]) as rn2 from dupcheck2 where rn1=1)

,test4 as (select *,iif([contact-email] = '','',iif(rn2=1,[contact-email],concat(rn2,'-',[contact-email]))) as 'contact-email2' from dupcheck3)
--where [contact-externalId] = 221987


select a.*,concat(a.[contact-note], (char(13)+char(10)),
nullif(concat('Client Rating: ',j.description,(char(13)+char(10))),concat('Client Rating: ',(char(13)+char(10))))
) as 'final-contact-note'
from test4 a 
left join client_rate j on a.[contact-externalId] = j.person_ref
--where [contact-externalId] = 138093
--where [contact-externalId] in (select person_ref from person where z_last_contact_action is not null)