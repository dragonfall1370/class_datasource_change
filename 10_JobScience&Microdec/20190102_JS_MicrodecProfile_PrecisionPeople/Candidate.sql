with linkedin as (select * from linksite where parent_object_name = 'person')
--,document1 as (select b.parent_object_ref,
--concat(b.displayname,'.',file_extension) as 'file',
--row_number() over( partition by b.displayname order by b.displayname) as rn
--from person a left join linkfile b on a.person_ref = b.parent_object_ref
--where b.parent_object_name = 'person')

--,document2 as (select *,iif(rn = 1,[file],concat(rn,'_',[file])) as filename from document1),
--document3 as (
--SELECT parent_object_ref as 'CandidateID',
--    STUFF((SELECT DISTINCT ', ' + filename
--           FROM document2 a 
--           WHERE a.parent_object_ref = b.parent_object_ref
--          FOR XML PATH('')), 1, 2, '') as 'DocumentName'
--FROM document2 b
--GROUP BY parent_object_ref)
,persontype2 as (select *,row_number() over (partition by person_ref order by person_ref) as row_num from person_type )
,persontype as (select * from persontype2 where row_num = 1)
,owners as (select * from person)

,main as (select b.person_ref as 'candidate-externalId',
iif(b.last_name is null or b.last_name ='','No Last Name',b.last_name) as 'candidate-last',
iif(b.first_name is null or b.first_name ='','No First Name',b.first_name) as 'candidate-first',
iif(b.email_address is null or b.email_address = '','',b.email_address) as 'candidate-email',
iif(b.title in ('MR','MRS','MS','MISS','DR'),trim(upper(b.title)),'') as 'candidate-title',
iif(b.salutation is null or b.salutation = '','',b.salutation) as 'salutation',
case when b.gender = 'F' then 'FEMALE'
when b.gender = 'M' then 'MALE'
else '' end as 'candidate-gender',
iif(b.date_of_birth is null or b.date_of_birth = '','',convert(datetime, CONVERT(float,b.date_of_birth))) as 'candidate-dob',
iif(c.county_state = '' or c.county_state is null,'',c.county_state) as 'candidate-State',
iif(c.post_town = '' or c.post_town is null,'',c.post_town) as 'candidate-city',
iif(c.zipcode is null or c.zipcode ='','',c.zipcode) as 'candidate-zipCode',
iif(c.address_line_1 is null or c.address_line_1 ='','',c.address_line_1) as 'candidate-address',
iif(b.mobile_telno is null or b.mobile_telno ='','',dbo.udf_GetNumeric(b.mobile_telno)) as 'candidate-phone',
iif(c.zc_telephone_number is null or c.zc_telephone_number ='','',c.zc_telephone_number) as 'candidate-homePhone',
iif(b.zc_day_telno is null or b.zc_day_telno = '','',b.zc_day_telno) as 'candidate-workphone',
iif(b.responsible_user = 0,'',f.email_address) as 'candidate-owners',
iif(b.qualification_note is null or b.qualification_note ='','',b.qualification_note) as 'candidate-education',
iif(e.website_url is null or e.website_url = '','',e.website_url) as 'candidate-linkedin',
concat(
nullif(concat('CV Last Updated: ',convert(datetime, CONVERT(float,b.cv_last_updated)),(char(13)+char(10))),concat('CV Last Updated: ',(char(13)+char(10)))),
--nullif(concat('Qualification Notes: ',b.qualification_note,(char(13)+char(10))),concat('Qualification Notes: ',(char(13)+char(10))))
nullif(concat('Int Availability: ',b.user_text1,(char(13)+char(10))),concat('Int Availability: ',(char(13)+char(10)))),
nullif(concat('Motiv to Change: ',b.user_text2,(char(13)+char(10))),concat('Motiv to Change: ',(char(13)+char(10)))),
nullif(concat('Notice Period: ',a.notice_period,(char(13)+char(10))),concat('Notice Period: ',(char(13)+char(10)))),
nullif(concat('Notice Period Mode: ',a.notice_period_mode,(char(13)+char(10))),concat('Notice Period Mode: ',(char(13)+char(10)))),
nullif(concat('Date Available: ',convert(datetime, CONVERT(float,a.date_available)),(char(13)+char(10))),concat('Date Available: ',(char(13)+char(10)))),
nullif(concat('Seeking: ',a.seeking,(char(13)+char(10))),concat('Seeking: ',(char(13)+char(10))))

) as 'candidate-note',
ROW_NUMBER() over ( partition by b.person_ref order by b.person_ref ) as rn
--f.DocumentName as 'candidate-document'

from candidate a
left join persontype d on a.person_type_ref = d.person_type_ref
left join person b on d.person_ref = b.person_ref
left join address c on a.person_type_ref = c.person_ref
left join linkedin e on a.candidate_ref = e.parent_object_ref
left join owners f on b.responsible_user = f.person_ref
--left join document3 f on a.candidate_ref = f.CandidateID
)
,main2 as (select *,row_number() over ( partition by [candidate-email] order by [candidate-email]) as rn2 from main where rn = 1)
select *,iif([candidate-email] is null or [candidate-email] = '','',iif(rn2=1,[candidate-email],concat(rn2,'-',[candidate-email]))) as email from main2

