with contactpos1 as (select *, ROW_NUMBER() over (partition by organisation_ref order by organisation_ref) as rn from position )
, contactpos2 as (select * from contactpos1 where rn = 1)
, alias as (select * from lookup where code_type = '108')
, alias2 as (select * from lookup where code_type = '119')
, jobtypelookup as (select * from lookup where code_type = 1010)
, jobtypecode as (select a.opportunity_ref,b.description from search_code a left join jobtypelookup b on a.code = b.code where a.opportunity_ref is not null and a.code_type = '1010')
, jobtypecode2 as (
SELECT opportunity_ref as 'Job_ID',
    STUFF((SELECT DISTINCT ', ' + a.description
           FROM jobtypecode a 
           WHERE a.opportunity_ref = b.opportunity_ref
          FOR XML PATH('')), 1, 2, '') as 'description'
FROM jobtypecode b
GROUP BY opportunity_ref)
, locationlookup as (select * from lookup where code_type = 1020)
, locationcode as (select a.opportunity_ref,b.description from search_code a left join locationlookup b on a.code = b.code where a.opportunity_ref is not null and a.code_type = '1020')
, locationcode2 as (
SELECT opportunity_ref as 'Job_ID',
    STUFF((SELECT DISTINCT ', ' + a.description
           FROM locationcode a 
           WHERE a.opportunity_ref = b.opportunity_ref
          FOR XML PATH('')), 1, 2, '') as 'description'
FROM locationcode b
GROUP BY opportunity_ref)
, qualificationlookup as (select * from lookup where code_type = 1025)
, qualificationcode as (select a.opportunity_ref,b.description from search_code a left join qualificationlookup b on a.code = b.code where a.opportunity_ref is not null and a.code_type = '1025')
, qualificationcode2 as (
SELECT opportunity_ref as 'Job_ID',
    STUFF((SELECT DISTINCT ', ' + a.description
           FROM qualificationcode a 
           WHERE a.opportunity_ref = b.opportunity_ref
          FOR XML PATH('')), 1, 2, '') as 'description'
FROM qualificationcode b
GROUP BY opportunity_ref)
, langlookup as (select * from lookup where code_type = 1030)
, langcode as (select a.opportunity_ref,b.description from search_code a left join langlookup b on a.code = b.code where a.opportunity_ref is not null and a.code_type = '1030')
, langcode2 as (
SELECT opportunity_ref as 'Job_ID',
    STUFF((SELECT DISTINCT ', ' + a.description
           FROM langcode a 
           WHERE a.opportunity_ref = b.opportunity_ref
          FOR XML PATH('')), 1, 2, '') as 'description'
FROM langcode b
GROUP BY opportunity_ref)
, industrylookup as (select * from lookup where code_type = 1005)
, industrycode as (select a.opportunity_ref,b.description from search_code a left join industrylookup b on a.code = b.code where a.opportunity_ref is not null and a.code_type = '1005')
, industrycode2 as (
SELECT opportunity_ref as 'Job_ID',
    STUFF((SELECT DISTINCT ', ' + a.description
           FROM industrycode a 
           WHERE a.opportunity_ref = b.opportunity_ref
          FOR XML PATH('')), 1, 2, '') as 'description'
FROM industrycode b
GROUP BY opportunity_ref)
, skilllookup as (select * from lookup where code_type = 1015)
, skillcode as (select a.opportunity_ref,b.description from search_code a left join skilllookup b on a.code = b.code where a.opportunity_ref is not null and a.code_type = '1015')
, skillcode2 as (
SELECT opportunity_ref as 'Job_ID',
    STUFF((SELECT DISTINCT ', ' + a.description
           FROM skillcode a 
           WHERE a.opportunity_ref = b.opportunity_ref
          FOR XML PATH('')), 1, 2, '') as 'description'
FROM skillcode b
GROUP BY opportunity_ref)


------------------------------- main script-----------------------------
, main as (select a.opportunity_ref as 'position-externalId', 
iif(b.person_ref is null or b.person_ref ='',0,b.person_ref) as 'position-contactId',
iif(a.displayname is null or a.displayname = '','No Job Name',a.displayname) as 'position-title',

iif(a.date_opened is null or a.date_opened ='','', convert(datetime, CONVERT(float,a.date_opened))) as 'position-startDate',
iif(a.date_closed is null or a.date_closed ='','', convert(datetime, CONVERT(float,a.date_closed))) as 'position-endDate',
iif(a.no_persons_reqd is null or a.no_persons_reqd = '','',a.no_persons_reqd) as 'position-headcount',
replace(replace(dbo.udf_StripHTML(m.job_description),'\x00\x0d\x00\x0a',(char(13)+char(10))),'\x00\x0d',': ') as 'position-publicDescription',

case when a.workflow = '1' then 'PERMANENT'
 when a.workflow = '2' then 'INTERIM_PROJECT_CONSULTING'
 when a.workflow = '3' then 'PERMANENT'
 when a.workflow = '4' then 'CONTRACT'
 when a.workflow = '5' then 'TEMPORARY'
end
as 'position-type',

f.email_address as 'position-owners',

iif(g.income_lower = '' or g.income_lower is null,'',g.income_lower) as 'salary from',
iif(g.income_upper = '' or g.income_upper is null,'',g.income_upper) as 'salary to',

concat( concat('ExternalID: ',a.opportunity_ref,(char(13)+char(10))),
nullif(concat('Status: ',d.description,(char(13)+char(10))),concat('Status: ',(char(13)+char(10)))),
nullif(concat('Source: ',c.description,(char(13)+char(10))),concat('Source: ',(char(13)+char(10)))),
nullif(concat('Package: ',a.text01,(char(13)+char(10))),concat('Package: ',(char(13)+char(10)))),
nullif(concat('Job Type: ',h.description,(char(13)+char(10))),concat('Job Type: ',(char(13)+char(10)))),
nullif(concat('Location: ',i.description,(char(13)+char(10))),concat('Location: ',(char(13)+char(10)))),
nullif(concat('Qualification: ',e.description,(char(13)+char(10))),concat('Qualification: ',(char(13)+char(10)))),
nullif(concat('Language: ',j.description,(char(13)+char(10))),concat('Language: ',(char(13)+char(10)))),
nullif(concat('Industry: ',k.description,(char(13)+char(10))),concat('Industry: ',(char(13)+char(10)))),
nullif(concat('Skills: ',l.description,(char(13)+char(10))),concat('Skills: ',(char(13)+char(10)))),
nullif(concat('Notes: ',replace(a.notes,'\x00\x0d\x00\x0a',(char(13)+char(10))),(char(13)+char(10))),concat('Notes: ',(char(13)+char(10))))
) as 'position-note',

row_number() over ( partition by a.displayname order by a.displayname ) as rn

from opportunity a



left join contactpos2 b on a.organisation_ref = b.organisation_ref
left join alias c on a.source = c.code
left join alias2 d on a.record_status = d.code
left join qualificationcode2 e on a.opportunity_ref = e.Job_ID
left join person f on a.responsible_user = f.person_ref
left join opport_web g on a.opportunity_ref = g.opportunity_ref
left join jobtypecode2 h on a.opportunity_ref = h.Job_ID
left join locationcode2 i on a.opportunity_ref = i.Job_ID
left join langcode2 j on a.opportunity_ref = j.Job_ID
left join industrycode2 k on a.opportunity_ref = k.Job_ID
left join skillcode2 l on a.opportunity_ref = l.Job_ID
left join opport_web m on a.opportunity_ref = m.opportunity_ref
)

select *, iif(rn=1,[position-title],concat(rn,'-',[position-title])) as position from main