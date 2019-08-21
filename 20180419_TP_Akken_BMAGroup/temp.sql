# NOTE:
# SET group_concat_max_len = 2147483647;


-- COMPANY ACTIVITIES COMMENTS

select username, fname, mname, lname from candidate_general where username = 'cand22805'
------------ CANDIDATE RESUME

select  
       cg.username as 'externalid'
       #,cg.sno, cg.profiletitle, cg.fname, cg.lname, cg.email, d.*
       #,d.profile_data #,CONVERT(d.profile_data USING utf8)
       ,'Resume' as title
       ,CONVERT(CAST(d.resume_data as BINARY) USING utf8) ,d.resume_data as 'resume_data'
       , REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( d.resume_data
                                        ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                                        ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                                        ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                                        ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                                        ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                                        ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'') as 'note2'
# select * # select count(*)
from candidate_general cg 
left join search_data d on d.uid = cg.sno
where d.type = 'cand' and d.resume_data <> ''
and cg.email = 'riveracolon.jd@hotmail.com' or cg.username = 'cand22805' or cg.sno = 78583

select * from search_data where profile_data like '%1. Support the startup activities at both Merck locations from ARIBA to SAP system%'; Katherine R. Barea
select * from search_data where resume_data like '%PGMS% confirma que la fecha de ingreso requerida es jueves 22 de marzo%';

select  cg.sno, cg.fname, cg.lname, d.profile_data, CONVERT(d.resume_data USING utf8)
select * # select count(*)
from candidate_general cg 
left join search_data d on d.sno = cg.sno
where d.resume_data = ''
(52979,37277,37278,44630,44631,44632,40037,40038,40039,40040)


select count(*) from search_data where sno <> uid --63579
select * from search_data where sno <> uid
select * from search_data where uid in (78583) or resume_data like '%Yaritza Serrano-Izquierdo%'
select count(*) from search_data where uid <> sno

select  
       cg.username as 'externalid'
       ,cg.sno, cg.profiletitle, cg.fname, cg.lname, cg.email, d.*
       ,d.profile_data #,CONVERT(d.profile_data USING utf8)
       ,'Resume' as title
       #,d.resume_data as 'note2' #,CONVERT(d.resume_data USING utf8)
       , REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( d.resume_data
                                        ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                                        ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                                        ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                                        ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                                        ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                                        ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'')
# select * # select count(*)
from candidate_general cg 
left join search_data d on d.uid = cg.sno
where d.type = 'cand' and d.resume_data <> '' and d.uid = 70788
and d.uid <> d.sno


------- document
select d.*, c.*
from con_resumes d
left join ( select concat('con',sno) as sno, concat(fname,' ',lname) as name from staffoppr_contact ) c on c.sno = d.username
#left join  ( select username, concat(fname,' ',lname) as name from candidate_general ) c on c.username = d.username 
where d.sno in (99047,96019,93430,93060,88862,79606,79099,79098,79097,74384,68377,68372,62908,62621,60854,60846,60046,59468,48282,48008,132658,130571,130570,130569,130568,123792,123276,108500,108499,106155,106150,105543,100249)
and d.username like 'con%'


select * from contact_doc
select distinct(left(con_id, 3)) from contact_doc where con_id
app
can
com
con
emp
opp
req


select 
        con.sno, con.fname, con.mname, con.lname
       ,u.name
       ,cd.* 
from contact_doc cd
left join staffoppr_contact con on concat('con',con.sno) = cd.con_id
left join users u on u.username = cd.username
where con.sno is not null and con.sno in (1732)
or con.fname like ('Karol') and con.lname like 'Cerdas'

select distinct( left(con_id, 3) ) from contact_doc
select *  from contact_doc

SELECT
  'aaaaa, bbbbb, ccccc',
  SUBSTRING_INDEX('aaaaa, bbbbb, ccccc', ',', 1) AS column_one,
  SUBSTRING_INDEX(SUBSTRING_INDEX('aaaaa, bbbbb, ccccc', ',', 2), ',', -1) AS column_two,
  SUBSTRING_INDEX(SUBSTRING_INDEX('aaaaa, bbbbb, ccccc', ',', 3), ',', -1) AS column_three
  
  
-----------
select CAST(CONVERT(res_name USING utf8) AS binary),convert(res_name USING utf8)  from con_resumes where sno = 10045
select count(*) #98389# from candidate_general cg
select count(distinct compstatus) from staffoppr_cinfo c
select count(distinct stateid) from staffoppr_cinfo c
select count(*) from cmngmt_pr 
select count(*) from con_resumes where sno = 72828
select count(*) from notes  #206270
select count(*) from staffoppr_cinfo c
select distinct acc_comp from  staffoppr_cinfo c
select distinct ACCESSTO from candidate_list 
select distinct company from notes
select distinct compstatus, count(*) from staffoppr_cinfo c group by compstatus
select distinct stateid, count(*) from staffoppr_cinfo c group by stateid
select distinct status, count(*) from staffoppr_cinfo c group by status
select distinct type from manage  where type like '%compsource%' or sno = 2169
select distinct username from con_resumes;
select count(*) from contact_doc 
select distinct username from contact_doc where docname like '%doc'; --89
select e.name, e.email, u.* from applicants e left join users u on e.name = u.name 
select e.sno, e.name, e.email from emp_list e left join users u on e.sno = u.username       
select * from ac_category where tdesc like '%Ba%'
select * from applicants where name like 'Franco Mondo';
select * from candidate_general where email = 'asanocampo@gmail.com'
select * from candidate_note
select * from candidate_ref
select * from candidate_work where wdesc like '%GLADYS V TERC%';
select * from candidate_work where wdesc like '%Troubleshooting and problem solver in the production assembling line%';
select * from cmngmt_pr where subject like '%GLADYS V TERC%'
select * from cmngmt_pr where subject like 'Ertec%'
select * from cmngmt_pr where subject like 'Finance Manager - Colombia';
select * from cmngmt_pr where subject like '%Leyla Tuckler%' and 
select * from cmngmt_pr where subject like '%PGMS confirma que la fecha de ingreso requerida es jueves 22 de marzo%'
select * from cmngmt_pr where subject like 'Submitted to Scotiabank Panama Employee';
select * from comp_grp where grpname like 'Ertec%'
select username, CONVERT(CAST(res_name as BINARY) USING utf8), CONVERT(CAST(filecontent as BINARY) USING utf8) from con_resumes where username in ('con22605') or  CONVERT(CAST(filecontent as BINARY) USING utf8) like '%Jennifer Ortiz Torres%'
select * from con_resumes;
select * from con_resumes where sno in (73386,100017,100018) or username  in ('con22605') #username like '%22605%' type = 'con' and 
select * from con_resumes where filecontent like '%GLADYS V TERC%' 
select * from con_resumes whereiletype = 'application/x-zip'  username = 'cand523'  f
select * from con_resumes where res_name like '%BCK Pathstone Agreement 2018.pdf%' 
select * from con_resumes where sno = 10045 or res_name like '%bryan%' where filecontent like '%PGMS confirma que la fecha de ingreso requerida es jueves 22 de marzo%' 
select * from con_resumes where type = 'con' order by username asc ;
select * from con_resumes where  username  = 'cand93014'
select * from consultant_work where ftitle like '%Submitted to Scotiabank Panama Employee%';
select * from consultant_work where wdesc like '%Troubleshooting and problem solver in the production assembling line%';
select * from contact_doc
select * from contact_doc ;
select * from contact_doc where body like '%Use of basic laboratory equipment%' or docname like '%doc'; #Jessica Zayas
select *  FROM contact_doc where con_id = 'cand55253'
select * from contact_doc where notes like  '%GLADYS V TERC%' 
select * from contact_email where subject like '%GLADYS V TERC%' 
select * from contact_event where enotes like '%PGMS confirma que la fecha de ingreso requerida es jueves 22 de marzo%' or etitle like '%Submitted%'
select * from department where deptname like 'ALL Employees%'
select * from hotjobs;    
select * from hotjobs where postitle like 'Finance Manager - Colombia';
select * from invite_calendar where name like 'ALL Employees%'
select * from mail_headers where subject like 'Finance Manager - Colombia';
select * from manage where name like 'Actively Searching';
select * from manage where name like '%Tourism%'
select * from manage where sno = 237
select * from manage where type like '%Stage%' or name like '%Reporting Manager%'
select * from notes desc ac_category;
select * from notes n where n.contactid  = 1064
select * from notes where notes like '%COO position, we agreed on FEE%'
select * from ol_abcontactproperties where pvalue like 'ALL Employees%'
select * from ol_abcontactproperties where pvalue like 'ALL Employees%'
select * from ol_email where ConversationTopic like 'Finance Manager - Colombia';
select * from ol_recipients where RecName like 'ALL Employees%'
select * from ol_recipients where RecName like 'ALL Employees%'
select * from staffoppr_cinfo c where c.sno = 1296
select * from staffoppr_cinfo c     where sno = 1064
select *  from staffoppr_contact c where fname like '%Katherine%'
select * from staffoppr_contact order by csno desc c where sno = 21121
select * from staffoppr_contact where fname like 'Tamara' and lname like 'Matos'
select * from staffoppr_contact where sno = 2961
select * from staffoppr_contact where sno = staffoppr_contact
select * from users
select res_name, CONVERT(CAST(res_name as BINARY) USING utf8) from con_resumes where sno = 10045
select res_name from api_con_resumes where res_name like '%ban%'
select sno,fname, lname from staffoppr_contact where  sno = 23750;

