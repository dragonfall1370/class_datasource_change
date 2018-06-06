select left(con_id,3), count(*)  from cmngmt_pr group by left(con_id,3)
acc	1438
app	22
can	236787
com	12705
con	12154
emp	19843
opp	89252
req	89004

select distinct title from cmngmt_pr
Appointment
Campaign
Data Export
Document
Email
Event
MFailed
Notes
Placed
PostingIP
Postings
REmail
Submissions
Submitted
Task

select count(*)  from cmngmt_pr --461458


--company
select distinct title, count(*) from cmngmt_pr where con_id like 'com%' group by title
Document	1031
Event	2
Notes	11671
Task	1
-- contact
select distinct title, count(*) from cmngmt_pr where con_id like 'con%' group by title 
Document	172
Email	1265
REmail	3355
Task	7362
select distinct title, count(*) from cmngmt_pr where con_id like 'oppr%' group by title 
Appointment	2
Document	16
Email	21088
Event	18831
Notes	48
PostingIP	2
REmail	49265
--job
select distinct title, count(*) from cmngmt_pr where con_id like 'req%' group by title 
--candidate
select distinct title, count(*) from cmngmt_pr where con_id like 'can%' group by title 
Appointment	255
Document	58591
--Email	8089
Event	1917
--MFailed	6
Notes	112900
--Placed	301
--REmail	36090
--Submissions	2
Submitted	18609
--Task	27

select distinct tysno,count(*) from cmngmt_pr group by tysno having count(*) > 1
select * from cmngmt_pr where con_id like 'cand29613'
select * from cmngmt_pr where subject like '%Esta por coordinarse un conference call para la semana d%'
select * from cmngmt_pr where title = 'Appointment' like '%Submitted to Veritas/Background Check - DDM-BMA%';

select 
        cp.con_id as 'externalId' ,cg.fname,cg.mname,cg.lname, cp.*
from cmngmt_pr cp
left join candidate_general cg on cg.username = cp.con_id where cp.title = 'Appointment'
------------------------------------------------------

-- activity
#select distinct title from cmngmt_pr
select * from appointments where title = 'Fredeick Miranda - Sales Sup. Covidien BMA' #--Appointment
select * from --Campaign
select * from --Data Export
select * from contact_doc --Document
select * from contact_email --Email
select * from --Event
select * from --MFailed
select * from notes n where n.type = 'cand'  and n.contactid = 29613 --Notes
select * from --Placed
select * from --PostingIP
select * from --Postings
select * from --REmail
select * from entity_submission_roledetails #--Submissions
select * from contact_event where con_id like 'cand29613' #--Submitted
Task


select * from staffoppr_contact where fname like 'Vanessa' and lname like 'Pagan'

---- final (not optimized)
select 
        cp.con_id as 'externalId' ,cg.fname,cg.mname,cg.lname ,cp.sno
       ,-10 as 'user_account_id'
       ,'comment' as 'category'
       ,'candidate' as 'type'
       ,cp.sdate as 'insert_timestamp'
       ,concat(
              'User: ',u.name,'\n'
              ,'Title: ',cp.title,'\n'
              ,'Subject: ',cp.subject,'\n'
              ,case when note.subtype is NULL or note.subtype = '' THEN '' ELSE concat('Sub Type: ',note.subtype,'\n') END
              ,case when note.notes is NULL or note.notes = '' THEN '' ELSE concat('Notes: ',note.notes,'\n') END
              ) as 'content'
# select count(*)
from cmngmt_pr cp
left join candidate_general cg on cg.username = cp.con_id
left join users u on u.username = cp.username
left join (
        #-- Notes
                select n.sno,'' as notes, st.name as subtype
                # select count(*) #206270 # select *
                from notes n
                left join (select sno, type, name from manage where type = 'Notes' ) st on st.sno = n.notes_subtype
                where n.type = 'cand' #and n.contactid = 29613
        union all
        #-- Submitted
                select sno, enotes as notes,'' as subtype
                # select count(*) #41713
                from contact_event #where con_id = 'cand29613'
       union all 
       #-- Document
                select sno, notes, '' as subtype
                # select count(*) #77229
                from contact_doc #where con_id = 'cand29613'
) note on note.sno = cp.tysno
#where cp.con_id like ('cand29613')
#order by cp.sdate desc 










alter table cmngmt_pr add column truong_notes text;

update cmngmt_pr
inner join notes on cmngmt_pr.tysno = notes.sno
set cmngmt_pr.truong_notes = notes.notes
where  cmngmt_pr.title = 'Notes'; #205980

update cmngmt_pr
inner join contact_event on cmngmt_pr.tysno = contact_event.sno
set cmngmt_pr.truong_notes = contact_event.enotes
where  cmngmt_pr.title in ('Event','Submitted'); #22528+18609=41137

update cmngmt_pr
inner join contact_doc on cmngmt_pr.tysno = contact_doc.sno
set cmngmt_pr.truong_notes = contact_doc.notes
where  cmngmt_pr.title = 'Document'; #68181

select count(*)  from cmngmt_pr #461458
where truong_notes is not null
and title 
#in ('Event','Submitted')
in ('Document')
in ('Notes')




----- CONTACT

select 
        cg.sno as 'externalId' ,cg.fname,cg.mname,cg.lname ,cp.con_id
       ,-10 as 'user_account_id'
       ,'comment' as 'category'
       ,'contact' as 'type'
       ,cp.sdate as 'insert_timestamp'
       ,concat(
              'User: ',u.name,'\n'
              ,'Title: ',cp.title,'\n'
              ,case when cp.subtype = '' THEN '' ELSE concat('Sub Type: ',cp.subtype,'\n') END
              ,'Subject: ',cp.subject,'\n'
              ,case when cp.truong_notes is NULL or cp.truong_notes = '' THEN '' ELSE concat('Notes: ',cp.truong_notes,'\n') END
              ) as 'content'
# select count(*) #89252# select *
from cmngmt_pr cp
left join staffoppr_contact cg on cg.sno = replace(cp.con_id,'oppr','')
left join users u on u.username = cp.username
where cg.sno is not null and cp.con_id like ('oppr%')
and cg.fname like 'Mabel' and cg.lname like 'Rodriguez'
and cg.fname like 'Vanessa' and cg.lname like 'Pagan'
#order by cp.sdate desc 



-- JOB

select 
       a.posid as 'externalId'
       ,a.company as 'company External ID'
       ,a.contact as 'position-contactId'
       ,a.postitle as 'position-title'
       ,-10 as 'user_account_id'
       ,'comment' as 'category'
       ,'job' as 'type'
       ,cp.sdate as 'insert_timestamp'
       ,concat(
              'User: ',u.name,'\n'
              ,'Title: ',cp.title,'\n'
              ,case when cp.subtype = '' THEN '' ELSE concat('Sub Type: ',cp.subtype,'\n') END
              ,'Subject: ',cp.subject,'\n'
              ,case when cp.truong_notes is NULL or cp.truong_notes = '' THEN '' ELSE concat('Notes: ',cp.truong_notes,'\n') END
              ) as 'content'
# select count(*) #89252# select *
from cmngmt_pr cp
left join posdesc a on a.posid = replace(cp.con_id,'req','')
left join users u on u.username = cp.username
where a.posid is not null and cp.con_id not like '%,%' and cp.con_id like ('req%')
and  a.postitle like'%BDM%'
#order by cp.sdate desc 