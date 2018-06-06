
# NOTE: SET group_concat_max_len = 2147483647;
# select distinct postype from api_jobs a
# select sno, type, name from manage where type = 'jotype' 
#select sno, type, name from manage 
# select * from TRUONG_companyowneremail


select
        a.posid as 'position-externalId'
       ,a.company as 'company External ID' ,com.cname as 'company name'
       ,case when con.sno is null then 0 else con.sno end as 'position-contactId' #, con.fname, con.mname, con.lname #a.contact
       ,o.email as 'position-owners' #,a.owner
       ,ind.name as 'INDUSTRY' #,a.industryid --<<
       ,case when jobtitle.postitle_rank = 1 then jobtitle.postitle else concat(jobtitle.postitle,' ',jobtitle.postitle_rank) end as 'position-title' #,a.postitle
       ,case a.posworkhr
              when 'fulltime' then 'FULL_TIME'
              when 'parttime' then 'PART_TIME'
        end as 'position-employmenttype'
       ,case type.name #a.postype
              when 'Temp/Contract' then 'CONTRACT'
              when 'Direct' then 'PERMANENT'
              when 'Temp/Contract to Direct' then 'TEMPORARY_TO_PERMANENT'
              when 'Internal Temp/Contract' then 'CONTRACT'
              when 'Internal Direct' then 'PERMANENT'
        end as 'position-type'
       ,a.no_of_pos 'position-headcount'
       ,date(a.posted_date) as 'position-startDate' #,a.posted_date
       ,date(a.remove_date) as 'position-endDate' #,a.remove_date
       ,intjob.note as 'position-internalDescription'
       ,pubjob.note as 'position-publicDescription'
       ,notejob.note as 'position-note'
       ,d.document as 'position-document'
       #notes as 'ACTIVITIES COMMENTS'
       #notes, tasklist as 'ACTIVITIES COMMENTS'
# select postitle # select count(*) #6584
from posdesc a
left join staffoppr_cinfo com on com.sno = a.company
left join (select sno, fname, mname, lname from staffoppr_contact ) con on con.sno = a.contact
left join TRUONG_companyowneremail o on o.owner = a.owner
left join truong_jobdocument d on d.sno = a.posid
left join (select sno, type, name from manage where type = 'jotype') type on type.sno = a.postype
left join (select sno, type, name from manage where type = 'joindustry') ind on ind.sno = a.industryid
left join truong_job jobtitle on jobtitle.posid = a.posid
left join truong_intjob intjob on intjob.posid = a.posid
left join truong_pubjob pubjob on pubjob.posid = a.posid
left join truong_notejob notejob on notejob.posid = a.posid
#where a.posid in (5457) #or a.postitle like '%Finance Manager%'
#where a.company = 774