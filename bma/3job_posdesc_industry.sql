
# NOTE:
# SET group_concat_max_len = 2147483647;
# select distinct postype from api_jobs a
# select sno, type, name from manage where type = 'jotype' 
#select sno, type, name from manage 
# select * from TRUONG_companyowneremail

select
        convert(a.posid,char(200)) as 'position-externalId'
        ,ind.name
       ,case ind.name
              when 'IT' then 28824
              when 'Commercial' then 28884
              when 'Mayor Accounts' then 28885
              when 'Operations & Management' then 28886
              when 'Supply Chain, Engineering, Construction' then 28887       
       end as 'INDUSTRY' #,a.industryid --<<
# select postitle # select count(*)
from posdesc a
left join ( select sno, type, name from manage where type = 'joindustry' ) ind on ind.sno = a.industryid
where ind.name is not null
