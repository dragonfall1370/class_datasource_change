
-- TASK WITH EMAIL ATTACHMENT INCLUDED
with a as ( 
       select top 10000  t.id --,t.whoid, t.subject, t.description, a.id, a.parentid
              , concat(a.id,'_',replace(a.Name,',','') ) as doc
       -- select count(*) --114426
       from task t
       left join attachment a on a.parentid = t.id where a.Name <> '' )
--select id, STUFF((SELECT ', ' + doc from a WHERE id = a.id FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') AS docs  from a GROUP BY a.id 
, a1 as (select id, STUFF((SELECT ', ' + doc from a WHERE id = a.id FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') AS docs  from a GROUP BY a.id )
select * from a1
--select count(*),count(distinct id) from a --23854

update task
set truong_att =  t.docs
from (
       --select id, STUFF((SELECT ', ' + doc from a WHERE id = a.id FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') AS docs  from a GROUP BY a.id 
       select * from a1
       ) t
where task.id = t.id

select count (*),count(distinct id) from task --57404
 
 
   select  a.id
              , concat(a.id,'_',replace(a.Name,',','') ) as doc
       -- select count(*) --119557
       from  attachment a
 
-- FIND ID
select id from Account where id in ('005800000032OjzAAE','00580000003DipbAAC','0051A000009H0LfQAK','005800000031SDBAA2','005C0000003bNFbIAM');
select id from Attachment where id in ('005800000032OjzAAE','00580000003DipbAAC','0051A000009H0LfQAK','005800000031SDBAA2','005C0000003bNFbIAM');
select id from Case_ where id in ('005800000032OjzAAE','00580000003DipbAAC','0051A000009H0LfQAK','005800000031SDBAA2','005C0000003bNFbIAM');
select id from CaseHistory where id in ('005800000032OjzAAE','00580000003DipbAAC','0051A000009H0LfQAK','005800000031SDBAA2','005C0000003bNFbIAM');
select id from Contact where id in ('005800000032OjzAAE','00580000003DipbAAC','0051A000009H0LfQAK','005800000031SDBAA2','005C0000003bNFbIAM');
select id from ContentBody where id in ('005800000032OjzAAE','00580000003DipbAAC','0051A000009H0LfQAK','005800000031SDBAA2','005C0000003bNFbIAM');
select id from ContentReference where id in ('005800000032OjzAAE','00580000003DipbAAC','0051A000009H0LfQAK','005800000031SDBAA2','005C0000003bNFbIAM');
select id from EmailMessage where id in ('005800000032OjzAAE','00580000003DipbAAC','0051A000009H0LfQAK','005800000031SDBAA2','005C0000003bNFbIAM');
select id from Event where id in ('005800000032OjzAAE','00580000003DipbAAC','0051A000009H0LfQAK','005800000031SDBAA2','005C0000003bNFbIAM');
select id from FileFieldData where id in ('005800000032OjzAAE','00580000003DipbAAC','0051A000009H0LfQAK','005800000031SDBAA2','005C0000003bNFbIAM');
select id from Lead where id in ('005800000032OjzAAE','00580000003DipbAAC','0051A000009H0LfQAK','005800000031SDBAA2','005C0000003bNFbIAM');
select id from RichTextAreaFieldData where id in ('005800000032OjzAAE','00580000003DipbAAC','0051A000009H0LfQAK','005800000031SDBAA2','005C0000003bNFbIAM');


-- FIND DOCUMENT
select * from Attachment where ID not like '00P%'
select * from Attachment where ID in ('00P1A00000krc0WUAQ','00P1A00000xs5vRUAQ','00P1A00000xs6G9UAI','00P1A00000zBs6eUAC')
select * from Attachment where Name like '%Mayeul PEROUSE%' OR Name like '%Haakon Froyset%'
/*mv 00P1A00000krc0WUAQ "00P1A00000krc0WUAQ_150815_VirgilMcClendon,_US.doc"
mv 00P1A00000xs5vRUAQ "00P1A00000xs5vRUAQ_170330_HeinzRotzoll_DE (project, change management).pdf"
mv 00P1A00000xs6G9UAI "00P1A00000xs6G9UAI_170330_HeinzRotzoll_DE (project, change management-german).pdf"
mv 00P1A00000zBs6eUAC "00P1A00000zBs6eUAC_Brendan Murphy Procurement Demand Planning, Supply Chain, Logistics Consultancy CV 2017.docx" */

select concat('mv "',Name,'" "',id,'_',Name,'"') from Attachment where Name in (
'171013_LFS_VijayChopra_US.docx',
'171016_NoemieLambert_FR.pdf',
'171018_Eggers_CPLIR_Atzel.pdf',
'171018_ProConseil_CPLIR_Gondouin.pdf',
'171020_SergeyMikheev_RU.pdf',
'171023_ElsemiekeLaarman_BE.docx',
'171023_LouiseOger_BE.pdf',
'171023_MatthieuRoze_FR.pdf',
'171023_MayeulPerousedeMontclos_FR.DOC',
'171023_NathalieleMer_FR.pdf',
'171023_RichardOshowole_UK.docx',
'rename.csv',
'rename_salesforce.sh',
'sed2P9WvG',
'WRD000.jpg')



select at.ParentId, concat(at.id,'_',replace(at.Name,',','') ) as doc --, a.name
        -- select count(*) --107656 = 99769 non supported + 4741
        from Attachment at
        left join Account a on a.id = at.ParentId
        left join Contact b on b.id = at.ParentId
        left join Lead c on c.id = at.ParentId
        where (at.name like '%doc' or at.name like '%docx' or at.name like '%pdf' or at.name like '%rtf' or at.name like '%xls' or at.name like '%xlsx')
        --where (at.name not like '%doc' and at.name not like '%docx' and at.name not like '%pdf' and at.name not like '%rtf' and at.name not like '%xls' and at.name not like '%xlsx')
        and (a.id is not null or b.id is not null or c.id is not null )



--  attactment
select * from Attachment where exists(select Id from Lead where Id = AccountId)
select count(*) from Attachment where Name like ''
select distinct (reverse( substring(reverse(Name),1,4 ) )) as type from Attachment

with t as (
       select reverse( substring(reverse(Name),1,4 ) ) as type from Attachment )
       select type , count(*) as AMOUNT from t
       where (type like '%doc' or type like '%docx' or type like '%pdf' or type like '%rtf' or type like '%xls' or type like '%xlsx')
       and (type not like '%doc' and type not like '%docx' and type not like '%pdf' and type not like '%rtf' and type not like '%xls' and type not like '%xlsx')
       group by type Order By type
-- TOTAL = 7887



select Comments_FJ_Fr__c, Comments_FNJ_Fr__c from Account
select X1st_LinkedIn_degree_contact_of__c from Contact

-- task
select * from task where whoid = '000000000000000AAA' and subject not like 'Unresolved Email%'
select count(*) from task where id in (select Id from Account);
select count(*) from task where whoid in (select Id from Account);
select count(*) from task where whatid in (select Id from Account);
select count(*) from task where id in (select Id from Contact);
select count(*) from task where whatid in (select Id from Contact);
select count(*) from task where id in (select Id from Lead)
select count(*) from task where whoid in (select Id from Lead) --28815
select count(*) from task where whatid in (select Id from Lead)

select * from contact where accountid = '001C000001Dp2I9IAJ'
select distinct whoid from task where whoid not in (
select whoid from task where whoid in (select Id from Contact) --22670;
union select whoid from task where whoid in (select Id from Lead) --28815
)

select t.WhoId, c.FirstName, c.LastName, * 
from Task t
left join Contact c on c.Id = t.WhoId

select Description, replace(_Description_,'  ',char(10)) as ABC
from task 
where exists(select Id from Account where Id = AccountId)
and Id = '00T0y00005H5BuAEAV'

select l._FirstName, l._LastName, l._Id, l._Email, t._Description_, * 
from task t
left join Lead l on l._Id = t._WhoId
where exists(select Id from Lead where Id = WhoId)


-- event
select * from Event
select count(*) from Event where id in ( select id from Contact) --0
select count(*) from Event where whoid in ( select id from Contact) --0
select count(*) from Event where whatid in ( select id from Contact) --0

-- note
select * from Note
select count(*) from Note where id in ( select id from Contact) --0
select count(*) from Note where parentid in ( select id from Contact) --39
select count(*) from Note where accountid in ( select id from Contact) --52
--select  a.*, n.* from Note n left join account a on a.id = n.parentid where a.id is not null
select  a.*, n.* from Note n left join account a on a.id = n.accountid where a.id is not null --and a.id <> n.parentid 

-- lead
select * from Lead
select l._FirstName, l._LastName, l._Id, l._Reference_CV__c, * from Attachment a
left join Lead l on l._Id = a._ParentId
where exists(select Id from Lead where Id = ParentId) --4917 rows



