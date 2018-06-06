
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
/*mv 00P1A00000krc0WUAQ "00P1A00000krc0WUAQ_150815_VirgilMcClendon,_US.doc"
mv 00P1A00000xs5vRUAQ "00P1A00000xs5vRUAQ_170330_HeinzRotzoll_DE (project, change management).pdf"
mv 00P1A00000xs6G9UAI "00P1A00000xs6G9UAI_170330_HeinzRotzoll_DE (project, change management-german).pdf"
mv 00P1A00000zBs6eUAC "00P1A00000zBs6eUAC_Brendan Murphy Procurement Demand Planning, Supply Chain, Logistics Consultancy CV 2017.docx" */

select * from Attachment where Name like '%Mayeul PEROUSE%' OR Name like '%Haakon Froyset%'

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
'171024_ChrisPaschal_US (Bio).doc',
'171024_ChrisPaschal_US(ConsultantAnalysisProjectHistory).xls',
'171024_JohnHowardPope_AUS.docx',
'171024_JulienLaupretre_FR.pdf',
'171024_NoémieLambert_FR (English).pdf',
'171024_NoémieLambert_FR (French).pdf',
'171024_PaulPhilippe_FR.docx',
'171024_PierreEdouardOuazzan_FR (English).pdf',
'171024_PierreEdouardOuazzan_FR (French).pdf',
'171024_RuiNorte_FR.pdf',
'171025_HaakonFroyset_2_NO.pdf',
'171025_HakonFroyset_NO.docx',
'171025_HakonFroyset_Projecthighlights_NO.docx',
'171025_LFS_AliJarrar_SAB.docx',
'171025_PierreEdouardOuazzan_FR(Summary of OE experience & achievements).docx',
'171026_AndySpiteri_FR.pdf',
'171026_JavierCasazza_CHL.pdf',
'171026_JavierCasazza_CHL (Spanish).pdf',
'171026_JonathanGriffiths_US.doc',
'171026_LFS_KonstantinBozhenko_RU_UKR.pdf',
'171026_ThomasMurray_US.pdf',
'CV Contrôle de Gestion v2- Mayeul PEROUSE.DOC',
'CV Haakon Froyset (English)2017 Consultant.docx',
'CV Haakon Froyset projecthighlights.docx',
'Hakon_Froyset_CV 2.pdf',
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

select count(*) from Attachment where Name like ''
-- select distinct (reverse( substring(reverse(Name),1,4 ) )) as type from Attachment

with t as (select reverse( substring(reverse(Name),1,4 ) ) as type from Attachment )
select type , count(*) as AMOUNT from t
where (type like '%doc' or type like '%docx' or type like '%pdf' or type like '%rtf' or type like '%xls' or type like '%xlsx')
and (type not like '%doc' and type not like '%docx' and type not like '%pdf' and type not like '%rtf' and type not like '%xls' and type not like '%xlsx')
group by type Order By type
-- TOTAL = 7887

