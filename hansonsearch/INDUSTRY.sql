/*
select COUNT(*)  from contacts WHERE Sector = 'FMCG' and descriptor = 2

select top 1000 * from SectorInstances si
select top 1000 * from Sectors s

with t as ( select si.ObjectId, s.Sector
            from SectorInstances si
            left join Sectors s on s.Sectorid = si.Sectorid 
            where s.sector is not null)

select
        DISTINCT c.ContactId, c.FirstName, c.LastName, c.DisplayName
                , t.Sector
        from contacts c
        left join t on t.ObjectId = c.contactid
        where c.descriptor = 2 and t.sector = 'FMCG'
        
                SELECT Distinct Contacts.DisplayName, Sectors.Sector, Contacts.ContactId
        FROM Segments INNER JOIN
             SgmtInstances ON Segments.SegmentId = SgmtInstances.SegmentId INNER JOIN
             Sectors ON Segments.SectorId = Sectors.SectorId INNER JOIN
             Contacts ON SgmtInstances.ObjectId = Contacts.ContactId
        Where Sectors.Sector like '%FMCG%' and Contacts.Descriptor = 2
Union
        Select Distinct DisplayName, Sector, ContactId from Contacts Where Sector<>'' and Sector like '%FMCG%' and Contacts.Descriptor = 2
Order By Sector
*/
--------
with ind as (
        SELECT Distinct Contacts.ContactId, Contacts.DisplayName, Sectors.Sector
        FROM Segments INNER JOIN
             SgmtInstances ON Segments.SegmentId = SgmtInstances.SegmentId INNER JOIN
             Sectors ON Segments.SectorId = Sectors.SectorId INNER JOIN
             Contacts ON SgmtInstances.ObjectId = Contacts.ContactId
        Where Contacts.Descriptor = 2 --Sectors.Sector like '%FMCG%' 
Union
        Select Distinct ContactId, DisplayName, Sector from Contacts Where Sector<>'' and Contacts.Descriptor = 2 --and Sector like '%FMCG%' 
)

--select sector,count(*) from ind group by sector Order By Sector
, ind0 as (
        select
        ContactId
        --, sector
/*        , case
                        when Sector = 'Ad Networks' then '28938'
                        when Sector = 'Advertising' then '28882'
                        when Sector = 'Advertising Agency ' then '28882'
                        when Sector = 'Branding/Design' then '28886'
                        when Sector = 'Business Intelligence' then '28888'
                        when Sector = 'Digital' then '28892'
                        when Sector = 'E Commerce' then '28893'
                        when Sector = 'Internal Communictions' then '28905'
                        when Sector = 'Investment Banking ' then '28906'
                        when Sector = 'IT' then '28902'
                        when Sector = 'Managmenet Consultancy' then '28909'
                        when Sector = 'Market Research/Insights' then '28910'
                        when Sector = 'Marketing' then '28911'
                        when Sector = 'Marketing services' then '28911'
                        when Sector = 'Media' then '28912'
                        when Sector = 'Medical Education' then '28913'
                        when Sector = 'Mobile Marketing' then '28914'
                        when Sector = 'Pharmacuetical/Bio Tech' then '28917'
                        when Sector = 'PR and Communications' then '28919'
                        when Sector = 'Public Affairs' then '28922'
                        when Sector = 'Recruitment Agency ' then '28925'
                        when Sector = 'Shopping Marketing' then '28928'
                        when Sector = 'Social Media' then '28929'
                        when Sector = 'Strategy Consulting' then '28931'
                        when Sector is null then 'Content Agency'
                        else ''
                        end as 'INDUSTRY' --> VC vertical.name

        , case
                        when Sector = 'Ad Networks' then 28938
                        when Sector = 'Advertising' then 28882
                        when Sector = 'Advertising Agency ' then 28882
                        when Sector = 'Branding/Design' then 28886
                        when Sector = 'Business Intelligence' then 28888
                        when Sector = 'Digital' then 28892
                        when Sector = 'E Commerce' then 28893
                        when Sector = 'Internal Communictions' then 28905
                        when Sector = 'Investment Banking ' then 28906
                        when Sector = 'IT' then 28902
                        when Sector = 'Managmenet Consultancy' then 28909
                        when Sector = 'Market Research/Insights' then 28910
                        when Sector = 'Marketing' then 28911
                        when Sector = 'Marketing services' then 28911
                        when Sector = 'Media' then 28912
                        when Sector = 'Medical Education' then 28913
                        when Sector = 'Mobile Marketing' then 28914
                        when Sector = 'Pharmacuetical/Bio Tech' then 28917
                        when Sector = 'PR and Communications' then 28919
                        when Sector = 'Public Affairs' then 28922
                        when Sector = 'Recruitment Agency ' then 28925
                        when Sector = 'Shopping Marketing' then 28928
                        when Sector = 'Social Media' then 28929
                        when Sector = 'Strategy Consulting' then 28931
                        when Sector is null then 28936
                        else ''
                        end as 'INDUSTRY' --> VC vertical.name
*/
                , case
                        when Sector = 'Accountancy' then 28881
                        when Sector = 'Arts and Culture' then 28883
                        when Sector = 'Asset Management' then 28884
                        when Sector = 'Automotive' then 28885
                        when Sector = 'Broadcaster' then 28887
                        when Sector = 'Charity' then 28889
                        when Sector = 'Construction' then 28890
                        when Sector = 'Defence' then 28891
                        when Sector = 'E-Commerce' then 28893
                        when Sector = 'Education' then 28894
                        when Sector = 'Energy' then 28895
                        when Sector = 'Engineering' then 28896
                        when Sector = 'Film and Entertainment' then 28897
                        when Sector = 'Financial Services' then 28898
                        when Sector = 'FinTech' then 28899
                        when Sector = 'FMCG' then 28900
                        when Sector = 'Healthcare' then 28901
                        when Sector = 'Information Technology' then 28902
                        when Sector = 'Infrastructure' then 28903
                        when Sector = 'Insurance' then 28904
                        when Sector = 'Luxury' then 28908
                        when Sector = 'Management Consultancy' then 28909
                        when Sector = 'Marketing Agency' then 28911
                        when Sector = 'Media Agency' then 28912
                        when Sector = 'Non Dept Gov Body' then 28915
                        when Sector = 'Not For Profit' then 28916
                        when Sector = 'Print and Publishing' then 28920
                        when Sector = 'Professional Services' then 28921
                        when Sector = 'Public Affairs Agency' then 28922
                        when Sector = 'Public Sector' then 28923
                        when Sector = 'Real Estate' then 28924
                        when Sector = 'Retail' then 28926
                        when Sector = 'Retail Banking' then 28927
                        when Sector = 'Social Media Agency' then 28929
                        when Sector = 'Sport' then 28930
                        when Sector = 'Technology' then 28918
                        when Sector = 'Telecoms' then 28932
                        when Sector = 'Trade Association' then 28933
                        when Sector = 'Transport' then 28934
                        when Sector = 'Utilities' then 28935
                        else ''
                        end as 'INDUSTRY' --> VC vertical.name
        from ind)
--select INDUSTRY,count(*) as AMOUNT from ind0 group by INDUSTRY Order By INDUSTRY
--select count(*) from ind0  where INDUSTRY <> '' --56309
--select * from ind0  where INDUSTRY <> ''




----------------------------------------------------------
----------------------------------------------------------
----------------------------------------------------------
-- CONTACT
with ind as (
        SELECT Distinct Contacts.ContactId, Contacts.DisplayName, Sectors.Sector
        FROM Segments INNER JOIN
             SgmtInstances ON Segments.SegmentId = SgmtInstances.SegmentId INNER JOIN
             Sectors ON Segments.SectorId = Sectors.SectorId INNER JOIN
             Contacts ON SgmtInstances.ObjectId = Contacts.ContactId
        Where Contacts.Descriptor = 1 --Sectors.Sector like '%FMCG%' 
Union
        Select Distinct ContactId, DisplayName, Sector from Contacts Where Sector<>'' and Contacts.Descriptor = 2 --and Sector like '%FMCG%' 
)

--select sector,count(*) from ind group by sector Order By Sector
, ind0 as (
        select
        ContactId
        , sector as 'INDUSTRY'
        from ind)
select INDUSTRY,count(*) as AMOUNT from ind0 group by INDUSTRY Order By INDUSTRY
--select count(*) from ind0  where INDUSTRY <> '' --56309
--select * from ind0  where INDUSTRY <> ''



-- 2018.01.23
select * from candidate_industry where vertical_id = 28919 --28919 PR and Communications Agency
select count(distinct candidate_id) from candidate_industry where vertical_id = 28919 --21747

select candidate_id,* from candidate_functional_expertise where functional_expertise_id = 3045; --3045 Medical Affairs
select count(distinct candidate_id) from candidate_functional_expertise where functional_expertise_id = 3045; --2209


-- On all of the candidates that have “Medical Education” FUNCTIONAL EXPERTISE, append the “PR and Communications Agency” INDUSTRY tag.
insert into candidate_industry (vertical_id,candidate_id,insert_timestamp) 
select 28919,t.candidate_id,now() 
from (  select distinct fe.candidate_id --count(distinct fe.candidate_id)
        from candidate_functional_expertise fe
        where fe.functional_expertise_id = 3045 
        and fe.candidate_id not in ( select count(distinct candidate_id) from candidate_industry where vertical_id = 28919 ) --1821
        ) t


