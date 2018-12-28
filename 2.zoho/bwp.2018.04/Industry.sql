
-- ALTER DATABASE [bwp] SET COMPATIBILITY_LEVEL = 130

with 
  i1 (id, ind ) as ( SELECT ContactId, ltrim(rtrim(ContactTypeofIndustry.value)) as ContactTypeofIndustry FROM Contacts m CROSS APPLY STRING_SPLIT(m.ContactTypeofIndustry,',') AS ContactTypeofIndustry ) -- CONTACT  29
, i2 (id, ind ) as ( SELECT JobOpeningId, ltrim(rtrim(Industry.value)) as Industry FROM JobOpenings m CROSS APPLY STRING_SPLIT(m.Industry,',') AS Industry ) -- JOB
, i3 (id, ind ) as ( SELECT CandidateId, ltrim(rtrim(Industry.value)) as Industry FROM Candidates m CROSS APPLY STRING_SPLIT(m.Industry,',') AS Industry ) -- CANDIDATE
--select distinct ind from i3
--select distinct i.ind from ( select distinct ind from i1 UNION ALL select distinct ind from i2 UNION ALL select distinct ind from i3 ) i

, i  as (
select
        id As 'externalId'
       ,case ind
              when 'Accounting' then 28884
              when 'Administration' then 28885
              when 'Advertising' then 28886
              when 'Architecture' then 28887
              when 'Art &amp; Galleries' then 28888
              when 'Art & Galleries' then 28888
              when 'Aviation' then 28889
              when 'Banking &amp; Finance' then 28890
              when 'Banking & Finance' then 28890
              when 'Business Development' then 28891
              when 'Communications' then 28892
              when 'Construction' then 28893
              when 'Consultancy' then 28804
              when 'Consulting' then 28894
              when 'Education' then 28895
              when 'Engineering' then 28882
              when 'Events' then 28896
              when 'Galleries & Museums' then 28897
              when 'Healthcare' then 28898
              when 'Hospitality' then 28899
              when 'HR & Administration' then 28900
              when 'HR &amp; Administration' then 28900
              when 'Industrial' then 28901
              when 'Investment' then 28902
              when 'IT' then 28824
              when 'Legal' then 28903
              when 'Logistics &amp; Supply Chain' then 28904
              when 'Logistics & Supply Chain' then 28904
              when 'Manufacturing' then 28905
              when 'Marketing' then 28774
              when 'Media' then 28839
              when 'Mining' then 28757
              when 'Oil & Gas' then 28906
              when 'Oil &amp; Gas' then 28906
              when 'Procurement' then 28907
              when 'Property & Engineering' then 28908
              when 'Property &amp; Engineering' then 28908
              when 'Property & Engi' then 28908
              when 'Property & Engineer' then 28908
              when 'Public Relations' then 28909
              when 'Publishing' then 28910
              when 'Rail' then 28911
              when 'Real Estate' then 28912
              when 'Recruitment' then 28913
              when 'Retail' then 28780
              when 'Sales' then 28868
              when 'Speciality Service Provider' then 28914
              when 'Telecommunications' then 28763
              when 'Trade' then 28915
              end as ind
       , current_timestamp as timestamp
from i2 where id is not null and ind <> '' and ind is not null )

--select externalId from i group by externalId having count(*) > 1
select * from i


-- contact
-- select distinct ContactTypeofIndustry from Contacts c

-- job
--select distinct Industry from JobOpenings J
--select JobOpeningId as 'position-externalId' ,Industry from JobOpenings J


-- candidate
--select distinct Industry from Candidates c
--select c.CandidateId As 'candidate-externalId', c.Industry from Candidates c

 