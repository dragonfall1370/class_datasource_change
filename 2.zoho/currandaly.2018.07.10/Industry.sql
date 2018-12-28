
-- ALTER DATABASE [currandaly] SET COMPATIBILITY_LEVEL = 130

with 
 i1 (id,ind) as (select ClientId, Industry from Clients c where Industry <> '')
--  i1 (id, ind ) as ( SELECT ClientId, ltrim(rtrim(Industry.value)) as Industry FROM Clients m CROSS APPLY STRING_SPLIT(m.Industry,',') AS Industry )
  --select * from i1
--, i2 (id, ind ) as ( SELECT JobOpeningId, ltrim(rtrim(Industry.value)) as Industry FROM JobOpenings m CROSS APPLY STRING_SPLIT(m.Industry,',') AS Industry ) -- JOB
--, i3 (id, ind ) as ( SELECT CandidateId, ltrim(rtrim(Industry.value)) as Industry FROM Candidates m CROSS APPLY STRING_SPLIT(m.Industry,',') AS Industry ) -- CANDIDATE
--select distinct ind from i3
--select distinct i.ind from ( select distinct ind from i1 UNION ALL select distinct ind from i2 UNION ALL select distinct ind from i3 ) i

, i  as (
select
        id As 'externalId'
       ,case ind
              when 'Accountancy' then 28735
              when 'Accounting' then 28886
              when 'Advertising and PR' then 28732
              when 'Airlines/Aviation' then 28887
              when 'Arts' then 28737
              when 'Automotive' then 28738
              when 'Banking' then 28768
              when 'Building and Construction' then 28767
              when 'Business Process Outsourcing' then 28803
              when 'Business Supplies and Equipment' then 28889
              when 'Chemicals' then 28890
              when 'Civil Engineering' then 28891
              when 'Community Services' then 28742
              when 'Construction' then 28892
              when 'Consultancy' then 28804
              when 'Consumer Services' then 28893
              when 'Design and Creative' then 28883
              when 'E-commerce' then 28885
              when 'Education and Training' then 28777
              when 'Education Management' then 28894
              when 'Electrical/Electronic Manufacturing' then 28895
              when 'Electronics' then 28881
              when 'Engineering' then 28882
              when 'Financial Services' then 28776
              when 'FMCG' then 28813
              when 'Food Production' then 28896
              when 'Gambling and Casinos' then 28897
              when 'Graduates and Trainees' then 28750
              when 'Graphic Design' then 28898
              when 'Headhunting' then 28899
              when 'Health, Wellness and Fitness' then 28900
              when 'Hospital and Catering' then 28784
              when 'Hospital Health Care' then 28901
              when 'Hospitality' then 28902
              when 'Human Resources and Personnel' then 28751
              when 'Information Technology and Services' then 28903
              when 'Insurance' then 28755
              when 'IT' then 28824
              when 'Legal ' then 28756
              when 'Leisure and Sport' then 28752
              when 'Leisure, Travel and Tourism' then 28904
              when 'Logistics and Supply Chain' then 28905
              when 'Logistics Distribution and Supply Chain' then 28760
              when 'Manufacturing and Production' then 28773
              when 'Marketing' then 28774
              when 'Marketing and Advertising' then 28906
              when 'Media' then 28839
              when 'Medical and Nursing' then 28840
              when 'Mining' then 28757
              when 'New Media and Internet' then 28770
              when 'Oil and Gas' then 28769
              when 'Outsourcing/Offshoring' then 28907
              when 'Pharmaceuticals' then 28758
              when 'Printing' then 28908
              when 'Property and Housing' then 28850
              when 'Public Relations and Communications' then 28859
              when 'Purchasing and Procurement' then 28852
              when 'Real Estate' then 28909
              when 'Real Estate and Property' then 28853
              when 'Recruitment Consultancy' then 28854
              when 'Retail' then 28780
              when 'Sales' then 28868
              when 'Science and Research' then 28764
              when 'Shared Services' then 28884
              when 'Staffing and Recruiting' then 28911
              when 'Supermarkets' then 28873
              when 'Telecommunications' then 28763
              when 'Textiles' then 28912
              when 'Trade and Services' then 28872
              when 'Transport and Rail' then 28796
              when 'Travel and Tourism' then 28875
              when 'Utilities' then 28778
              when 'Venture Capital and Private Equity' then 28913
              when 'Veterinary' then 28914
              else 00000
              end as ind
       , current_timestamp as timestamp
from i1 where id is not null and ind <> '' and ind is not null )

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

 