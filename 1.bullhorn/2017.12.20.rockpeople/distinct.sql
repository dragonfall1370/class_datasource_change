
with
-- SkillName: split by separate rows by comma, then combine them into SkillName
  SkillName0(userid, skillID) as (SELECT userid, Split.a.value('.', 'VARCHAR(2000)') AS skillID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(skillIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') AS Split(a) )
, SkillName(userId, SkillName) as (SELECT userId, SL.name from SkillName0 left join bullhorn1.BH_SkillList SL ON SkillName0.skillID = SL.skillID WHERE SkillName0.skillID <> '')
select distinct ltrim(SkillName) from SkillName --where SkillName
--select * from bullhorn1.BH_SkillList SL where name 
in ('Product Mgmt & Marketing','Customer/Data Analytics','Cash Ops','Investment/Portfolio Mgmt','Investment research and analysis','Credit admin/ops','Card Ops','HR Analytics','Compensation','Benefits','L&D','Robotic process automation (RPA)','AI & machine learning','JD Edwards ERP','Avaloq','Cognos','Hyperion','Bloomberg','Reuters','Matlab','Labview','Pro E+','SAS','Qlikview','Tableau','R Programming','SPSS','Mobile app developer','Assistant Manager','Senior Manager','Local','Startup','Social Insights & Analytics','Art Creative Director','Copy Art Director','Integrated Marketing','Digital media','Customer/Data Analytics','Risk & Compliance','Advisory/Sales','Investment/Portfolio Mgmt','Project Mgmt/Transformation','Client Service/Call Centre','Capex or Opex category sourcing','Chemical sourcing','Consumables category','Electrical category','Electronic component category','EMS category','Flavour category','Frangrance category','IT category sourcing','Logistic category sourcing','Marketing category sourcing','Mechanical category','NPI category sourcing','Oil & gas sourcing','Professional category sourcing','Project category sourcing','Supplier mgmt','Raw material sourcing','Reverse auction','Distribution','Media research')


-- BusinessSector: split by separate rows by comma, then combine them into Business Sector(Industry)
with
  BusinessSector0(userid, businessSectorID) as (SELECT userid, Split.a.value('.', 'VARCHAR(2000)') AS businessSectorID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(businessSectorIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') AS Split(a) )
, BusinessSector(userId, BusinessSector) as (SELECT userId, BSL.name from BusinessSector0 left join bullhorn1.BH_BusinessSectorList BSL ON BusinessSector0.businessSectorID = BSL.businessSectorID )
 select distinct BusinessSector from BusinessSector

-- CATEGORY - VC FE info
with
  CateSplit(userid, categoryid) as (SELECT userid, Split.a.value('.','varchar(2000)') AS categoryID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(categoryIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') as Split(a) )
, CName(Userid, Name) as (SELECT Userid, CL.occupation from CateSplit left join bullhorn1.BH_CategoryList CL ON CateSplit.categoryid = CL.categoryID )
select distinct Name from CName

, industry as (
        select  ca.candidateID
        , c.userid
        , case c.Name
        when 'Accounting & Finance' then null
        when 'Aerospace' then 28739
        when 'Agriculture' then 28736
        when 'Alternative Energy' then null
        when 'Automobile & Parts' then 28932
        when 'Aviation' then 28933
        when 'Banking' then 28768
        when 'Building Automation' then null
        when 'Casino, Gaming' then null
        when 'Chemicals' then 28900
        when 'Compliance' then null
        when 'Compressor & Refrigeration' then 28935
        when 'Computer Hard Ware' then null
        when 'Computer Software' then null
        when 'Construction Equipment' then 28916
        when 'Consulting' then 28804
        when 'Consumer Goods' then 28884
        when 'Contracts Managers' then null
        when 'Corporate Secretaries' then null
        when 'Defense' then null
        when 'eCommerce /Digital Media' then 28886
        when 'Education' then 28777
        when 'Electrical' then 28936
        when 'Electronics & Semiconductor' then 28918
        when 'Elevator/Escalator' then 28930
        when 'Energy (Power Generation)' then null
        when 'Engineering Services' then null
        when 'Environment, Water, Utilities' then null
        when 'EPC' then null
        when 'Fire, Security & Surveillance' then 28893
        when 'FMCG' then null
        when 'Food' then null
        when 'Healthcare' then null
        when 'Hospitality' then null
        when 'Human Resources & Administration' then 28751
        when 'HVAC' then 28937
        when 'Information Technology (Hardware)' then 28914
        when 'Information Technology (Software)' then 28915
        when 'Inhouse Legal' then null
        when 'Insurance/Financial Services' then null
        when 'Logistics' then null
        when 'Manufacturing' then null
        when 'Marine & Offshore Engineering' then 28925
        when 'Material Handling' then null
        when 'Media & Entertainment' then null
        when 'Medical Devices/Technology' then null
        when 'Mining' then null
        when 'Oil & Gas' then 28769
        when 'Other' then null
        when 'Other Area(s)' then null
        when 'Paints & Adhesives' then null
        when 'Paper & Packaging' then 28912
        when 'Pharmaceutical & Biotechnology' then null
        when 'Plant Automation' then 28938
        when 'Plastics' then null
        when 'Power Tools & Parts' then 28939
        when 'Precision Engineering' then 28929
        when 'Private Equity' then null
        when 'Private Practice (Legal)' then null
        when 'Procurement' then null
        when 'Professional Services' then null
        when 'Pumps & Valves' then null
        when 'Real Estate - Property - Construction' then 28853
        when 'Recruitment/Executive Search' then 28854
        when 'Renewable Energy' then 28910
        when 'Retail' then 28780
        when 'Sales & Marketing (B2B)' then null
        when 'Shipping' then null
        when 'Supply Chain' then 28760
        when 'Supply Chain, Logistics, Procurement' then 28760
        when 'Telco' then null
        when 'Telecommunications' then 28763
        when 'Testing & Certification' then 28919
        when 'Transportation' then 28796
        when 'Travel & Leisure' then null
        when 'Warehouse Logistcs' then null
        when 'Wireless' then null
        end as 'industry'
from cname c
left join bullhorn1.Candidate CA on CA.userID = c.Userid
)
select * from industry where candidateID is not null and industry is not null


-- SPECIALTY - VC SFE info
with
  SpecSplit(userid, specialtyid) as (SELECT userid,Split.a.value('.','varchar(2000)') AS SpecialtyID FROM (SELECT userid,CAST('<M>' + REPLACE(cast(specialtyIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate) t CROSS APPLY x.nodes('/M') as Split(a) )
, SpeName(Userid, Name) as (SELECT Userid, VS.name from SpecSplit left join bullhorn1.View_Specialty VS ON SpecSplit.SpecialtyID = VS.specialtyID WHERE SpecSplit.specialtyid <> '')
select distinct Name from SpeName

-- ADMISSION
with
  AdmissionRows(userId, CombinedText) as (select UCOI.userID, concat(text1,' ',text2) as CombinedText from bullhorn1.BH_UserCustomObjectInstance UCOI inner join bullhorn1.BH_CustomObjectInstance COI On UCOI.instanceID = COI.instanceID)
, admission(Userid, Admission) as (SELECT Userid, STUFF((SELECT ' || ' + CombinedText from  AdmissionRows WHERE Userid = c.Userid and CombinedText is not NULL and CombinedText <> '' FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 4, '')  AS URLList FROM  AdmissionRows as c GROUP BY c.Userid)
select top 100 * from admission
