
/*
-- SPECIALTY - VC SFE info
with
  SpecSplit(userid, specialtyid) as (SELECT userid,Split.a.value('.','varchar(2000)') AS SpecialtyID FROM (SELECT userid,CAST('<M>' + REPLACE(cast(specialtyIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate) t CROSS APPLY x.nodes('/M') as Split(a) )
, SpeName(Userid, Name) as (SELECT Userid, VS.name from SpecSplit left join bullhorn1.View_Specialty VS ON SpecSplit.SpecialtyID = VS.specialtyID WHERE SpecSplit.specialtyid <> '')
select distinct specialtyid from SpecSplit

-- ADMISSION
with
  AdmissionRows(userId, CombinedText) as (select UCOI.userID, concat(text1,' ',text2) as CombinedText from bullhorn1.BH_UserCustomObjectInstance UCOI inner join bullhorn1.BH_CustomObjectInstance COI On UCOI.instanceID = COI.instanceID)
, admission(Userid, Admission) as (SELECT Userid, STUFF((SELECT ' || ' + CombinedText from  AdmissionRows WHERE Userid = c.Userid and CombinedText is not NULL and CombinedText <> '' FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 4, '')  AS URLList FROM  AdmissionRows as c GROUP BY c.Userid)
select top 100 * from admission
*/

/*
-- BusinessSector: split by separate rows by comma, then combine them into Business Sector(Industry)
with
  BusinessSector0(userid, businessSectorID) as (SELECT userid, Split.a.value('.', 'VARCHAR(2000)') AS businessSectorID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(businessSectorIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') AS Split(a) )
, BusinessSector(userId, BusinessSector) as (SELECT userId, BSL.name from BusinessSector0 left join bullhorn1.BH_BusinessSectorList BSL ON BusinessSector0.businessSectorID = BSL.businessSectorID where BSL.name is not null )
--select count(*) from BusinessSector --11792
select CA.candidateID, concat(ca.FirstName,' ',ca.lastName)
        ,case BS.BusinessSector
        When 'Advertising' then 'Advertising and Marketing'
        When 'Aerospace' then 'Aerospace'
        When 'Agriculture' then 'Agriculture'
        When 'Asset Management' then 'Asset Management'
        When 'Banking' then 'Banking'
        When 'Chemicals' then 'Chemicals'
        When 'Commodity Trading' then 'Agriculture'
        When 'Consumer Electronics' then 'Agriculture'
        When 'Education' then 'Higher Education'
        When 'Energy' then 'Oil & Energy'
        When 'Engineering' then 'Engineering Services'
        When 'Finance' then 'Financial Services'
        When 'FMCG' then 'FMCG'
        When 'Food & Beverages' then 'Food & Beverage'
        When 'Government' then 'Government'
        When 'Healthcare/Pharmaceuticals' then 'Healthcare'
        When 'HOSPITALITY/TRAVEL' then 'Travel & Hospitality'
        When 'HR Consulting' then 'Professional Services'
        When 'Insurance' then 'Insurance'
        When 'IT Hardware' then 'Information Technology'
        When 'IT Software' then 'Information Technology'
        When 'Legal' then 'Legal Services'
        When 'Logistics' then 'Logistics and Supply Chain'
        When 'Management Consulting' then 'Professional Services'
        When 'Manufacturing' then 'Light Industrial'
        When 'Media' then 'Media'
        When 'Property' then 'Real Estate'
        When 'Recruitment' then 'Staffing and Recruiting'
        When 'Retail' then 'Retail'
        When 'Semiconductor' then 'Light Industrial'
        When 'Shipping' then 'Logistics and Supply Chain'
        When 'Telecommunications' then 'Telecommunications'
        When 'Transportation' then 'Transportation'
        end as industry       
from BusinessSector bs --33
left join bullhorn1.Candidate CA on ca.userid = bs.userid where ca.isPrimaryOwner = 1 and CA.candidateID = 4435
--select distinct BusinessSector from BusinessSector --33
*/

with
-- FE
  CateSplit(userid, categoryid) as (SELECT userid, Split.a.value('.','varchar(2000)') AS categoryID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(categoryIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') as Split(a) )
, CName(Userid, Name) as (SELECT Userid, CL.occupation from CateSplit left join bullhorn1.BH_CategoryList CL ON CateSplit.categoryid = CL.categoryID )
--select count(distinct userid) from CName --11792
--select distinct Name from CName

-- SFE
-- SkillName: split by separate rows by comma, then combine them into SkillName
, SkillName0(userid, skillID) as (SELECT userid, Split.a.value('.', 'VARCHAR(2000)') AS skillID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(skillIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') AS Split(a) )
, SkillName(userId, SkillName) as (SELECT userId, SL.name from SkillName0 left join bullhorn1.BH_SkillList SL ON SkillName0.skillID = SL.skillID WHERE SkillName0.skillID <> '')
--select distinct SkillName from SkillName
-- where SkillName in ('Product Mgmt & Marketing','Customer/Data Analytics','Cash Ops','Investment/Portfolio Mgmt','Investment research and analysis','Credit admin/ops','Card Ops','HR Analytics','Compensation','Benefits','L&D','Robotic process automation (RPA)','AI & machine learning','JD Edwards ERP','Avaloq','Cognos','Hyperion','Bloomberg','Reuters','Matlab','Labview','Pro E+','SAS','Qlikview','Tableau','R Programming','SPSS','Mobile app developer','Assistant Manager','Senior Manager','Local','Startup','Social Insights & Analytics','Art Creative Director','Copy Art Director','Integrated Marketing','Digital media','Customer/Data Analytics','Risk & Compliance','Advisory/Sales','Investment/Portfolio Mgmt','Project Mgmt/Transformation','Client Service/Call Centre','Capex or Opex category sourcing','Chemical sourcing','Consumables category','Electrical category','Electronic component category','EMS category','Flavour category','Frangrance category','IT category sourcing','Logistic category sourcing','Marketing category sourcing','Mechanical category','NPI category sourcing','Oil & gas sourcing','Professional category sourcing','Project category sourcing','Supplier mgmt','Raw material sourcing','Reverse auction','Distribution','Media research')

, cfe as (select CA.candidateID, concat(ca.FirstName,' ',ca.lastName) as fullname
        --, CName.Name as fe
, case cn.name
When 'Accounting & Finance' then 'Accounting & Finance'
When 'Banking' then 'Banking'
When 'Commercial Planning' then 'Commercial Planning'
When 'Fundraising' then 'Fundraising'
When 'General Manager' then 'General Manager'
When 'HR' then 'HR'
When 'Insurance' then 'Insurance'
When 'IT Skills' then 'IT Knowledge'
When 'IT&T Functions' then 'IT & T'
When 'Job Seniority' then 'Job Seniority'
When 'Marketing & Communications' then 'Marketing & Communications'
When 'Media' then 'Media'
When 'Operations' then 'Operations'
When 'Other Area(s)' then 'Administrative'
When 'Procurement' then 'Procurement'
When 'Sales/BD' then 'Sales & BD'
When 'Supply Chain' then 'Supply Chain Management' end as fe        
        --, sn.SkillName as sfe
, case sn.SkillName
When 'Account Management' then 'Account mgmt'
When 'Advisory' then 'Advisory'
When 'Affiliate Sales' then 'Affiliate sales'
When 'AIX' then 'AIX'
When 'Application support' then 'Application support'
When 'AR and AP' then 'Accounts receivable & Accounts payables'
When 'Architect' then 'Software architect'
When 'ASP.NET' then 'ASP.net'
When 'Asset Management' then 'Asset mgmt ops'
When 'Audit & Assurance Director' then 'Audit & assurance'
When 'banking ops & processing' then 'Branch ops'
When 'BCM/BCP' then 'BCM/BCP'
When 'Bonds/Fixed Income' then 'Bonds/Fixed income'
When 'Brand Manager' then 'Brand manager'
When 'Business Analyst' then 'Business analyst'
When 'Business Development' then 'Business development'
When 'Business Intelligence(BI)' then 'Business Intelligence(BI)'
When 'Business Objects' then 'Business Objects'
When 'business planning' then 'Business planning'
When 'BUSINESS PLANNING / ANALYSIS' then 'Business planning / analysis'
When 'C#' then 'C#'
When 'C&B' then 'C&B'
When 'C++' then 'C++'
When 'Campus Recruitment' then 'Campus recruitment'
When 'CAPACITY PLANNING' then 'Capacity planning'
When 'CFO' then 'CFO'
When 'Change mgt/M&A/HR Transformation/Project Management' then 'Change mgmt/M&A/HR transformation project'
When 'CIO/CTO' then 'CIO/CTO'
When 'Claims processing' then 'Claims processing'
When 'Client Services' then 'Credit admin/ops'
When 'Commercial Banking' then 'Wholesale banking ops'
When 'Commodities' then 'Commodities'
When 'Compliance' then 'Compliance'
When 'Consolidation' then 'Consolidation'
When 'Consumer/Retail Banking' then 'Retail banking ops'
When 'Content' then 'Content'
When 'Corp Comms' then 'Corp comms'
When 'Corporate Banking' then 'Wholesale banking ops'
When 'Corporate Finance' then 'Corporate finance'
When 'Costing Controller' then 'Costing'
When 'Credit Controller' then 'Credit control'
When 'CRM' then 'CRM software'
When 'Crystal Reports' then 'Crystal reports'
When 'custody & settlements' then 'Custody & settlements'
When 'Customer Service' then 'Customer service'
When 'Data Analytics' then 'Data analytics'
When 'Data Centre Operations' then 'Data centre ops'
When 'Data Marketing' then 'Data marketing'
When 'Data Warehouse' then 'Data warehouse'
When 'Database Administrator' then 'Database administrator'
When 'Datamining' then 'Data mining'
When 'Demand Planning' then 'Demand planning'
When 'Derivatives' then 'Derivatives'
When 'Developer' then 'Software developer'
When 'Digital Marketing' then 'Digital marketing'
When 'Direct Purchasing' then 'Direct purchasing'
When 'Direct Tax' then 'Direct tax'
When 'Distribution' then 'Distribution'
When 'Diversity' then 'Diversity'
When 'Ecommerce' then 'eCommerce'
When 'Employee Communications' then 'Employee communications'
When 'Employee Relations' then 'Employee relations'
When 'Equities' then 'Equities'
When 'ERP - Oracle' then 'Oracle ERP'
When 'ERP - Others' then 'Other ERP'
When 'ERP - SAP' then 'SAP ERP'
When 'ERP- PeopleSoft' then 'Peoplesoft ERP'
When 'ETL' then 'ETL'
When 'Executive/Analyst' then 'Executive/Analyst'
When 'Expat' then 'Mobility'
When 'External Audit' then 'External audit'
When 'FINANCE CONTROLLER' then 'Finance controller'
When 'Finance Director' then 'Finance director'
When 'Finance Shared Services' then 'Finance shared services'
When 'Finance/Accounting Manager' then 'Finance manager'
When 'FINANCIAL ACCOUNTANT' then 'Financial accounting'
When 'Financial Analyst' then 'Financial planning & analysis'
When 'Front Office' then 'Sales/BD'
When 'fund admin/ops' then 'Fund admin/ops'
When 'Fundraising' then 'Fundraising'
When 'Futures' then 'Futures'
When 'FX/MM' then 'FX/MM'
When 'Hedge Fund' then 'Hedge fund'
When 'Hedging' then 'Hedging'
When 'Helpdesk/Desktop Support' then 'Helpdesk/Desktop Support'
When 'HR Director' then 'HR generalist'
When 'HR Executive' then 'HR generalist'
When 'HR Generalist' then 'HR generalist'
When 'HR Manager' then 'HR generalist'
When 'HR Operations' then 'HR ops'
When 'hr reporting' then 'HR reporting'
When 'HRBP' then 'HR BP'
When 'HRIS' then 'HRIS'
When 'HRSS' then 'HRSS'
When 'Indirect Purchasing' then 'Indirect purchasing'
When 'Indirect Tax' then 'Indirect tax'
When 'Internal Audit Director' then 'Internal audit'
When 'Inventory management' then 'Inventory mgmt'
When 'Investment banking' then 'Investment banking'
When 'IT Audit' then 'IT audit'
When 'IT governance' then 'IT governance'
When 'IT Manager' then 'IT mgmt'
When 'IT Security/ID Admin' then 'IT security/ID admin'
When 'Java' then 'Java'
When 'Journalist' then 'Journalist'
When 'JSP/Java servlets' then 'JSP/Java servlets'
When 'Linux' then 'Linux'
When 'Logistics' then 'Logistics'
When 'Management Accountant' then 'Management accounting'
When 'Manager' then 'Manager'
When 'manufacturing sourcing' then 'Manufacturing sourcing'
When 'Marcoms' then 'Marcomms'
When 'market planning' then 'Market planning'
When 'Market/Credit Risk' then 'Market/Credit risk'
When 'Marketing Manager' then 'Marketing manager'
When 'Master Data Management' then 'Master data mgmt'
When 'MATERIALS MGMT' then 'Material mgmt'
When 'Media Sales' then 'Media sales'
When 'midas/kondor+' then 'Midas/Kondor+'
When 'MS SQL' then 'MS SQL'
When 'Murex' then 'Murex'
When 'Network administrator' then 'Network administrator'
When 'Network Security' then 'Network security'
When 'New production introduction' then 'New product introduction'
When 'OD/L&D' then 'OD'
When 'Order fulfilment' then 'Order fulfilment'
When 'PA/Executive Admin/Secretary' then 'PA/Executive Admin/Secretary'
When 'packaging category sourcing' then 'Packaging category sourcing'
When 'payment & cash management' then 'Payment ops'
When 'Payroll' then 'Payroll'
When 'PL SQL/Oracle' then 'PL SQL/Oracle'
When 'Planning & Analysis Manager' then 'Financial Planning & Analysis'
When 'PMO' then 'PMO'
When 'Policy' then 'Policy'
When 'PR' then 'PR'
When 'PR Agency' then 'PR agency'
When 'Private Banking' then 'PB ops'
When 'Process re-engineering' then 'Process re-engineering'
When 'procurement excellence' then 'Procurement excellence'
When 'Product Control' then 'Product control'
When 'Product Management' then 'Product Mgmt/Marketing'
When 'Program Sales' then 'Program sales'
When 'Programming' then 'Media programming'
When 'Project/Programme Manager' then 'Project/Programme mgmt'
When 'projects & change management' then 'Project mgmt & transformation'
When 'Recruitment' then 'Recruitment'
When 'Regional' then 'Regional'
When 'Regulatory/Compliance' then 'Compliance'
When 'Relationship Management' then 'Relationship mgmt'
When 'Researcher/Resourcer' then 'Researcher/Resourcer'
When 'retail planning' then 'Retail planning'
When 'Risk Management' then 'Risk mgmt'
When 'Ruby on Rails' then 'Ruby on rails'
When 'Sales' then 'Sales'
When 'Sales Operations' then 'Sales ops'
When 'SAP ABAP' then 'SAP ABAP'
When 'SAP Basis' then 'SAP Basis'
When 'SCM processes - business' then 'SCM process - business'
When 'SCM processes – mfg' then 'SCM process - mfg'
When 'SEM' then 'SEM'
When 'SEO' then 'SEO'
When 'Sharepoint' then 'Sharepoint'
When 'Social Media' then 'Social media'
When 'Software Qa Analyst' then 'Software QA'
When 'Software Tester' then 'Software testing'
When 'Solaris' then 'Solaris'
When 'Storage Administrator' then 'Storage administrator'
When 'Structured/Hybrid Products' then 'Structured/Hybrid Products'
When 'Supply and demanding planning' then 'Supply and demand planning'
When 'Supply Planning' then 'Supply planning'
When 'System Administrator' then 'System administrator'
When 'System Integration' then 'System integration'
When 'T SQL/Sybase' then 'T SQL/Sybase'
When 'T&D' then 'T&D'
When 'Talent Management' then 'Talent Mgmt'
When 'Tax Director' then 'Tax'
When 'Tax Manager' then 'Tax'
When 'Team Lead' then 'Team lead'
When 'Technical Writer' then 'Technical writer'
When 'Trade Finance' then 'Trade ops'
When 'Transaction Banking' then 'Transaction services'
When 'Treasury director' then 'Treasury'
When 'Treasury Manager' then 'Treasury'
When 'Underwriter' then 'Underwriting'
When 'UNIX' then 'Unix'
When 'VB' then 'VB'
When 'VB.NET' then 'VB.NET'
When 'VBA' then 'VBA'
When 'VC++' then 'VC++'
When 'Voice/VoIP' then 'Voice/VoIP'
When 'VP/Director' then 'VP/Director'
When 'Warehouse and distribution' then 'Warehouse'
When 'Wealth Management' then 'Wealth Mgmt Ops'
When 'Website Developer' then 'Web app developer'
When 'Windows NT/2000/XP' then 'Windows servers'
When 'Workforce Planning' then 'Workforce planning'
When 'Algo/Electronic Trading' then 'Algo/Electronic trading'
When 'Consumer asset' then 'Consumer asset'
When 'Corporate planning' then 'Corporate strategy'
When 'Merchandise planning' then 'Merchandise planning'
When 'Community Management' then 'Community mgmt'
When 'CRM' then 'CRM'
When 'Digital media planner' then 'Digital media planner'
When 'Supply chain excellence' then 'Supply chain excellence'
When 'Supply chain finance' then 'Supply chain finance'
end as sfe
from bullhorn1.Candidate CA
left join CName cn on cn.userid = ca.userid
left join SkillName sn on sn.userid = ca.userid
where ca.isPrimaryOwner = 1
)

, t as (
select candidateID, fullname
, case fe
when 'Human Resource(sample only)' then 2981
when 'Automotive / Automotive Parts' then 3094
when 'Banking & Finance' then 3095
when 'Chemical / Material' then 3096
when 'Engineering' then 3097
when 'Executive Management' then 3098
when 'HR, GA & Facilities' then 3099
when 'IT & Telecoms' then 3100
when 'Industrial Equipment / Plant / Parts' then 3101
when 'Legal' then 3102
when 'Medical Device / Diagnostics / Analytical' then 3103
when 'Pharmaceutical / CRO / Reagents / Personal Care' then 3104
when 'Real Estate & Insurance' then 3105
when 'Sales & Marketing' then 3106
when 'Semiconductor / Embedded Device' then 3107
when 'Supply Chain Management' then 3108
when 'Accounting & Finance' then 3093
when 'Music' then 2982
when 'Administrative' then 3111
when 'Banking' then 3112
when 'Commercial Planning' then 3113
when 'Fundraising' then 3114
when 'General Manager' then 3115
when 'HR' then 3116
when 'Insurance' then 3117
when 'IT & T' then 3118
when 'IT Knowledge' then 3119
when 'Job Seniority' then 3120
when 'Marketing & Communications' then 3121
when 'Media' then 3122
when 'Operations' then 3123
when 'Procurement' then 3124
when 'Sales & BD' then 3125 end as feid
, sfe
from cfe
)
select top 200 * from cfe
--select top 200 *
--from t where feid is not null
--and candidateID = 300




