/*
with
  BusinessSector0(userid, businessSectorID) as (SELECT userid, Split.a.value('.', 'VARCHAR(2000)') AS businessSectorID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(businessSectorIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') AS Split(a) )
, BusinessSector(userId, BusinessSector) as (SELECT userId, STUFF((SELECT DISTINCT ', ' + BSL.name from BusinessSector0 left join bullhorn1.BH_BusinessSectorList BSL ON BusinessSector0.businessSectorID = BSL.businessSectorID WHERE BusinessSector0.businessSectorID <> '' and userId = a.userId FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '')  AS URLList FROM BusinessSector0 as a where a.businessSectorID <> '' GROUP BY a.userId)
--, BusinessSector(userId, BusinessSector) as (SELECT userId, BSL.name from BusinessSector0 left join bullhorn1.BH_BusinessSectorList BSL ON BusinessSector0.businessSectorID = BSL.businessSectorID )
-- select distinct BusinessSector from BusinessSector0

select 
              C.candidateID as 'candidate-externalId' , C.userid
		, Coalesce(NULLIF(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
              , Coalesce(NULLIF(replace(C.LastName,'?',''), ''), concat('Lastname-',C.userID)) as 'contact-lastName'
              , BS.BusinessSector as 'Industry' --<<
from bullhorn1.Candidate C --where C.isPrimaryOwner = 1 --8545
left join BusinessSector BS on BS.userid = C.userid
where C.isPrimaryOwner = 1 and BS.BusinessSector is not null  
*/

with
-- BusinessSector: split by separate rows by comma, then combine them into Business Sector(Industry)
  BusinessSector0(userid, businessSectorID) as (SELECT userid, Split.a.value('.', 'VARCHAR(2000)') AS businessSectorID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(businessSectorIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') AS Split(a) )
, BusinessSector(userId, BusinessSector) as (SELECT userId, BSL.name from BusinessSector0 left join bullhorn1.BH_BusinessSectorList BSL ON BusinessSector0.businessSectorID = BSL.businessSectorID WHERE BusinessSector0.businessSectorID <> '')
--select top 100 * from BusinessSector where userid = 11994
, bs0(userid,BusinessSector) as (
       select userid
       ,case
when businessSector = 'Accounts' then 'Accounting Settlements'
when businessSector = 'Advisory' then 'Consultancy Advisory'
when businessSector = 'Asset Management' then 'Asset Management'
when businessSector = 'Back Office' then 'Accounting Settlements'
when businessSector = 'Broker' then 'Broking'
when businessSector = 'Business Manager' then 'Consultancy Advisory'
when businessSector = 'Buy Side Analytics' then 'Asset Management'
when businessSector = 'BYT - Bright Young Thang' then 'Capital Markets'
when businessSector = 'Cash Origination' then 'Investment Banking'
when businessSector = 'Cash Sales' then 'Capital Markets'
when businessSector = 'Cash Trading' then 'Capital Markets'
when businessSector = 'Corporate Finance' then 'Investment Banking'
when businessSector = 'Counterparty Credit Risk' then 'Risk Management'
when businessSector = 'Credit Research' then 'Investment Banking'
when businessSector = 'Credit Risk' then 'Risk Management'
when businessSector = 'Delta One Trading' then 'Capital Markets'
when businessSector = 'Derivatives Origination' then 'Investment Banking'
when businessSector = 'Derivatives Sales' then 'Capital Markets'
when businessSector = 'Derivatives Structuring' then 'Capital Markets'
when businessSector = 'Derivatives Trading' then 'Capital Markets'
when businessSector = 'Desk Analyst' then 'Capital Markets'
when businessSector = 'Desk Quantitative Analyst' then 'Capital Markets'
when businessSector = 'Desk Support' then 'Accounting Settlements'
when businessSector = 'Due Diligence' then 'Risk Management'
when businessSector = 'Economist' then 'Investment Banking'
when businessSector = 'Equity Research' then 'Investment Banking'
when businessSector = 'Fixed Income Research' then 'Investment Banking'
when businessSector = 'Front office' then 'Capital Markets'
when businessSector = 'Fund Management' then 'Asset Management'
when businessSector = 'Fund Manager' then 'Asset Management'
when businessSector = 'Hedge Funds' then 'Asset Management'
when businessSector = 'Human Resourcers Management' then 'Human Resources'
when businessSector = 'IT' then 'Information Technology'
when businessSector = 'IT Development' then 'Information Technology'
when businessSector = 'Legal Support' then 'Legal Counsel'
when businessSector = 'Leveraged Finance' then 'Investment Banking'
when businessSector = 'Loan Book Managment' then 'Capital Markets'
when businessSector = 'Long Short Strategies' then 'Capital Markets'
when businessSector = 'Macro Strategies' then 'Capital Markets'
when businessSector = 'Market Analytics and Research' then 'Capital Markets'
when businessSector = 'Market Risk' then 'Risk Management'
when businessSector = 'Methodology' then 'Risk Management'
when businessSector = 'Model Validation' then 'Capital Markets'
when businessSector = 'Operational Risk' then 'Risk Management'
when businessSector = 'Portfolio Management' then 'Capital Markets'
when businessSector = 'Private Equity Management' then 'Private Equity'
when businessSector = 'Product Control' then 'Accounting Settlements'
when businessSector = 'Prop Trading' then 'Capital Markets'
when businessSector = 'Quantitative Analytics BO' then 'Capital Markets'
when businessSector = 'Quantitative Analytics FO' then 'Capital Markets'
when businessSector = 'Quantitative Analytics MO' then 'Capital Markets'
when businessSector = 'Quantitative Developer' then 'Capital Markets'
when businessSector = 'Relative Value Trading' then 'Capital Markets'
when businessSector = 'Research' then 'Investment Banking'
when businessSector = 'Research and Strategy' then 'Investment Banking'
when businessSector = 'Risk Control' then 'Risk Management'
when businessSector = 'Risk Management' then 'Risk Management'
when businessSector = 'Solutions Marketing Structurer/Sales' then 'Capital Markets'
when businessSector = 'Strategy' then 'Investment Banking'
when businessSector = 'Strategy / Strategist' then 'Capital Markets'
when businessSector = 'Valuations' then 'Risk Management'
              end as 'BusinessSector'
              from businessSector --where businessSector <> ''
)

,bs1(userid,businessSector) as (
       select userid,
       case
       when businessSector = 'Accounting Settlements' then 28885
       when businessSector = 'Asset Management' then 28886
       when businessSector = 'Broking' then 28887
       when businessSector = 'Capital Markets' then 28888
       when businessSector = 'Consultancy Advisory' then 28889
       when businessSector = 'Human Resources' then 28890
       when businessSector = 'Information Technology' then 28891
       when businessSector = 'Investment Banking' then 28892
       when businessSector = 'Legal Counsel' then 28893
       when businessSector = 'Private Equity' then 28894
       when businessSector = 'Risk Management' then 28895
       end as 'businessSector'
       from bs0 )


	select --top 200
		 C.candidateID as 'candidate-externalId'
		, Coalesce(NULLIF(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
                , Coalesce(NULLIF(replace(C.LastName,'?',''), ''), concat('Lastname-',C.userID)) as 'contact-lastName'
		, C.middleName as 'candidate-middleName'
		, bs1.BusinessSector as 'Industry' --<<
	-- select count(*) --136361-- select distinct title --employeeType --employmentPreference -- select skillset, skillIDlist, customTextBlock1 --select top 10 * 
	from bullhorn1.Candidate C
	left join bs1 on bs1.userid = C.userid
	where C.isPrimaryOwner = 1 and bs1.BusinessSector is not null
