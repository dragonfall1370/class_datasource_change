--select [dbo].[ufn_GetHashCode]('a')

--select [dbo].[ufn_GetHashCode]('b')

--select [dbo].[ufn_GetHashCode]('ab')

select [dbo].[ufn_GetHashCode]('001b00000044tF3AAI')

select [dbo].[ufn_GetHashCode]('000000000000000AAA')

--select 97*1 + 98 * 2

--select 98*1 + 97*2

select [dbo].[ufn_PopulateFileName]('/a\dk|jfdb,c.doc', '001b00000044tF3AAI')

select [dbo].[ufn_PopulateFileName]('/a\dk|jfdb,c.doc', '9999')

select reverse('/a\dk|jfdb,c.doc')

--select
--BillingCountry
--from Account

--select * from Account
--where Website like '%w.alpha-sense.com%'

--select [dbo].[ufn_RefineWebAddress]('www.alpha-sense.com/dfjdk')

--select count(*) from VCCompanies where len([company-document]) > 0 --212

select count(*) from VCCompanies
where [company-name] like 'Candidate%'

select count(*) from VCCompanies
where [company-name] like 'Client%'

select count(*) from VCCompanies

select count(*) from Contact where AccountId in (
	select Id from Account
	where [Name] like '%Candidate%'
)
-- 16974
-- 17727
select count(*) from Contact
-- 29887
select count(*) from Contact where AccountId not in (
	select Id from Account
	where [Name] like 'Candidate%' or [Name] like 'Client%'
)
-- 12913

select Id from Account
	where [Type] = 'Customer'


select count(*) from VCCompanies
where [company-name] like 'Client%'

select count(*) from Contact where AccountId = '001200000035uJOAAY'

select * from Account
where Id in (
	'0010O00001jTgaOQAS'
	, '0010O00001vwJP2QAM'
	, '001200000035uJOAAY'
	, '0010O00001r8mZpQAI'
	, '0010O00001l6yRhQAI'

)

--select count(*) from Contact -- 29887
select * from Contact
where AccountId not in (
	select Id from Account
)
-- 128

--select * from Account where Id = '000000000000000AAA'

--select * from Opportunity

--select * from OpportunityHistory

--select * from [Lead]

--select * from NotifDeliveryUserPref

select * from Contact where Id = '0030O000023u9dsQAA'

select * from Account where Id = '0012000001aI1PhAAK'

select * from Contact where AccountId in (
	select Id from Account
	where [Name] like '%Candidate%'
)

select count(*) from Contact -- 30642
select 30642 - 755 -- 29887
select 12160 + 755
select 12787 - 12160 -- 627

select 

-- contacts: 12160 => 12787
-- candidates: 17727

--select 12160 + 17727 -- 29887

--select count(*) from Contact -- 29887

select count(*) from Attachment
where ParentId in (
	select Id from Opportunity
)

-- 155

select count(*) from Attachment
where ParentId in (
	select Id from Contact where Id is not null and Id <> '000000000000000AAA'
)
-- 109

select count(*) from Attachment
where ParentId in (
	select Id from Account
)
-- 277

select count(*) from Attachment
where ParentId in (
	select Id from [EmailMessage]
)
-- 21

--select 277 + 109 + 155 + 21 -- 562
-- Account + Contact + Opportunity + EmailMessage

select * from Opportunity