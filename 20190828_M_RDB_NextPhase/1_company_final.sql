with companyinIDobject as (
        select o.ObjectID,o.LocationId,o.FileAs,o.FlagText,o.SourceId,o.CreatedOn
        from dbo.Objects o
        where o.ObjectTypeId=2 --company
)
, alladdress as (
        select ad.ObjectId,ad.AddressId,adt.Description,ad.Building,ad.Street,ad.District,ad.City,ad.PostCode
		, lvct.ValueName as 'County'
		, lvc.ValueName as 'Country'
		, lvc.SystemCode as CountryCode
		, row_number() OVER(partition by ad.ObjectID order by ad.AddressID desc) AS rn --,ad.CountyValueId,ad.CountryValueId
        from dbo.Address ad
        left join dbo.AddressTypes adt on ad.AddressTypeId=adt.AddressTypeId
        left join dbo.ListValues lvc on lvc.ListValueId=ad.CountryValueId --country
        left join dbo.ListValues lvct on lvct.ListValueId=ad.CountyValueId --county
) --select distinct Country from alladdress
, company_address as (--All addresses group by company
        select ObjectId
		, string_agg(concat_ws(', '
				, nullif(trim(Building),'')
				, nullif(trim(Street),'')
				, nullif(trim(District),'')
				, nullif(trim(City),'')
				, nullif(trim(PostCode),'')
				, nullif(trim(County),'')
				, nullif(trim(Country),'')), char(10)) as company_address
        from alladdress
        group by ObjectId
)
, address_1 as (
        select  aad.ObjectId
                , nullif(TRIM(' ,''".+|*()[]\/{}' FROM aad.Building),'') as Building
                , nullif(TRIM(' ,''".+|*()[]\/{}' FROM aad.Street),'') as Street
                , nullif(TRIM(' ,''".+|*()[]\/{}' FROM aad.District),'') as District
                , nullif(TRIM(' ,''".+|*()[]\/{}' FROM aad.City),'') as City
                , nullif(TRIM(' ,''".+|*[]\/{}' FROM aad.PostCode),'') as PostCode
                , nullif(TRIM(' ,''".+|*[]\/{}' FROM aad.County),'') as County
                , nullif(TRIM(' ,''".+|*[]\/{}' FROM aad.Country),'') as Country
				, nullif(TRIM(' ,''".+|*[]\/{}' FROM aad.CountryCode),'') as CountryCode
                , concat_ws(', ',
					nullif(TRIM(' ,''".+|*()[]\/{}' FROM aad.Building),''),
					nullif(TRIM(' ,''".+|*()[]\/{}' FROM aad.Street),''),
					nullif(TRIM(' ,''".+|*()[]\/{}' FROM aad.District),''),
					nullif(TRIM(' ,''".+|*()[]\/{}' FROM aad.City),''),
					nullif(TRIM(' ,''".+|*[]\/{}' FROM aad.PostCode),''),
					nullif(TRIM(' ,''".+|*[]\/{}' FROM aad.County),''),
					nullif(TRIM(' ,''".+|*[]\/{}' FROM aad.Country),'')) as 'company_address'
        from alladdress aad
        where rn =1
)
--INJECT OTHER ADDRESSES LATER
, phone as (--PHONE as COMPANY SWITCHBOARD
		select p.ObjectID,p.PhoneId,p.CreatedOn
		, case when patindex('%[a-zA-Z]%',Num) > 0 then left(trim(num), patindex('%[a-zA-Z]%',Num) - 1) 
			else trim(Num) end as phone
        from Phones p
        where p.CommunicationTypeId = 79 --Phone
		) --reused for other entities
, switchboard as (select ObjectID
		, string_agg(nullif(phone,''),',') as switchboard
		from phone
		group by ObjectID
		)
, otherphone as (--ALL OTHER PHONES
				select p.ObjectID,p.PhoneId,p.CreatedOn
				, case when patindex('%[a-zA-Z]%',Num) > 0 then left(trim(num), patindex('%[a-zA-Z]%',Num) - 1) 
				else trim(Num) end as phone
        from Phones p
        where p.CommunicationTypeId in (81, 82, 87, 88) --phone: day, evening, office, home
		) 
, allotherphone as (select ObjectID
		, string_agg(nullif(phone,''),',') as allotherphone
		from otherphone
		group by ObjectID
)
, fax as (--FAX
			select p.ObjectID,p.PhoneId,p.CreatedOn
				, case when patindex('%[a-zA-Z]%',Num) > 0 then left(trim(num), patindex('%[a-zA-Z]%',Num) - 1) 
				else trim(Num) end as fax
        from dbo.Phones p
        where p.CommunicationTypeId = 80
		)
, fax_company as (select ObjectID
		, string_agg(nullif(fax,''),',') as fax_company
		from fax
		group by ObjectID
		)
, email as (--EMAIL
        select p.ObjectID, p.PhoneId, TRIM('.' FROM p.Num) as Email--,ROW_NUMBER() OVER(PARTITION BY p.ObjectID ORDER BY p.CreatedOn ASC) AS rn
        from Phones p
        where p.CommunicationTypeId=78 --CommunicationTypes: Email
        and p.Num like '%_@_%.__%'
)
, officeemail as (--OFFICE EMAIL
        select p.ObjectID,STRING_AGG(TRIM('.' FROM p.Num),',') as officeemail--,ROW_NUMBER() OVER(PARTITION BY p.ObjectID ORDER BY p.CreatedOn ASC) AS rn
        from Phones p
        where p.CommunicationTypeId=85 --Email (Office)
        and p.Num like '%_@_%.__%'
        group by p.ObjectID
)
, personemail as (--PERSON EMAIL
        select p.ObjectID,STRING_AGG(TRIM('.' FROM p.Num),',') as personemail--,ROW_NUMBER() OVER(PARTITION BY p.ObjectID ORDER BY p.CreatedOn ASC) AS rn
        from dbo.Phones p
        where p.CommunicationTypeId=86 --CommunicationTypes: Email (Personal)
        and p.Num like '%_@_%.__%'
        group by p.ObjectID
)
, network as (--NETWORK
        select p.ObjectID, STRING_AGG(TRIM('.' FROM p.Num),', ') as link --,ROW_NUMBER() OVER(PARTITION BY p.ObjectID ORDER BY p.CreatedOn ASC) AS rn
        from dbo.Phones p
        where p.CommunicationTypeId=90 --CommunicationTypes: Social Networking
        --and REPLACE(TRIM(' ,''".+|*()[]\/{}-' FROM p.Num),' ','') NOT LIKE '%[0-9]%' --get all values
		group by p.ObjectID
)
, allweb as (--URL WEBSITE
        select p.ObjectID,SUBSTRING(TRIM(' ,''".+|*()[]\/{}-' FROM p.Num),0,100) as website
		, p.CreatedOn,ROW_NUMBER() OVER(PARTITION BY p.ObjectID ORDER BY p.CreatedOn desc) AS rn
        from dbo.Phones p
        where p.CommunicationTypeId=89 --CommunicationTypes: URL
        and REPLACE(TRIM(' ,''".+|*()[]\/{}-' FROM p.Num),' ','') NOT LIKE '%[0-9]%'
)
, website as (
        select ObjectID, website
        from allweb
        where rn=1
)
, source as (
        select s.SourceId,s.SystemCode,s.Description
        from dbo.Sources s
)
, location as (
        select l.LocationId,l.Code,l.Description
        from dbo.Locations l
)
, userinfo as (
        select u.UserId,u.LoginName,u.UserName,u.UserFullName,u.JobTitle,u.Inactive
        from Users u
)
, consultant as (select cc.ClientConsultantId
				, cc.ClientId
				, cc.userId
				, u.EmailAddress
				, u.UserFullName
				, cc.UserRelationshipId
				, ur.Description
				, cc.CommissionPerc
				from ClientConsultants cc
				left join Users u on cc.UserId=u.UserId
				left join UserRelationships ur on ur.UserRelationshipId = cc.UserRelationshipId
)
, owners as (select ClientId
				, string_agg(EmailAddress, ',') within group (order by UserRelationshipId) as owners
				from consultant
				group by ClientId
			)
, consultant_info as (
        select ClientId
		, string_agg(
			concat_ws(' - '
				, nullif(UserFullName,'')
				, coalesce('[Commission(%): ' + convert(varchar(max),CommissionPerc, 1)+ ']',NULL))
			, ', ') as consultant_info
        from consultant
        group by ClientId
)
, canvassperiod as (select c.ClientID,lv.ValueName as canvassperiod
        from dbo.Clients c 
        left join dbo.ListValues lv  on c.CanvassPeriodValueId=lv.ListValueId
		)
, dup as (select c.ClientID,c.Company, ROW_NUMBER() OVER(PARTITION BY c.Company ORDER BY c.ClientID desc) AS rn 
        from dbo.Clients c)
, sector_all as (select so.SectorObjectId
		, so.SectorId
		, s.SectorName
		, so.ObjectId
		, so.Notes
		from SectorObjects so
		left join Sectors s on so.SectorId = s.SectorId
		--where so.SectorId = 50 --Old jobs
		)
, sector_share as (select ObjectId
		, string_agg(SectorName, ', ') as sector_share
		from sector_all
		group by ObjectId
		)
, attribute_filter as (select --distinct am.Description as FEUnique --23 rows
					a.AttributeMasterId
					, am.Description as FE
					, am.ParentAttributeMasterId
					, am.AllowClient
					, am.AllowContact
					, am.AllowApplicant
					, a.AttributeId
					, case when a.Notes is NULL or a.Notes = '' then a.Description
								else a.Notes end as SFE
					, a.Description --reference
				from Attributes a
				left join AttributeMaster am on am.AttributeMasterId = a.AttributeMasterId
				where am.Description not like '%DO NOT USE%'
				--and am.AllowApplicant = 'Y'
				--order by FE, SFE
)
, attribute_grade as (select oa.ObjectID
				, FE
				, concat_ws(' - ', a.SFE, nullif(g.Description,'')) as attribute
				from ObjectAttributes oa
				left join attribute_filter a on a.AttributeId = oa.AttributeId
				left join Grades g on g.GradeId = oa.Grade
				--where oa.ObjectId = 8666
)
, attribute_group as (select ObjectID, FE
		, string_agg(convert(nvarchar(max),nullif(attribute,'')), ', ') within group (order by attribute) as attribute_group
		from attribute_grade
		group by ObjectID, FE
)
, attribute_all as (select ObjectID
		, string_agg(convert(nvarchar(max),concat_ws(': ', nullif(FE, ''), nullif(attribute_group, ''))), char(10)) as attribute_all
		from attribute_group
		where FE is not NULL
		group by ObjectID
		)
--MAIN SCRIPT
select
        concat('NP', c.ClientID) as [company-externalId]
        , iif(dup.rn > 1, concat(dup.Company, ' - ', dup.rn), coalesce(c.Company, concat('No company name - ', c.ClientID))) as [company-name]
		, a.company_address as [company-locationName]
		, a.company_address as [company-locationAddress]
        , a.District as [company-locationDistrict]
        , a.City as [company-locationCity]
        , a.PostCode as [company-locationZipCode]
        , a.County as [company-locationState]
        , a.CountryCode as [company-locationCountry]
        --, tc.ABBREVIATION as [company-locationCountry]
		, f.fax_company as [company-fax]
		, sw.switchboard as [company-switchBoard]
        , ap.allotherphone as [company-phone]
        , nullif(w.website,'') as [company-website]
        , o.owners as [company-owners]
--INJECT
		, c.VatNo --Tax (GST/VAT/Sales) number
		, c.RegNo --Company / Business number
--NOTE
        , concat_ws(char(10)
                , coalesce('External ID: ' + convert(varchar(max),c.ClientID),NULL)
                , coalesce('Created By: ' + nullif(ui1.UserFullName,''),NULL)
                , coalesce('Created On: ' + convert(varchar(max),nullif(c.CreatedOn,''),120),NULL)
                --, coalesce('Updated By: ' + nullif(ui2.UserFullName,''),NULL)
                --, coalesce('Updated On: ' + convert(varchar(max),nullif(c.UpdatedOn,''),120),NULL)
				, coalesce('Our reference: ' + nullif(c.OurReference,''),NULL)
				--, coalesce('When establish: ' + nullif(cdc.Alpha1,''),NULL)
				--, coalesce('Holidays: ' + nullif(cdc.Alpha2,''),NULL)
				--, coalesce('Private Health: ' + nullif(cdc.Alpha3,''),NULL)
				--, coalesce('Pension: ' + nullif(cdc.Alpha4,''),NULL)
				--, coalesce('Life Assurance: ' + nullif(cdc.Alpha5,''),NULL)
				--, coalesce('Bonus: ' + nullif(cdc.Alpha6,''),NULL)
				--, coalesce('Staff Discount: ' + nullif(cdc.Alpha7,''),NULL)
				--, coalesce('Share Scheme: ' + nullif(cdc.Alpha8,''),NULL)
				--, coalesce('Profit Share: ' + nullif(cdc.Alpha9,''),NULL)
				--, coalesce('No. of Employe: ' + nullif(convert(varchar,cdc.Number1),''),NULL)
				--, coalesce('Car Allowance: ' + nullif(convert(varchar,cdc.Number2),''),NULL)
				--, coalesce('Profit Share: ' + nullif(convert(varchar,cdc.Number3),''),NULL)
				--, coalesce('Company Car: ' + nullif(cdc.Flag1,''), NULL)
				--, coalesce('Season Ticket : ' + nullif(cdc.Flag2,''), NULL)
				--, coalesce('Cost of Living: ' + nullif(cdc.Flag3,''), NULL)
				, coalesce('Company Location: ' + nullif(l.Description,''), NULL)
                , coalesce('Default Term Perc: ' + nullif(convert(varchar(max),c.DefaultTermPerc),''),NULL)
                --, coalesce('Company Status: ' + nullif(cs.Description,''),NULL)
                , coalesce('Company Source: ' + nullif(s.Description,''), NULL)
                --, coalesce('Canvass Period: ' + nullif(convert(varchar(max),cvp.canvassperiod),''),NULL)
				--, coalesce('Primary Email: ' + nullif(e.Email,''),NULL)
                --, coalesce('File As: ' + nullif(cio.FileAs,''),NULL)
                --, coalesce('Flag Text: ' + nullif(cio.FlagText,''),NULL)
				--, coalesce('Share this client: ' + nullif(ss.sector_share,''),NULL)
                --, coalesce('Office Email: ' +nullif(oe.officeemail,''),NULL)
                --, coalesce('Personal Email: ' +nullif(pe.personemail,''),NULL)
                --, coalesce('Mobile: ' +nullif(mo.mobilelist,''),NULL)
                --, coalesce('Company Phone: ' +nullif(cph.Phone,''),NULL)
                --, coalesce('Switch Board: ' +nullif(ps.Phone,''),NULL)
                --, coalesce('Phone Day: ' +nullif(pd.Phone,''),NULL)
                --, coalesce('Phone Evening: ' +nullif(pn.Phone,''),NULL)
                --, coalesce('Home Phone: ' +nullif(hp.Phone,''),NULL)
                --, coalesce('Office Phone: ' +nullif(pol.officephonelist,''),NULL)
                --, coalesce('URL: ' + coalesce(nullif(w.web,''),NULL)
                --, coalesce('Network: ' + nullif(n.link,''),NULL)
                --, coalesce('--Consultants Info--' + char(10) + nullif(ci.consultant_info,''),NULL)
				--, coalesce('Client Employment Details: ' +nullif(ced.Description,''),NULL) --no value
				--, coalesce('Attributes: ' + nullif(att.attribute_all,''),NULL)
                , coalesce(char(10) + '--Notes--' + char(10) + coalesce(nullif(c.Notes,''),'NONE'), NULL)
                --, coalesce(char(10) + '--Client Profile--' + char(10) + coalesce(nullif(cp.ProfileDocument,''),'NONE'),NULL)
        ) as [company-note]
from dbo.Clients c
left join dup on c.ClientID = dup.ClientID
left join userinfo ui1 on ui1.UserId=c.CreatedUserId
left join userinfo ui2 on ui2.UserId=c.UpdatedUserId
left join companyinIDobject cio on c.ClientID=cio.ObjectID
left join owners o on o.ClientID=c.ClientID
left join address_1 a on a.ObjectId = c.ClientID --primary address (latest address)
--left join tmp_country tc on tc.COUNTRY COLLATE Latin1_General_CI_AI = a.Country
left join clientstatus cs on cs.ClientStatusId=c.StatusId --status
left join ClientSectorDefinedColumns cdc on cdc.ClientID = c.ClientID --Custom columns
left join Locations l on l.LocationId = c.LocationId --locations
left join Sources s on s.SourceId = c.SourceId --source
--left join canvassperiod cvp on cvp.ClientId=c.ClientId
left join fax_company f on f.ObjectID = c.ClientID
left join switchboard sw on sw.ObjectID = c.ClientID
--left join sector_share ss on ss.ObjectID = c.ClientID
--left join consultant_info ci on ci.ClientId = c.ClientId
--left join ClientProfile cp on cp.ClientId = c.ClientId
--left join ClientEmploymentDetails ced on ced.ClientId = c.ClientId --no value
left join website w on w.ObjectId = c.ClientId
left join allotherphone ap on ap.ObjectId = c.ClientId
--left join email e on e.PhoneId = c.PrimaryEmailAddressPhoneId
--left join officeemail oe on oe.ObjectId = c.ClientId
--left join personemail pe on pe.ObjectId = c.ClientId
--left join network n on n.ObjectId = c.ClientId
--left join attribute_all att on att.ObjectId = c.ClientId --attribute group by entities
where c.ClientID not in (select ObjectId from SectorObjects where SectorId = 49) --Deleted Clients
--total 15619

UNION

select 'NP999999999','Default Company','','','','','','','','','','','','','','','This is Default Company from Data Import'