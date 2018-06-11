with temp as (select Main_Site_Unique
			, ltrim(Stuff(
			  Coalesce(' ' + NULLIF(Key_Word_Codes_001, ''), '')
			+ Coalesce(';' + NULLIF(Key_Word_Codes_002, ''), '')
			+ Coalesce('; ' + NULLIF(Key_Word_Codes_003, ''), '')
			+ Coalesce('; ' + NULLIF(Key_Word_Codes_004, ''), '')
			+ Coalesce('; ' + NULLIF(Key_Word_Codes_005, ''), '')
			+ Coalesce('; ' + NULLIF(Key_Word_Codes_006, ''), '')
			+ Coalesce('; ' + NULLIF(Key_Word_Codes_007, ''), '')
			+ Coalesce('; ' + NULLIF(Key_Word_Codes_008, ''), '')
			+ Coalesce('; ' + NULLIF(Key_Word_Codes_009, ''), '')
			+ Coalesce('; ' + NULLIF(Key_Word_Codes_010, ''), '')
			, 1, 1, '')) as Industry
from clients1)

, compIndustries as (select Main_Site_Unique, Industry, ltrim(rtrim(value)) as FinalIndustry
from temp 
cross apply string_split(Industry,';')
where Industry is not null)
select concat('GP',Main_Site_Unique) as ex_id, FinalIndustry industry
from compIndustries

--select distinct FinalIndustry industry, current_timestamp as insert_timestamp
--from compIndustries

--select * from Clients1