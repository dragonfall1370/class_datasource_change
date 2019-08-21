drop table if exists VC_ComWebAdrFix

select
CLNT_ID as ComId
, dbo.ufn_RefineWebAddress(
	case(WEB_ADDR)
		when 'N/A' then ''
		when '0' then ''
		when 'x' then ''
		when 'xx' then ''
		when 'xxxx' then ''
		else trim( '/.,- ' from WEB_ADDR)
	end
) as webAdr
into VC_ComWebAdrFix

from CLNTINFO_DATA_TABLE

select * from VC_ComWebAdrFix
--where ComId = 30160
where webAdr like 'x%'

--update VC_ComWebAdrFix
--set webAdr = ''
----where webAdr = 'x'
--where ComId in (79238,
--70875)