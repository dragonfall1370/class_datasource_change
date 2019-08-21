select cl.cliID as 'company-externalId'
, cl.cliname as 'company-name'
, 'ZA' as 'company-locationCountry'
, u.udf1 as 'company-owners'
, cl.URL as 'company-website'
#, CC.companyDescription
, cl.Tel as 'company-phone'
, cl.Fax as 'company-fax'

, concat(cl.Address1
	,case when (cl.Address2 = '' OR cl.Address2 is NULL) THEN '' ELSE concat(', ',cl.Address2) END
	,case when (cl.Address3 = '' OR cl.Address3 is NULL) THEN '' ELSE concat(', ',cl.Address3) END
	,case when (cl.Address4 = '' OR cl.Address4 is NULL) THEN '' ELSE concat(', ',cl.Address4) END
	,case when (cl.Address5 = '' OR cl.Address5 is NULL) THEN '' ELSE concat(', ',cl.Address5) END
	,case when (cl.PostCode = '' OR cl.PostCode is NULL) THEN '' ELSE concat(', PostCode: ',cl.PostCode) END
	) as 'company-locationaddress'

, concat(cl.Address3,char(10),cl.Address5,char(10)) as 'company-locationName'
, cl.Address3 as 'company-locationCity'
, cl.Address5 as 'company-locationState'
, cl.PostCode as 'company-locationZipCode'
, replace(cl.Notes,'>','') as 'company-note'
# select cl.cliID, cl.cliname, cli.filename #count(*)
from emergingsearch.client cl
left join emergingsearch.users u ON cl.Owner = u.userid
left join (SELECT cliID, GROUP_CONCAT(distinct(ID),'.',replace(FileName,',',' ')) as filename FROM clifiles GROUP BY cliID) cli on cli.CliID = cl.cliID
where cl.cliname != '' and cl.cliname is not null
#order by cl.cliID


# select * from clifiles
select cl.cliID, cli.filename,length(cli.filename) as len_filename,'COMPANY' as entity_type, 'legal_document' as document_type  from emergingsearch.client cl left join (SELECT cliID, CONCAT(ID,'.',FileName) as filename FROM clifiles where FileName not like '%.msg') cli on cli.CliID = cl.cliID
where cli.filename is not null 
#cl.cliID = 11884932
#filename = '12.Cli11884932_AlisonRichardson.pdf'
#filename like '%.pdf'
