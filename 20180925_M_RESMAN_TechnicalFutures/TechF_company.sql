---DUPLICATION REGCONITION
with dup as (SELECT ClientID, ClientName, ROW_NUMBER() OVER(PARTITION BY ClientName ORDER BY ClientID ASC) AS rn
	FROM ClientMaster) --no duplicate companies

/* ADVANCED NAMES FOR COMPANY DOCUMENTS

--COMPANY DOCUMENTS (waiting for real files from TECHFUTURES)
, Documents as (select ClientID
	, concat(right(left(FileServerRoute,len(FileServerRoute)-len('\Cache0\')),charindex('\',reverse(left(FileServerRoute,len(FileServerRoute)-len('\Cache0\'))))-1),'_',right(FileServerLocation,CHARINDEX('\',reverse(FileServerLocation))-1)) as NewFile
	, right(left(FileServerRoute,len(FileServerRoute)-len('\Cache0\')),charindex('\',reverse(left(FileServerRoute,len(FileServerRoute)-len('\Cache0\'))))-1) as Prefix
	, right(FileServerLocation,CHARINDEX('\',reverse(FileServerLocation))-1) as SuffixFile
	, FileServerLocation
	from ClientDocs)
*/

--COMPANY DOCUMENTS
, DocumentsRow as (select distinct ClientID
	, right(FileServerLocation,CHARINDEX('\',reverse(FileServerLocation))-1) as NewFile
	, row_number() over(partition by right(FileServerLocation,CHARINDEX('\',reverse(FileServerLocation))-1) order by ClientID desc) as  rn
	from ClientDocs
	where lower(right(FileServerLocation,CHARINDEX('.',reverse(FileServerLocation)))) in ('.pdf','.doc','.docx','.xls','.xlsx','.rtf','.msg','.txt','.htm','.html'))

--RENAME DOCUMENTS WITH ROW NUMBERS
, Documents as (select clientID
	, case when rn > 1 then concat(left(NewFile,CHARINDEX('.',NewFile)-1),'_',rn-1,right(NewFile,CHARINDEX('.',reverse(NewFile))))
		else NewFile end as NewFile
	from DocumentsRow)

, CompDocuments as (select ClientID, string_agg(NewFile,',') as CompDocuments
	from Documents
	group by ClientID)

--MAIN SCRIPT
select concat('TF',cm.ClientID) as 'company-externalId'
	, case when dup.rn > 1 then concat(dup.ClientName,' - ',dup.rn)
		else dup.ClientName end as 'company-name'
	, concat_ws(' ',nullif(cm.WorkPhoneArea,''),nullif(cm.WorkPhone,'')) as 'company-switchBoard'
	, concat_ws(' ',nullif(cm.FaxArea,''),nullif(cm.Fax,'')) as 'company-fax'
	, left(cm.WebAddress,100) as 'company-website'
	, u.Email as 'company-owners'
	, concat_ws(', ', nullif(cm.StreetAddress,'')
		, nullif(nullif(cm.StreetCity,''),'NA')
		, nullif(nullif(cm.StreetSuburb,''),'NA')
		, nullif(nullif(cm.StreetPostCode,''),'NA')
		, nullif(nullif(cm.StreetCountry,''),'NA')) as 'company-locationName'
	, concat_ws(', ', nullif(cm.StreetAddress,'')
		, nullif(nullif(cm.StreetCity,''),'NA')
		, nullif(nullif(cm.StreetSuburb,''),'NA')
		, nullif(nullif(cm.StreetPostCode,''),'NA')
		, nullif(nullif(cm.StreetCountry,''),'NA')) as 'company-locationAddress'
	, nullif(cm.StreetCity,'NA') as 'company-locationCity'
	, nullif(cm.StreetSuburb,'NA') as 'company-locationDistrict'
	, nullif(cm.StreetPostCode,'NA') as 'company-locationZipCode'
	, case
		when cm.StreetCountry = 'NEW ZEALAND' then 'NZ'
		when cm.StreetCountry = 'AUSTRALIA' then 'AU'
		when cm.StreetCountry = 'IRELAND' then 'IE'
		else NULL end as 'company-locationCountry'
	, d.CompDocuments as 'company-documents'
	, concat_ws(char(10),'Company external ID: ',cm.ClientID
		, coalesce('Email: ' + nullif(cm.Email,''),'')
		, coalesce('Industry: ' + nullif(cm.Industry,''),'')
		, coalesce('Comments: ' + nullif(cm.Comments,''),'')
		) as 'company-note'
	from ClientMaster cm
	left join dup on dup.ClientID = cm.ClientID
	left join CompDocuments d on d.ClientID = cm.ClientID
	left join Users u on u.ConsultantID = cm.ConsultantID

	UNION ALL

	select 'TF9999999','Default company','','','','','','','','','','','','This is default contact from Data import'