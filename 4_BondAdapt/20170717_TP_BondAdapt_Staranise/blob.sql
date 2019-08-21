-- select * from DOCUMENTS_TMP
-- SELECT * FROM staranise2.dbo.DOCUMENTS where DOC_ID = 1564925

select convert(DOCUMENT USING varchar(max)) FROM staranise2.dbo.DOCUMENTS where DOC_ID = 1564925

select DOCUMENT FROM staranise2.dbo.DOCUMENTS where DOC_ID = 1564925
select convert(varchar(max),convert(varbinary(max),DOCUMENT)) FROM staranise2.dbo.DOCUMENTS where DOC_ID = 1564925
select convert(varchar(max),convert(varbinary(max),DOCUMENT)) COLLATE SQL_Latin1_General_CP1_CI_AS FROM staranise2.dbo.DOCUMENTS where DOC_ID = 1564925
select convert(varchar(max),convert(varbinary(max),DOCUMENT)) COLLATE Latin1_General_CI_AS  FROM staranise2.dbo.DOCUMENTS where DOC_ID = 1564925
select convert(varchar(max),convert(varbinary(max),DOCUMENT)) COLLATE Latin1_General_BIN  FROM staranise2.dbo.DOCUMENTS where DOC_ID = 1564925
select convert(varchar(max),convert(varbinary(max),DOCUMENT)) COLLATE Latin1_General_100_CI_AS FROM staranise2.dbo.DOCUMENTS where DOC_ID = 1564925
select convert(VARCHAR(max),convert(varbinary(max),DOCUMENT)) COLLATE Arabic_CI_AS FROM staranise2.dbo.DOCUMENTS where DOC_ID = 1564925

select ASCII( convert(varchar(max),convert(varbinary(max),DOCUMENT)) )  FROM staranise2.dbo.DOCUMENTS where DOC_ID = 1564925
select cast(convert(nvarchar(max),convert(varbinary(max),DOCUMENT)) as varchar(max)) FROM staranise2.dbo.DOCUMENTS where DOC_ID = 1564925
select convert(nvarchar(max),convert(varbinary(max),DOCUMENT)) COLLATE SQL_Latin1_General_CP1_CI_AS  FROM staranise2.dbo.DOCUMENTS where DOC_ID = 1564925
select convert(varchar(max),convert(varbinary(max),DOCUMENT)) COLLATE Finnish_Swedish_CI_AS  FROM staranise2.dbo.DOCUMENTS where DOC_ID = 1564925


select convert(varchar(max),convert(varbinary(max),DOCUMENT)).StringValue.DefineEncoding(Encodings.Latin1) FROM staranise2.dbo.DOCUMENTS where DOC_ID = 1564925
select convert(varchar(max),convert(varbinary(max),DOCUMENT)) FROM staranise2.dbo.DOCUMENTS where DOC_ID = 1564925
select convert(varchar(max),DOCUMENT) FROM staranise2.dbo.DOCUMENTS where DOC_ID = 1564925

