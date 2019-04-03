/*with docdup as (select ClntInfo.id as 'ClientID', ClientFiles.filename as 'filename' from ClntInfo left join ClientFiles on ClntInfo.id = ClientFiles.clientid),

docname as
(SELECT ClientID as 'ClientID',
    STUFF((SELECT DISTINCT concat(', ', filename)
           FROM docdup a 
           WHERE a.ClientID = b.ClientID 
          FOR XML PATH('')), 1, 2, '') as 'FileName'
FROM docdup b
GROUP BY ClientID)*/

with techenv as (select ClntInfo.id, longtextcache.chunk as 'description' from ClntInfo left join longtextcache on ClntInfo.techenvironment = longtextcache.id),

Companydup as (select a.id, a.name, ROW_NUMBER() over(partition by a.name
order by a.id asc) as 'dupcompany' from ClntInfo a),

company as (select
ClntInfo.id as 'company-externalId',
iif(Companydup.dupcompany= 1,companydup.name,concat(companydup.name,'-',companydup.dupcompany)) as 'company-name',
ClntInfo.addr1 + ', ' + Clntinfo.addr2 as 'company-locationAddress',
ClntInfo.addr1 + ', ' + Clntinfo.addr2 + ' SwitchBoard: ' + (iif(ClntInfo.phone='' or ClntInfo.phone is null,'',ClntInfo.phone))as 'company-locationName',
ClntInfo.Town as 'company-locationCity',
case when ClntInfo.Country = 'South Africa' then 'ZA'
when (ClntInfo.country = CTCode.country_name or ClntInfo.Country = CTCode.Country_Code)
then CTCode.Country_Code else '' end as 'company-locationCountry',
iif(ClntInfo.zip='' or ClntInfo.zip is null,'',ClntInfo.zip) as 'company-locationZipCode',
iif(ClntInfo.phone='' or ClntInfo.phone is null,'',ClntInfo.phone) as 'company-switchBoard',
iif(ClntInfo.fax='' or Clntinfo.fax is null, '' ,ClntInfo.fax) as 'company-fax',
ClntInfo.url as 'company-website',
iif(ClntInfo.consultantid = Vuser.id, Vuser.email,'') as 'company-owner',
concat('ACTIVE FLAG: ', (iif(ClntInfo.activeflag=1,'Active','Inactive')), (char(13)+char(10)), (char(13)+char(10)),
(iif(techenv.id = clntinfo.id,nullif(concat('TECHNICAL ENVIRONMENT: ',(char(13)+char(10)), techenv.description),concat('TECHNICAL ENVIRONMENT: ',(char(13)+char(10)))), '')),
(iif(ClntInfo.agencynotes = longtextcache.id,concat('AGENCY NOTE: ',(char(13)+char(10)), longtextcache.chunk),'')),(char(13)+char(10))

) as 'company-note',
row_number() over (partition by ClntInfo.id order by ClntInfo.id asc) as 'row_num'


from dbo.ClntInfo
left join longtextcache on ClntInfo.agencynotes = longtextcache.id
/*left join docname on ClntInfo.id = docname.FileName*/
left join techenv on ClntInfo.id = techenv.id
left join CTCode on clntinfo.country = CTCode.Country_Name
left join Companydup on clntinfo.id = Companydup.id
left join VUser on ClntInfo.consultantid = Vuser.id )

select * from company where row_num = 1