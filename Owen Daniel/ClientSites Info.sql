with clientsite as (select SiteID, 
SiteName,
ClientID,
Town,
County,
PostalCode,
iif(MainSwitchboard is null,'',MainSwitchboard) as 'SwitchBoard',  
concat(nullif(concat(building, ', '),', '),
nullif(concat(Town, ', '),', '),
nullif(concat(County, ', '),', '),
PostalCode) as Address, 
iif(strNotes is null,'',strNotes) as 'Note', 
locationID as 'location-externalID',
row_number() over (partition by ClientID order by ClientID) as 'row_num'
from tblClientSites left join tblAddress on tblClientSites.AddressID =  tbladdress.id )

, clientnum as (select clientid from clientsite where row_num = 2)

select * from clientsite a left join clientnum b on a.ClientID = b.ClientID where a.ClientID in (b.ClientID) and SiteName <> ''
