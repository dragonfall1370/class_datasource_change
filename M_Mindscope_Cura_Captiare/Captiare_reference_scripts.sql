Draft query for Captiare

--DUPLICATION RECOGNITION
with dup as (SELECT ClientID, ClientName, ROW_NUMBER() OVER(PARTITION BY ClientName ORDER BY ClientID ASC) AS rn 
FROM Client where Deleted = 0)

--COMPANY ADDRESSES > Some companies have more than 1 address


--COMPANY PHONE > Some companies have more than 1 phones - Default phone can be found from "Client" table


--MAIN SCRIPT
select
concat('MSC',C.ClientID) as 'company-externalId' --> MSC is abbreviation for Mindscope Cura
, C.ClientName as '(OriginalCompanyName)'
, iif(C.ClientID in (select ClientID from dup where dup.rn > 1)
	, iif(dup.ClientName = '' or dup.ClientName is NULL,concat('Client Name -',dup.ClientID),concat(dup.ClientName,'-DUPLICATE-',dup.ClientID))
	, iif(C.ClientName = '' or C.ClientName is null,concat('Client Name -',dup.ClientID),C.ClientName)) as 'company-name'
from Client C
where C.Deleted = 0

select * from Client where ClientID = 7 -- DefaultPhoneID = 14127

select * from Note where NoteTypeID = 700 or NoteTypeID = 701

select * from Note where NoteID = 49696

select * from Note where NoteTypeID = 210 or NoteTypeID = 220

select * from NoteType where NoteTypeID = 210 or NoteTypeID = 220

select * from Client_Address where AddressID = 8543 | 9579 - 10955 (clientID = 7)

select * from Address where AddressID in (9579, 10955)
--where AddressID = 8543

select * from AddressType

select * from City

select * from ProvinceState

select * from Country

select * from Phone where PhoneID in (14127, 16487)

select * from Client_Phone where ClientID = 7

select * from PhoneType

select * from IndustryType

select * from Office

select ClientID, count(ClientID) from Client_Address group by ClientID having count(ClientID) > 1