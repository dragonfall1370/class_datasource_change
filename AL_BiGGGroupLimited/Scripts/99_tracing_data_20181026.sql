select
customText1, customText3, *
from bullhorn1.BH_ClientCorporation
where
--len(trim(isnull(convert(nvarchar(max), companyDescription), ''))) > 0
len(trim(isnull(convert(nvarchar(max), customText1), ''))) > 0
or len(trim(isnull(convert(nvarchar(max), customText3), ''))) > 0

select
--customTextBlock1
userID
, email
, email_old
, email2
, email3
, externalEmail
, isValidEmail
from bullhorn1.BH_UserContact uc
--left join bullhorn1.BH_Client c on uc.userID = c.userID
--where c.isPrimaryOwner = 1

DECLARE @json NVARCHAR(MAX)
SET @json=N'{"person":{"info":{"name":"John", "name":"Jack"}}}'

SELECT value
FROM OPENJSON(@json,'$.person.info') 