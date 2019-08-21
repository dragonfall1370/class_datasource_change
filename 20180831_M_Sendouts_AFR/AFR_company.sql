---DUPLICATION REGCONITION
with dup as (SELECT Company_Number, Company_Name, ROW_NUMBER() OVER(PARTITION BY Company_Name ORDER BY Company_Number ASC) AS rn
FROM Company) --no duplicate companies

---MAIN SCRIPT
select concat('AFR',c.Company_Number) as 'company-externalId'
, c.Company_Name as 'company-name'
, c.Owned_By_Person_Number
, u.Email as 'company-owners'
, concat_ws(char(10),concat('Company External ID - CO#: ', c.Company_Number)
	, iif(c.Created_DTTM is NULL,NULL,concat('Entered date: ', convert(varchar(20),c.Created_DTTM,120)))
	, iif(c.Comment is NULL,NULL,concat('Comment: ', c.Comment))
	) as 'company-note'
from Company c
left join AFR_User u on c.Owned_By_Person_Number = u.UserID
order by c.Company_Number


--->>> DEFAULT CONTACT PLACEHOLDER <<<---
select concat('AFR',Company_Number) as 'contact-companyId'
, concat('AFR999',Company_Number) as 'contact-externalId'
, 'DEFAULT CONTACT' as 'contact-lastName'
, Company_Name as 'contact-firstName'
, 'This is default contact for the company' as 'contact-note'
from Company
order by Company_Number