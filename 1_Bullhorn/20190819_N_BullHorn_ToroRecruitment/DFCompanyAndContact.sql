with company as (
        select clientCorporationID, name 
        from bullhorn1.BH_ClientCorporation CC 
        where CC.status <> 'Archive'
), contact as (
        select clientID,userID,clientCorporationID 
        from bullhorn1.BH_Client 
        where (isdeleted <> 1 and status <> 'Archive')
), companywithoutcontact as (
        select clientCorporationID, name 
        from company
        where clientCorporationID not in (select DISTINCT clientCorporationID from contact)
), titledup as (
        select j.jobPostingID,j.clientUserID,j.title,j.startDate,ROW_NUMBER() OVER(PARTITION BY j.clientUserID,j.title,CONVERT(VARCHAR(10),j.startDate,120) ORDER BY j.jobPostingID) AS rn
        from bullhorn1.BH_JobPosting j
), detailtakedfvalue as (
        select
                case 
                        when a.clientCorporationID in (select DISTINCT clientCorporationID from company) then CONCAT('TRCP',a.clientCorporationID)
                        else CONCAT('DEFAULT_TRCP',a.clientCorporationID)
                end ProjectCompany,
                CC.Name as CompanyName,   
                case 
                        when a.clientCorporationID in (select DISTINCT clientCorporationID from company) 
                                then
                                (
                                        case 
                                                when a.clientCorporationID = c.clientCorporationID then CONCAT('TRCT',a.clientUserID)
                                                else CONCAT('DEFAULT_TRCT',a.clientCorporationID)
                                        end
                                )
                        else 
                                (
                                        case 
                                                when a.clientCorporationID = c.clientCorporationID then CONCAT('TRCT',a.clientUserID)
                                                else CONCAT('DEFAULT_TRCT',a.clientCorporationID)
                                        end
                                )
                end 'positioncontactId',
                a.clientUserID,c.clientID,
                uc2.firstName,uc2.lastName
        from bullhorn1.BH_JobPosting a
        left join bullhorn1.BH_ClientCorporation CC on a.clientCorporationID=CC.clientCorporationID
        left join bullhorn1.BH_UserContact UC on a.userID = UC.userID
        left join bullhorn1.BH_Client cl on cl.userID=a.clientUserID
        left join contact c on c.userID=a.clientUserID
        left join bullhorn1.BH_UserContact UC2 on a.clientUserID = UC2.userID
        where (a.isdeleted <> 1 and a.status <> 'Archive')
), DefaultContact as (
        select DISTINCT positioncontactId,ProjectCompany
        from detailtakedfvalue
        where positioncontactId like 'DEFAULT_TRCT%'
), DefaultCompany as (
        select DISTINCT ProjectCompany,CompanyName
        from detailtakedfvalue
        where ProjectCompany like 'DEFAULT_TRCP%'
)
--select  ProjectCompany as 'company-externalId',
--       case when ROW_NUMBER() OVER(PARTITION BY CompanyName ORDER BY ProjectCompany) >1 then CONCAT(CompanyName, ' - ', ROW_NUMBER() OVER(PARTITION BY CompanyName ORDER BY ProjectCompany)) else CompanyName end 'company-name'
--from DefaultCompany;
select positioncontactId as 'contact-externalId','Default Contact' as 'contact-Lastname',ProjectCompany as 'contact-companyId'
        --case when ROW_NUMBER() OVER(PARTITION BY firstName,lastName ORDER BY ProjectCompany) >1 then CONCAT(lastName, ' - ', ROW_NUMBER() OVER(PARTITION BY firstName,lastName ORDER BY ProjectCompany)) else lastName end 'contact-Lastname'
from DefaultContact;
