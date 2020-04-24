

-- Reference Summary
-- select referenceTitle,* from bullhorn1.BH_UserReference where referenceTitle is not null;
with ReferenceSummary(userId, reference) as (
       SELECT r.userId
       , STRING_AGG(
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( 
                            stuff(
                                   + Coalesce('Candidate Title: ' + NULLIF(convert(nvarchar(max),r.candidateTitle), '') + char(10), '')
                                   + Coalesce('Client Corporation: ' + NULLIF(convert(nvarchar(max),r.clientCorporationID), '') + char(10), '')
                                   + Coalesce('Company: ' + NULLIF(convert(nvarchar(max),r.companyName), '') + char(10), '')
                                   + Coalesce('Date Added: ' + NULLIF(convert(nvarchar(max),r.dateAdded), '') + char(10), '')
                                   + Coalesce('Employment End: ' + NULLIF(convert(nvarchar(max),r.employmentEnd), '') + char(10), '')
                                   + Coalesce('Employment Start: ' + NULLIF(convert(nvarchar(max),r.employmentStart), '') + char(10), '')
                                   + Coalesce('Job Posting: ' + NULLIF(convert(nvarchar(max),a.title), '') + char(10), '')
                                   + Coalesce('Reference Email: ' + NULLIF(convert(nvarchar(max),r.referenceEmail), '') + char(10), '')
                                   + Coalesce('Reference First Name: ' + NULLIF(convert(nvarchar(max),r.referenceFirstName), '') + char(10), '')
                                   + Coalesce('Reference Last Name: ' + NULLIF(convert(nvarchar(max),r.referenceLastName), '') + char(10), '')
                                   + Coalesce('Reference Phone: ' + NULLIF(convert(nvarchar(max),r.referencePhone), '') + char(10), '')
                                   + Coalesce('Reference Title: ' + NULLIF(convert(nvarchar(max),r.referenceTitle), '') + char(10), '')
                                   + Coalesce('Reference: ' + NULLIF(convert(nvarchar(max),r.referenceUserID), '') + char(10), '')
                                   + Coalesce('Status: ' + NULLIF(convert(nvarchar(max),r.status), '') + char(10), '')
                                   + Coalesce('Reference ID: ' + NULLIF(convert(nvarchar(max),r.userReferenceID), '') + char(10), '')
                                   + Coalesce('Years Known: ' + NULLIF(convert(nvarchar(max),r.yearsKnown), '') + char(10), '')
                            , 1, 0, '')
                     ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                     ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                     ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                     ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                     ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                     ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'') --as es
       ,char(10) ) WITHIN GROUP (ORDER BY r.dateadded) reference
       FROM bullhorn1.BH_UserReference r 
       left join bullhorn1.BH_JobPosting a on a.jobPostingID = r.jobPostingID
       GROUP BY r.userId        
       )
--select top 10 * from referenceSummary where reference like '%Job Posting%' --userid in (163454);
--select distinct jobPostingID FROM bullhorn1.BH_UserReference r 


-- NEWEST EDUCATION
-- select * from bullhorn1.BH_UserEducation 
, EducationGroup as (select userID, max(userEducationID) as userEducationID from bullhorn1.BH_UserEducation group by userID)
, Education as (
       select EG.userID
              , UE.certification
              , UE.city
              , UE.comments
              --, UE.customText1
              , UE.dateAdded
              , UE.degree
              , UE.endDate
              , UE.expirationDate
              , UE.gpa
              , convert(varchar(10),UE.graduationDate,110) as graduationDate
              , UE.major
              , UE.school
              , UE.startDate
              , UE.state
              , UE.userEducationID       
       from EducationGroup EG left join bullhorn1.BH_UserEducation UE on EG.userEducationID = UE.userEducationID)
-- Education Summary
, EducationSummary(userId, es) as (
       SELECT e.userId
       , STRING_AGG(
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( 
                            stuff(
                                     coalesce('Date Added: ' + nullif(cast(e.dateAdded as nvarchar(max)), '') + char(10), '')                   
                                   + coalesce('Certification: ' + nullif(cast(e.certification as nvarchar(max)), '') + char(10), '')
                                   + coalesce('City: ' + nullif(cast(e.city as nvarchar(max)), '') + char(10), '')
                                   + coalesce('Comments: ' + nullif(cast(e.comments as nvarchar(max)), '') + char(10), '')
                                   --+ coalesce('Country: ' + nullif(cast(e.customText1 as nvarchar(max)), '') + char(10), '')
                                   + coalesce('Degree: ' + nullif(cast(e.degree as nvarchar(max)), '') + char(10), '')
                                   + coalesce('End Date: ' + nullif(cast(e.endDate as nvarchar(max)), '') + char(10), '')
                                   + coalesce('Expiration Date: ' + nullif(cast(e.expirationDate as nvarchar(max)), '') + char(10), '')
                                   + coalesce('GPA: ' + nullif(cast(e.gpa as nvarchar(max)), '') + char(10), '')
                                   + coalesce('Graduation Date: ' + nullif(cast(e.graduationDate as nvarchar(max)), '') + char(10), '')
                                   + coalesce('Major: ' + nullif(cast(e.major as nvarchar(max)), '') + char(10), '')
                                   + coalesce('School: ' + nullif(cast(e.school as nvarchar(max)), '') + char(10), '')
                                   + coalesce('Start Date: ' + nullif(cast(e.startDate as nvarchar(max)), '') + char(10), '')
                                   + coalesce('State: ' + nullif(cast(e.state as nvarchar(max)), '') + char(10), '')
                                   --+ coalesce('Education ID: ' + nullif(cast(e.userEducationID as nvarchar(max)), '') + char(10), '')
                            , 1, 0, '')
                     ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                     ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                     ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                     ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                     ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                     ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'') --as es
       ,char(10) ) WITHIN GROUP (ORDER BY dateadded) es
       FROM bullhorn1.BH_UserEducation e
       GROUP BY e.userId        
       )
 --select top 10 * from EducationSummary where userid in (163454);
-- select * from bullhorn1.BH_UserEducation where customText1 is not null


-- select * from bullhorn1.BH_UserCertification where licenseNumber is not null;


-- Employment History -- select *  from bullhorn1.BH_userWorkHistory
, EmploymentHistory(userId, eh) as (
       SELECT userId
         , STRING_AGG(
--       , STUFF(( 
--                     select char(10) + 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( 
                            stuff(
                              coalesce('Bonus: ' + nullif(cast(bonus as nvarchar(max)), '') + char(10), '')
                            + coalesce('Client Corporation: ' + nullif(cast(clientCorporationID as nvarchar(max)), '') + char(10), '')
                            + coalesce('Comments: ' + nullif(cast(comments as nvarchar(max)), '') + char(10), '')
                            + coalesce('Commission: ' + nullif(cast(commission as nvarchar(max)), '') + char(10), '')
                            + coalesce('Company Name: ' + nullif(cast(companyName as nvarchar(max)), '') + char(10), '')
                            + coalesce('Date Added: ' + nullif(cast(dateAdded as nvarchar(max)), '') + char(10), '')
                            + coalesce('End Date: ' + nullif(cast(endDate as nvarchar(max)), '') + char(10), '')
                            + coalesce('Job Posting: ' + nullif(cast(title as nvarchar(max)), '') + char(10), '') --jobPostingID
                            --+ coalesce('Placement: ' + nullif(cast(placementID as nvarchar(max)), '') + char(10), '')
                            + coalesce('Salary Low: ' + nullif(cast(salary1 as nvarchar(max)), '') + char(10), '')
                            + coalesce('Salary High: ' + nullif(cast(salary2 as nvarchar(max)), '') + char(10), '')
                            + coalesce('Salary Type: ' + nullif(cast(salaryType as nvarchar(max)), '') + char(10), '')
                            + coalesce('Start Date: ' + nullif(cast(startDate as nvarchar(max)), '') + char(10), '')
                            + coalesce('Termination Reason: ' + nullif(cast(terminationReason as nvarchar(max)), '') + char(10), '')
                            + coalesce('Title: ' + nullif(cast(title as nvarchar(max)), '') + char(10), '')
                            --+ coalesce('User Work History ID: ' + nullif(cast(userWorkHistoryID as nvarchar(max)), '') + char(10), '')
                                   --+ coalesce('Comments: ' + nullif(replace([dbo].[udf_StripHTML](comments),'Â ',''), '') + char(10), '')
                                   --+ coalesce('Comments: ' + nullif(replace([dbo].[fn_ConvertHTMLToText](comments),'Â ',''), '') + char(10), '')
                            , 1, 0, '') 
                     ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                     ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                     ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                     ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                     ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                     ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'') --as eh
--                     from bullhorn1.BH_userWorkHistory
--       WHERE userId = a.userId order by startDate desc
--       FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS es 
--       FROM bullhorn1.BH_userWorkHistory as a
--       left join bullhorn1.BH_JobPosting j on j.jobPostingID = a.jobPostingID
--       --where userid in (164043)
--       GROUP BY a.userId 
       ,char(10) ) WITHIN GROUP (ORDER BY startDate desc) eh
       FROM bullhorn1.BH_userWorkHistory GROUP BY userId
       )
--select top 10 * from EmploymentHistory where userid in (164043);


select --top 10
         C.candidateID as 'candidate-externalId'
        , Stuff( coalesce(' ' + nullif(es.es, '') + char(10) , '') + coalesce(char(10) + 'Reference: ' + char(10) + nullif(rs.Reference, '') + char(10), ''), 1, 1, '') as 'candidate-workPhone'
from bullhorn1.Candidate C --where C.isPrimaryOwner = 1
left join EducationSummary es on es.userID = C.userID
left join ReferenceSummary rs on rs.userid = C.userid
where C.isdeleted <> 1 and C.status <> 'Archive'
--and rs.userid is not null