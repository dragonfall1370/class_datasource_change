with
------------businessSector---------------
 category (categoryID,name) as (select categoryID,name from bullhorn1.BH_Category)
, tmp_businessSectorIDList1 (userid, BussinessID) as
(SELECT userid,LTRIM(RTRIM(m.n.value('.[1]','varchar(8000)'))) AS BussinessID 
FROM (SELECT userid, CAST('<XMLRoot><RowData>' + REPLACE(cast(businessSectorIDList as varchar(max)),',','</RowData><RowData>') + '</RowData></XMLRoot>' AS XML) AS x FROM  bullhorn1.BH_UserContact CA where CA.status not like '%Archive%' and CA.status <> '' )t
CROSS APPLY x.nodes('/XMLRoot/RowData')m(n)
)
--select cast(BussinessID as int) from tmp_businessSectorIDList1

, tmp_businessSectorIDList2(userid, bussinesid) as (select distinct userid, cast(BussinessID as varchar(max)) from (select userid, BussinessID from tmp_businessSectorIDList1) a)
--select * from tmp_businessSectorIDList2
--, t1(userId, IndustryName) as (select tmp_1.userid, BS.name from tmp_1 inner join bullhorn1.BH_BusinessSector BS ON tmp_1.bussinesid = BS.businessSectorID)
--, t1(userid, name, bussinesid) as (select tmp_businessSectorIDList2.userid, BS.name, tmp_businessSectorIDList2.bussinesid from tmp_businessSectorIDList2 inner join bullhorn1.BH_BusinessSector BS ON tmp_businessSectorIDList2.bussinesid = BS.businessSectorID)
--, t1(userid,name,bussinesid) as (select tmp_businessSectorIDList2.userid, BS.name, tmp_businessSectorIDList2.bussinesid from tmp_businessSectorIDList2 inner join bullhorn1.BH_BusinessSector BS ON tmp_businessSectorIDList2.bussinesid = BS.businessSectorID)
--select * from t1 order by userid
, t(userid, name) as (select tmp_businessSectorIDList2.userid, cast(BS.name as varchar(max)) from tmp_businessSectorIDList2 inner join bullhorn1.BH_BusinessSector BS ON tmp_businessSectorIDList2.bussinesid = BS.businessSectorID)
--select * from t where userid = 3144 --order by userid 

, tmp_businessSectorIDList3(userid, name) as (SELECT userid, name = 
    STUFF((SELECT DISTINCT ', ' + name
           FROM t b 
           WHERE b.userid = a.userid 
          FOR XML PATH('')), 1, 2, '')
FROM t a
GROUP BY userid
)
--select * from tmp_businessSectorIDList3 where userid = 893
--select * from tmp_businessSectorIDList3 order by userid

----------------------------
, note as (select 
ca.candidateid 
--, case when (ca.comments = '' OR ca.comments is NULL) THEN '' ELSE concat('Notes: ',ca.comments,char(10)) END  as 'comments'
, case when (ISNULL(REPLACE(cast(ca.comments as nvarchar(max)),CHAR(13),''), '') = '') THEN '' ELSE concat('Notes: ',ca.comments) END as 'comments'

--, case when (ca.employeeType = '' OR ca.employeeType is NULL) THEN '' ELSE concat('Employment Type / Job Type: ',ca.employeeType,char(10)) END as 'employeeType'
, case when (cast(tmp_businessSectorIDList3.name as varchar(max)) = '' OR tmp_businessSectorIDList3.name is NULL) THEN '' ELSE REPLACE(REPLACE(concat('Industry: ',tmp_businessSectorIDList3.name,char(10)), '&amp;', '&'), 'amp;', '') END as 'Industry'
, case when (t1.name = '' OR t1.name is NULL) THEN '' ELSE concat('Functional Expertise: ',t1.name,char(10)) END as 'categoryname'
--, case when (uc.specialtyIDList = '' OR uc.specialtyIDList is NULL) THEN '' ELSE concat('Sub functional expertise: ',uc.specialtyIDList,char(10)) END as 'uc.specialtyIDList'
, case when (ISNULL(REPLACE(cast(uc.specialtyIDList as nvarchar(max)),CHAR(13),''), '') = '') THEN '' ELSE concat('Sub functional expertise: ',uc.specialtyIDList) END as 'specialtyIDList'

--, case when (uc.skillIDList = '' OR uc.skillIDList is NULL) THEN '' ELSE concat('Skills: ',uc.skillIDList,char(10)) END as 'uc.skillIDList'
--, case when (ISNULL(REPLACE(cast(uc.skillIDList as nvarchar(max)),CHAR(13),''), '') = '') THEN '' ELSE concat('Skills: ',uc.skillIDList) END as 'skillIDList'

, case when (uc.employmentPreference = '' OR uc.employmentPreference is NULL) THEN '' ELSE concat('Desired Industry: ',uc.employmentPreference,char(10)) END as 'employmentPreference'

--, case when (uc.customTextBlock1 = '' OR uc.customTextBlock1 is NULL) THEN '' ELSE concat('Skills: ',uc.customTextBlock1,char(10)) END as 'uc.customTextBlock1'
, case when (ISNULL(REPLACE(cast(uc.customTextBlock1 as nvarchar(max)),CHAR(13),''), '') = '') THEN '' ELSE concat('Skills: ',uc.customTextBlock1) END as 'customTextBlock1'

--, case when (uc.skillSet = '' OR uc.skillSet is NULL) THEN '' ELSE concat('Skills: ',uc.skillSet,char(10)) END as 'uc.skillSet'
, case when (ISNULL(REPLACE(cast(uc.skillSet as nvarchar(max)),CHAR(13),''), '') = '') THEN '' ELSE concat('Skills: ',uc.skillSet) END as 'skillSet'

, case when (uc.customText4 = '' OR uc.customText4 is NULL) THEN '' ELSE concat('Candidate picker: ',uc.customText4,char(10)) END as 'customText4'
, case when (uc.customtext1 = '' OR uc.customtext1 is NULL) THEN '' ELSE concat('Customtext1: ',uc.customtext1,char(10)) END as 'customtext1'
, case when (uc.fax = '' OR uc.fax is NULL) THEN '' ELSE concat('Skype ID: ',uc.fax,char(10)) END as 'fax'
, case when (uc.customtext11 = '' OR uc.customtext11 is NULL) THEN '' ELSE concat('We ve Met: ',uc.customtext11,char(10)) END as 'customtext11'
, case when (ca.type = '' OR ca.type is NULL) THEN '' ELSE concat('Representation Commitment: ',ca.type,char(10)) END as 'type'
, case when (ca.status = '' OR ca.status is NULL) THEN '' ELSE concat('Active/Passive and general VC Pipeline: ',ca.status,char(10)) END as 'status'
, case when (ca.candidateSourceID = '' OR ca.candidateSourceID is NULL) THEN '' ELSE concat('Subjective Assessment: ',ca.candidateSourceID,char(10)) END as 'candidateSourceID'
, case when (uc.workAuthorized = '' OR uc.workAuthorized is NULL) THEN '' ELSE concat('Valid work permit: ',uc.workAuthorized,char(10)) END as 'workAuthorized'
, case when (uc.customtext10 = '' OR uc.customtext10 is NULL) THEN '' ELSE concat('Will relocate: ',uc.customtext10,char(10)) END as 'customtext10'
, case when (ISNULL(REPLACE(cast(uc.desiredLocations as nvarchar(max)),CHAR(13),''), '') = '') THEN '' ELSE concat('Work Aspirations Desired locations: ',uc.desiredLocations) END as 'desiredLocations'
, case when (uc.hourlyRateLow = '' OR uc.hourlyRateLow is NULL) THEN '' ELSE concat('Contract rate: ',uc.hourlyRateLow,char(10)) END as 'hourlyRateLow'
, case when (uc.hourlyRate = '' OR uc.hourlyRate is NULL) THEN '' ELSE concat('Desired contract rate: ',uc.hourlyRate,char(10)) END as 'hourlyRate'
--, case when (uc.description = '' OR uc.description is NULL) THEN '' ELSE concat('Original CV: ',uc.description,char(10)) END as 'uc.description'
, case when (ISNULL(REPLACE(cast(uc.description as nvarchar(max)),CHAR(13),''), '') = '') THEN '' ELSE concat('Original CV: ',uc.description) END as 'description'
, case when (uc.customtext2 = '' OR uc.customtext2 is NULL) THEN '' ELSE concat('School name: ',uc.customtext2,char(10)) END as 'customtext2'
, case when (uc.referredByUserID = '' OR uc.referredByUserID is NULL) THEN '' ELSE concat('Referral Source: ',uc.referredByUserID,char(10)) END as 'referredByUserID'
	from bullhorn1.BH_Candidate ca
	left join bullhorn1.BH_UserContact UC on ca.recruiterUserID = uc.userID
	left join tmp_businessSectorIDList3 ON uc.userid = tmp_businessSectorIDList3.userid
	left join category t1 ON UC.categoryID = t1.categoryID
  )

--select * from note
--select candidateid,count(*) from notes group by candidateid having count(*) > 1
, notes as (select candidateid, concat(comments,Industry,categoryname,specialtyIDList,employmentPreference,customTextBlock1,skillSet,customText4,customtext1,fax,customtext11,type,status,candidateSourceID,workAuthorized,customtext10,desiredLocations,hourlyRateLow,hourlyRate,description,customtext2,referredByUserID) as 'candidate-note' from note)


select * from notes
--select jobPostingID, count(*) from tmp_6 group by jobPostingID having count(*) > 1
