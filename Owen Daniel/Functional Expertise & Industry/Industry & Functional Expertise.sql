select * from tblBusinessArea
select * from tblJobCategory

------Company Industry
select ClientID, b.description from tblClient a left join tblBusinessArea b on a.BusAreaID = b.BusAreaID where a.BusAreaID is not null

------Job Industry
select ltrim(rtrim(VacancyRef)), b.description from tblVacancy a left join tblBusinessArea b on a.BusAreaID = b.BusAreaID where a.BusAreaID is not null
------Job Functional Expertise
select a.VacancyRef, b.Description from tblVacancy a left join tblJobCategory b on a.JobCatID = b.JobCatID where a.JobCatID is not null
