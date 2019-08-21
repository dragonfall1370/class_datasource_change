select * from bullhorn1.BH_CategoryList

select bl.name as CategoryName,vs.name as SpecialtyName from bullhorn1.BH_CategoryList bl
left join bullhorn1.View_Specialty vs on bl.categoryid = vs.parentcategoryid


select  * from bullhorn1.View_Specialty

with 
tmp (parentcategoryid,URLList) as (
SELECT parentcategoryid
	,STUFF((SELECT DISTINCT ', ' + name from bullhorn1.View_Specialty WHERE parentcategoryid = b.parentcategoryid FOR XML PATH ('')), 1, 2, '')  AS URLList 
	FROM bullhorn1.View_Specialty as b GROUP BY b.parentcategoryid )

select bl.*,
	t.parentcategoryid,
	replace(replace(t.URLList,'&amp;gt;',''),'&amp;','') 
 from tmp t
 left join bullhorn1.BH_BusinessSectorList bl on t.parentcategoryid = bl.businesssectorid
