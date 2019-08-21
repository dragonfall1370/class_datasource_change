 CREATE TABLE Temp_CandidateCustomFields (
 candidateExternalId CHAR(60),
 additionalType char(60),
 formId int,
 fieldIdPublicationDate int,
 fieldIdEducation int,
 fieldIdCareerLevel int,
 insertTimeStamp DATETIME,
 opleidingsniveau_c char(60),
 EducationValue char(10),
 fieldDateValue date,
 carriere_niveau_c varchar(1000),
 CareerLevelValue char(10)
 )
 
insert into Temp_CandidateCustomFields 
select
	concat('BNS_',tc.id) as 'candidateExternalId',
    'add_cand_info' as 'additionalType',
    1005 as formId,
    1015 as fieldIdPublicationDate,
    1016 as fieldIdEducation,
    1017 as fieldIdCareerLevel,
    coalesce(tc.date_entered, now()) as 'insertTimestamp',
    cc.opleidingsniveau_c, 
    case when opleidingsniveau_c like '%Middelbare school%' then 4
		 when opleidingsniveau_c like '%Middelbareschool%' then 4
         when opleidingsniveau_c like '%VMBO%' then 6
         when opleidingsniveau_c like 'MBO' then 3
         when opleidingsniveau_c like '%HBO%' then 1
         when opleidingsniveau_c like '%Propedeuse%' then 5
         when opleidingsniveau_c like 'WO' then 7
         when opleidingsniveau_c like '%MBA / WO%' then 2
         when opleidingsniveau_c like '%MBAWO%' then 2
         else '' end as EducationValue,
		if(cc.her_publicatie_datum_c is null or her_publicatie_datum_c = '0000-00-00', null, her_publicatie_datum_c) as fieldDateValue, 
        cc.carriere_niveau_c,
	case when carriere_niveau_c like '%Scholier (Middelbare school)%' then 4
		 when carriere_niveau_c like '%scholier%' then 4
		 when carriere_niveau_c like '%Student%' then 7
		 when carriere_niveau_c like '%Startfunctie (weinig ervaring)%' then 6
         -- when carriere_niveau_c like '%Midcareer (ervaren)%' then 3
         when carriere_niveau_c like '%Ervaren%' then 3
         when carriere_niveau_c like '%ervaren%' then 3
		 when carriere_niveau_c like '%Manager/Leidinggevende%' then 2
         when carriere_niveau_c like '%Manager/Leidinggevende%' then 2
		 when carriere_niveau_c like '%Manager/Leidinggevende%' then 2
		 when carriere_niveau_c like '%Senior Management%' then 5
         when carriere_niveau_c like '%Directie%' then 1 
    else '' end as CareerLevelValue
from Temp_Candidates tc left join contacts_cstm cc on tc.id = cc.id_c

select count(*) from Temp_CandidateCustomFields

-- select distinct(opleidingsniveau_c)
-- from contacts_cstm 

-- select distinct(carriere_niveau_c)
-- from contacts_cstm order by ltrim(carriere_niveau_c)
 
-- select * from contacts_cstm where her_publicatie_datum_c = '' -- = '0000-00-00'