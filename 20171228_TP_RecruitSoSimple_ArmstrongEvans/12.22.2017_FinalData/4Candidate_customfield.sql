
with cf as (
        select
          c.candidate_externalId as 'candidate_externalId'
        , c.candidate_title as 'candidate_title'
        , concat(c.candidate_firstName,' ', c.candidate_Lastname) as 'fullname'
        , case convert(varchar,c.Industry)
                when 'Backup Power - CHP' then 28892
                when 'Backup Power - CHP,Backup Powe' then 28893
                when 'Backup Power - Generators' then 28894
                when 'Backup Power - Generators,Back' then 28895
                when 'Backup Power - Switchgear' then 28896
                when 'Backup Power - UPS' then 28897
                when 'Building Services - Fire & Sec' then 28898
                when 'Building Services - HVAC' then 28899
                when 'Building Services - Lifts, Doo' then 28900
                when 'Building Services - M&E' then 28901
                when 'Building Services - Maintenanc' then 28902
                when 'Building Services - Telecoms' then 28903
                when 'Incomplete' then 28904
                when 'Marine' then 28889
                when 'Plant - Compressed Air' then 28905
                when 'Plant - Cranes' then 28906
                when 'Plant - Forklift' then 28907
                when 'Plant - HGV' then 28908
                when 'Plant - Machinery' then 28909
                when 'Plant - Powered Access' then 28910
                when 'Plant - Pumps' then 28911
                when 'Renewable - Biomass' then 28912
                when 'Renewable - Solar' then 28913
                when 'Renewable - Wind' then 28914
                end as industry
        , case convert(varchar,c.functionalexpertise)
                when 'Engineering' then 3097
                when 'Design' then 2986
                when 'Design Engineer' then 2986
                when 'Design,Engineering' then 2987
                when 'Executive' then 2988
                when 'Operational' then 2989
                when 'Opertional' then 2989
                when 'Sales' then 2990
                when 'Support' then 2991
                end as 'functionalexpertise'
        , case convert(varchar,c.subfunctionalexpertise) 
                when 'Account Manager' then 227
                when 'Administration' then 226
                when 'Adminstration' then 225
                when 'Admisitration' then 224
                when 'Applications Engineer' then 199
                when 'Applications Engineer,CAD Tech' then 198
                when 'Applications Engineer,Electric' then 200
                when 'Applications Engineer,Electron' then 201
                when 'Applications Engineer,Mechanic' then 202
                when 'CAD Technician' then 197
                when 'Commissiong Engineer' then 203
                when 'Commissioning Engineer' then 204
                when 'Consultant' then 221
                when 'Contract Manager' then 220
                when 'Controls Engineer' then 205
                when 'Coordinator' then 219
                when 'Coordinator/Controller' then 218
                when 'Customer Service' then 234
                when 'Design Engineer' then 193
                when 'Electrical Design Engineer' then 196
                when 'Field Service Engineer' then 206
                when 'General/Operations Manager' then 217
                when 'H&S Manager' then 216
                when 'Managing Director' then 215
                when 'Mechanical Design Engineer' then 195
                when 'Mechanical Design Engineer,Fie' then 194
                when 'Operator' then 223
                when 'Production Engineer' then 207
                when 'Production Manager' then 208
                when 'Project Engineer' then 209
                when 'Project Manager' then 222
                when 'Quotations Engineer' then 231
                when 'Sales Director' then 214
                when 'Sales Director,Sales Manager' then 213
                when 'Sales Engineer' then 230
                when 'Sales Executive' then 229
                when 'Sales Manager' then 228
                when 'Service Manager' then 233
                when 'Service Sales Manager' then 232
                when 'Technical Manager' then 210
                when 'Workshop/Depot Engineer' then 211
                when 'Workshop/Depot Manager' then 212
                end as 'subfunctionalexpertise'
        , case c.source
                when 'CV Library' then 29096
                when 'Facebook' then 29097
                when 'Google+' then 29098
                when 'Indeed' then 29085
                when 'Indeed' then 29099
                when 'Jobsite' then 29100
                when 'LinkedIn' then 29090
                when 'Monster' then 29101
                when 'Newsletter' then 29102
                when 'Other' then 29103
                when 'Reed' then 29104
                when 'Referral' then 29092
                when 'Search Engine' then 29105
                when 'Total' then 29106
                when 'Twitter' then 29107
                when 'Web Registration' then 29108
                end as 'Source'
        , case c.Status 
                when 'Active' then 1
                when 'Actively Looking' then 2
                when 'Archived' then 3
                when 'Placed Internally' then 4
                end as 'Status'
	-- select count (*) --12791
	-- select distinct convert(varchar,c.Industry)
	-- select distinct convert(varchar,c.functionalexpertise)  
	-- select distinct convert(varchar,c.subfunctionalexpertise) 
	-- select distinct convert(varchar,c.source)
	-- select distinct convert(varchar,c.status)
	from CandidatesImportAutomappingTemplate c
)
--select candidate_externalId,  source from cf where source is not null
--select candidate_externalId,  industry from cf where industry is not null
/*SELECT
         candidate_externalId as additional_id , fullname 
        , 'add_cand_info' as additional_type
        , 1006 as form_id
        , 1017 as field_id
        , convert(varchar,Status) as field_value
from cf where Status is not null */
select candidate_externalId, functionalexpertise,subfunctionalexpertise from cf where functionalexpertise is not null
--select candidate_externalId, subfunctionalexpertise from cf where subfunctionalexpertise is not null