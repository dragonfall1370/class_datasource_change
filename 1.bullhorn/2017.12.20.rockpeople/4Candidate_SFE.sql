
-- FE
with
  CateSplit(userid, categoryid) as (SELECT userid, Split.a.value('.','varchar(2000)') AS categoryID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(categoryIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') as Split(a) )
, CName(Userid, Name) as (SELECT Userid, CL.occupation from CateSplit left join bullhorn1.BH_CategoryList CL ON CateSplit.categoryid = CL.categoryID )
--select distinct Name from CName
, fe as (
                select
                 C.candidateID, concat(C.FirstName,' ',C.LastName) as fullname
                , case CN.Name
                        when 'BBOther' then 3111
                        when 'CEO / Managing Director' then 3112
                        when 'COO' then 3113
                        when 'Country Manager' then 3114
                        when 'Drill & Blast Engineer' then 3115
                        when 'Electrician' then 3116
                        when 'Exploration Geologist' then 3117
                        when 'Exploration Manager' then 3118
                        when 'General Manager (Operations)' then 3119
                        when 'General Manager - Exploration' then 3120
                        when 'Geology Manager / Chief Geologist' then 3121
                        when 'Geophysicist' then 3122
                        when 'Geotechnical Engineer' then 3123
                        when 'Hydrogeologist' then 3124
                        when 'Logistics Manager' then 3125
                        when 'Maintenance Manager' then 3126
                        when 'Management Accountant' then 3127
                        when 'Marketing Manager' then 3128
                        when 'Metallurgist' then 3129
                        when 'Mine Geologist' then 3130
                        when 'Mining Manager' then 3131
                        when 'Other' then 3132
                        when 'Planning Engineer (Mining Engineer)' then 3133
                        when 'Process Manager' then 3134
                        when 'Production Engineer (Mining Engineer)' then 3135
                        when 'Project Manager' then 3136
                        when 'Resource Development Manager' then 3137
                        when 'Resource Geologist' then 3138
                        when 'Safety Advisor' then 3139
                        when 'Senior Mining Engineer' then 3140
                        when 'Technical Services Manager' then 3141                
                        end as 'fe'
                from bullhorn1.Candidate C
                left join CName CN on C.userID = CN.userId
                where C.isPrimaryOwner = 1 and CN.userId is not null
)
--select count(*) from fe where fe is not null
select * from fe where fe is not null --and candidateID <100






-- SFE

with
-- SkillName: split by separate rows by comma, then combine them into SkillName
  SkillName0(userid, skillID) as (SELECT userid, Split.a.value('.', 'VARCHAR(2000)') AS skillID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(skillIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') AS Split(a) )
, SkillName(userId, SkillName) as (SELECT userId, SL.name from SkillName0 left join bullhorn1.BH_SkillList SL ON SkillName0.skillID = SL.skillID WHERE SkillName0.skillID <> '')
--select distinct SkillName from SkillName where SkillName in ('Alert','Analysis','Assets','Audit','Audits','Bank','Bar','Billing','BLASTING','bottomhole pressure','Boutique','Branding','Brokering','Budget Preparation','Budgets','Catering','cinema','Closures','Communications','COMPASS','Compliance','compliance monitoring','CONSULTANTS','Contract Negotiation','Contracts','core logging','Corrective Action','CULTURE','CYCLONE/R','Defence','Demand','Design','Dispatch','Documentation','Drinking','Due Diligence','Dynamics','e-commerce','Employment','Energy','Engineering Project Management','ENVIRONMENT','Environmental','Equipment','Excalibur Stripe Simulator','Execution','expectations','Facility','FAST','Feasibility Studies','Fire','Flagship','Food Safety','FUNCTION','GEOLOGY/GEOPHYSICS','geophysical interpretation','Geophysics','Geostatistics','Health and Safety','Hotel','Infrastructure','Key Performance Indicators','KPI','Leadership','Logistics','Management Plan','mine design','Minerals','Msds','Music','Performance','Performance Management','Policies','Procurement','Project Control','Project Development','RAILROADS','Recruiting','Recruitment','Refining','Regional Management','Reporting','rock mechanics','Root Balance','SAFETY ISSUES','Safety Studies','Scheduling','Scope of works','SCORE','SITE','Slope Stability','Stakeholder','Statistics','strategic assets','Supervising','SUPPLY','Supply & Demand','Targets','Traffic','TRAINING STAFF/USERS')
, sfe as (
                select
                 C.candidateID, concat(C.FirstName,' ',C.LastName) as fullname
                 , 3132 as 'fe'
                --, SN.SkillName
                , case sn.SkillName
                        when 'Alert' then 184
                        when 'Analysis' then 185
                        when 'Assets' then 186
                        when 'Audit' then 187
                        when 'Audits' then 188
                        when 'Bank' then 189
                        when 'Bar' then 190
                        when 'Billing' then 191
                        when 'BLASTING' then 192
                        when 'bottomhole pressure' then 193
                        when 'Boutique' then 194
                        when 'Branding' then 195
                        when 'Brokering' then 196
                        when 'Budget Preparation' then 197
                        when 'Budgets' then 198
                        when 'Catering' then 199
                        when 'cinema' then 200
                        when 'Closures' then 201
                        when 'Communications' then 202
                        when 'COMPASS' then 203
                        when 'Compliance' then 204
                        when 'compliance monitoring' then 205
                        when 'CONSULTANTS' then 206
                        when 'Contract Negotiation' then 207
                        when 'Contracts' then 208
                        when 'core logging' then 209
                        when 'Corrective Action' then 210
                        when 'CULTURE' then 211
                        when 'CYCLONE/R' then 212
                        when 'Defence' then 213
                        when 'Demand' then 214
                        when 'Design' then 215
                        when 'Dispatch' then 216
                        when 'Documentation' then 217
                        when 'Drinking' then 218
                        when 'Due Diligence' then 219
                        when 'Dynamics' then 220
                        when 'e-commerce' then 221
                        when 'Employment' then 222
                        when 'Energy' then 223
                        when 'Engineering Project Management' then 224
                        when 'ENVIRONMENT' then 225
                        when 'Environmental' then 226
                        when 'Equipment' then 227
                        when 'Excalibur Stripe Simulator' then 228
                        when 'Execution' then 229
                        when 'expectations' then 230
                        when 'Facility' then 231
                        when 'FAST' then 232
                        when 'Feasibility Studies' then 233
                        when 'Fire' then 234
                        when 'Flagship' then 235
                        when 'Food Safety' then 236
                        when 'FUNCTION' then 237
                        when 'GEOLOGY/GEOPHYSICS' then 238
                        when 'geophysical interpretation' then 239
                        when 'Geophysics' then 240
                        when 'Geostatistics' then 241
                        when 'Health and Safety' then 242
                        when 'Hotel' then 243
                        when 'Infrastructure' then 244
                        when 'Key Performance Indicators' then 245
                        when 'KPI' then 246
                        when 'Leadership' then 247
                        when 'Logistics' then 248
                        when 'Management Plan' then 249
                        when 'mine design' then 250
                        when 'Minerals' then 251
                        when 'Msds' then 252
                        when 'Music' then 253
                        when 'Performance' then 254
                        when 'Performance Management' then 255
                        when 'Policies' then 256
                        when 'Procurement' then 257
                        when 'Project Control' then 258
                        when 'Project Development' then 259
                        when 'RAILROADS' then 260
                        when 'Recruiting' then 261
                        when 'Recruitment' then 262
                        when 'Refining' then 263
                        when 'Regional Management' then 264
                        when 'Reporting' then 265
                        when 'rock mechanics' then 266
                        when 'Root Balance' then 267
                        when 'SAFETY ISSUES' then 268
                        when 'Safety Studies' then 269
                        when 'Scheduling' then 270
                        when 'Scope of works' then 271
                        when 'SCORE' then 272
                        when 'SITE' then 273
                        when 'Slope Stability' then 274
                        when 'Stakeholder' then 275
                        when 'Statistics' then 276
                        when 'strategic assets' then 277
                        when 'Supervising' then 278
                        when 'SUPPLY' then 279
                        when 'Supply & Demand' then 280
                        when 'Targets' then 281
                        when 'Traffic' then 282
                        when 'TRAINING STAFF/USERS' then 283                
                        end as 'sfe'
                from bullhorn1.Candidate C
                left join SkillName SN on C.userID = SN.userId
                where C.isPrimaryOwner = 1 and  SN.userId is not null
                and SN.SkillName in ('Alert','Analysis','Assets','Audit','Audits','Bank','Bar','Billing','BLASTING','bottomhole pressure','Boutique','Branding','Brokering','Budget Preparation','Budgets','Catering','cinema','Closures','Communications','COMPASS','Compliance','compliance monitoring','CONSULTANTS','Contract Negotiation','Contracts','core logging','Corrective Action','CULTURE','CYCLONE/R','Defence','Demand','Design','Dispatch','Documentation','Drinking','Due Diligence','Dynamics','e-commerce','Employment','Energy','Engineering Project Management','ENVIRONMENT','Environmental','Equipment','Excalibur Stripe Simulator','Execution','expectations','Facility','FAST','Feasibility Studies','Fire','Flagship','Food Safety','FUNCTION','GEOLOGY/GEOPHYSICS','geophysical interpretation','Geophysics','Geostatistics','Health and Safety','Hotel','Infrastructure','Key Performance Indicators','KPI','Leadership','Logistics','Management Plan','mine design','Minerals','Msds','Music','Performance','Performance Management','Policies','Procurement','Project Control','Project Development','RAILROADS','Recruiting','Recruitment','Refining','Regional Management','Reporting','rock mechanics','Root Balance','SAFETY ISSUES','Safety Studies','Scheduling','Scope of works','SCORE','SITE','Slope Stability','Stakeholder','Statistics','strategic assets','Supervising','SUPPLY','Supply & Demand','Targets','Traffic','TRAINING STAFF/USERS')
)
select count(*) from sfe where sfe is not null 
select * from sfe where sfe is not null --and candidateID <100
--select count(distinct ltrim(SkillName)) as Skill from SkillName --where SkillName
--select distinct ltrim(SkillName) as Skill from SkillName --where SkillName
-- select * from bullhorn1.BH_SkillList SL where name in ('Product Mgmt & Marketing','Customer/Data Analytics','Cash Ops','Investment/Portfolio Mgmt','Investment research and analysis','Credit admin/ops','Card Ops','HR Analytics','Compensation','Benefits','L&D','Robotic process automation (RPA)','AI & machine learning','JD Edwards ERP','Avaloq','Cognos','Hyperion','Bloomberg','Reuters','Matlab','Labview','Pro E+','SAS','Qlikview','Tableau','R Programming','SPSS','Mobile app developer','Assistant Manager','Senior Manager','Local','Startup','Social Insights & Analytics','Art Creative Director','Copy Art Director','Integrated Marketing','Digital media','Customer/Data Analytics','Risk & Compliance','Advisory/Sales','Investment/Portfolio Mgmt','Project Mgmt/Transformation','Client Service/Call Centre','Capex or Opex category sourcing','Chemical sourcing','Consumables category','Electrical category','Electronic component category','EMS category','Flavour category','Frangrance category','IT category sourcing','Logistic category sourcing','Marketing category sourcing','Mechanical category','NPI category sourcing','Oil & gas sourcing','Professional category sourcing','Project category sourcing','Supplier mgmt','Raw material sourcing','Reverse auction','Distribution','Media research')
