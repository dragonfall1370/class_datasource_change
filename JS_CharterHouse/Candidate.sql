with 
document1 as (select table1,code,reverse(left(REVERSE(filename), charindex('\', REVERSE(filename)) - 1)) as filename from attachment)

,document2 as (select code,string_agg(cast(trim(filename) as varchar(max)),',') as filename from document1 where table1 = 'CN' group by code)
,test as (
select 
       a.code as 'candidate-externalId',
       iif(title in ('MR','MRS','MS','MISS','DR'),upper(replace(title,'.','')),'') as 'candidate-title',
       iif(forename is null or forename = '','No First Name',forename) as 'candidate-firstname',
       iif(surname = '' or surname is null,'No Last Name',surname) as 'candidate-lastname',
       middlename as 'candidate-middlename',
       salutation as 'preferred-name',
       address as 'candidate-address',
       postcode as 'candidate-postcode',
       cast(telhome as nvarchar(max)) as 'candidate-homePhone',
       cast(telwork as nvarchar(max)) as 'candidate-workPhone',
       dbo.udf_GetNumeric(telother) as 'candidate-phone',
       email as 'candidate-email',
       dob as 'candidate-dob',
       url as 'candidate-linkedin',
       iif(b.filename is null or b.filename ='','',b.filename) as 'candidate-document',
       
       Concat( 'External ID: ',a.code,
              nullif(concat('Salutation: ',salutation,(char(13)+char(10))),concat('Salutation: ',(char(13)+char(10)))),
              'Status: ',
              case 
              when status = 1 then 'Working for us'
              when status = 2 then 'Interview Pending'
              when status = 3 then 'Interested Position'
              when status = 4 then 'Active Looking'
              when status = 5 then 'Considering Opportunities'
              when status = 6 then 'On Contract'
              when status = 7 then 'Found Own Job'
              when status = 8 then 'Not Looking'
              when status = 9 then 'Other'
              when status = 10 then 'Placed by us'
              end,
              (char(13)+char(10)),
              nullif(concat('Employer: ',lastempl,(char(13)+char(10))),concat('Employer: ',(char(13)+char(10)))),
              nullif(concat('Job: ',lastpost,(char(13)+char(10))), concat('Job: ',(char(13)+char(10)))),
              nullif(concat('Salary: ',lastsal,(char(13)+char(10))),concat('Salary: ',(char(13)+char(10))))
       )as 'candidate-note',
       
       ROW_NUMBER() over (partition by email order by email) as rn
       --owningcons as 'candidate-owners'
from candidate a
left join document2 b on a.code = b.code )

select --top 123
       iif([candidate-email] is null or [candidate-email] ='','',iif(rn=1,[candidate-email],concat(rn,'-',[candidate-email]))) as email
       ,* 
from test





-- SOURCE
select a.*, b.source from candsource a
left join dbo.sources b on a.sourceid = b.sourceid
where a.sourceid <> 0


-- OWNER
with t as ( 
       select
              a.code as 'candidate-externalId', o.email
              --distinct a.owningcons as 'candidate-owners', o.email
       -- select distinct a.owningcons
       from candidate a
       left join owners o on o.initials = a.owningcons
       where a.owningcons is not null
)
select top 123
       [candidate-externalId]
       , case
              when email = 'christ@charterhouse.com.hk' then '[{"ownerId":"28985","primary":"true"}]'
              when email = '20170216_082644.983_annabella.poon@hrboss.com' then '[{"ownerId":"28950","primary":"true"}]'
              when email = 'vivianl@charterhouse.com.hk' then '[{"ownerId":"28966","primary":"true"}]'
              when email = 'tassaneea@charterhouse.com.hk' then '[{"ownerId":"28967","primary":"true"}]'
              when email = 'richardn@charterhouse.com.hk' then '[{"ownerId":"28968","primary":"true"}]'
              when email = 'devik@charterhouse.com.hk' then '[{"ownerId":"28969","primary":"true"}]'
              when email = 'bharatim@charterhouse.com.hk' then '[{"ownerId":"28970","primary":"true"}]'
              when email = 'katet@charterhouse.com.hk' then '[{"ownerId":"28971","primary":"true"}]'
              when email = 'enquiries@charterhouse.com.hk' then '[{"ownerId":"28972","primary":"true"}]'
              when email = 'raymondc@charterhouse.com.hk' then '[{"ownerId":"28973","primary":"true"}]'
              when email = 'katrinaw@charterhouse.com.hk' then '[{"ownerId":"28974","primary":"true"}]'
              when email = 'katiel@charterhouse.com.hk' then '[{"ownerId":"28975","primary":"true"}]'
              when email = 'daryl.lee@charterhouse.com.hk' then '[{"ownerId":"28976","primary":"true"}]'
              when email = 'aileeny@charterhouse.com.hk' then '[{"ownerId":"28977","primary":"true"}]'
              when email = 'thomasf@charterhouse.com.hk' then '[{"ownerId":"28978","primary":"true"}]'
              when email = 'nicky.dhillon@charterhouse.com.hk' then '[{"ownerId":"28979","primary":"true"}]'
              when email = 'lindar@charterhouse.com.hk' then '[{"ownerId":"28980","primary":"true"}]'
              when email = 'traceyb@charterhouse.com.hk' then '[{"ownerId":"28981","primary":"true"}]'
              when email = 'andrewm@charterhouse.com.hk' then '[{"ownerId":"28982","primary":"true"}]'
              when email = 'alexc@charterhouse.com.hk' then '[{"ownerId":"28983","primary":"true"}]'
              when email = 'catherinem@charterhouse.com.hk' then '[{"ownerId":"28986","primary":"true"}]'
              when email = 'dilalr@charterhouse.com.hk' then '[{"ownerId":"28987","primary":"true"}]'
              when email = 'jameso@charterhouse.com.hk' then '[{"ownerId":"28988","primary":"true"}]'
              when email = 'arunh@charterhouse.com.hk' then '[{"ownerId":"28989","primary":"true"}]'
              when email = '20170120_085032.208_eloise.sutton@vincere.io' then '[{"ownerId":"28946","primary":"true"}]'
              when email = '20160317_111733.874_nam.nguyen@hrboss.com' then '[{"ownerId":"28942","primary":"true"}]'
              when email = '20170518_074136.656_vu.nguyen@hrboss.com' then '[{"ownerId":"28951","primary":"true"}]'
              when email = '20170523_090651.413_testing1@gmail.com' then '[{"ownerId":"28952","primary":"true"}]'
              when email = '20160317_111742.070_pooja.gulati@hrboss.com' then '[{"ownerId":"28944","primary":"true"}]'
              when email = '20170110_115857.362_linhdory.nguyen@vincere.io' then '[{"ownerId":"28948","primary":"true"}]'
              when email = '20170120_085031.130_annabella.poon@vinceredev.com' then '[{"ownerId":"28945","primary":"true"}]'
              when email = '20170120_085032.626_samuel.collier@vincere.io' then '[{"ownerId":"28947","primary":"true"}]'
              when email = '20160202_063600.611_nam.nguyen@hrboss.com' then '[{"ownerId":"28940","primary":"true"}]'
              when email = '20160317_111737.469_nigel.gardiner@hrboss.com' then '[{"ownerId":"28941","primary":"true"}]'
              when email = '20160317_111746.061_vu.nguyen@hrboss.com' then '[{"ownerId":"28943","primary":"true"}]'
              when email = '20160317_112143.618_bernie.schiemer@hrboss.com' then '[{"ownerId":"28939","primary":"true"}]'
              when email = '20170120_085032.419_linhdory.nguyen@vincere.io' then '[{"ownerId":"28949","primary":"true"}]'
              when email = 'pollyn@charterhouse.com.hk' then '[{"ownerId":"28956","primary":"true"}]'
              when email = 'ruths@charterhouse.com.hk' then '[{"ownerId":"28957","primary":"true"}]'
              when email = 'alexandres@charterhouse.com.hk' then '[{"ownerId":"28954","primary":"true"}]'
              when email = 'stevew@charterhouse.com.hk' then '[{"ownerId":"28955","primary":"true"}]'
              when email = 'antoinet@charterhouse.com.hk' then '[{"ownerId":"28958","primary":"true"}]'
              when email = 'keirb@charterhouse.com.hk' then '[{"ownerId":"28959","primary":"true"}]'
              when email = 'stevenp@charterhouse.com.hk' then '[{"ownerId":"28960","primary":"true"}]'
              when email = 'madeleines@charterhouse.com.hk' then '[{"ownerId":"28961","primary":"true"}]'
              when email = 'guye@charterhouse.com.hk' then '[{"ownerId":"28962","primary":"true"}]'
              when email = 'maggie.yu@charterhouse.com.hk' then '[{"ownerId":"28963","primary":"true"}]'
              when email = 'angelal@charterhouse.com.hk' then '[{"ownerId":"28964","primary":"true"}]'
              when email = 'yifan.yan@charterhouse.com.hk' then '[{"ownerId":"28965","primary":"true"}]'
              when email = 'arvinderp@charterhouse.com.hk' then '[{"ownerId":"28990","primary":"true"}]'
              when email = 'paulg@charterhouse.com.hk' then '[{"ownerId":"28992","primary":"true"}]'
              when email = 'darrenc@charterhouse.com.hk' then '[{"ownerId":"28993","primary":"true"}]'
              when email = 'simonl@charterhouse.com.hk' then '[{"ownerId":"28994","primary":"true"}]'
              when email = 'roccol@charterhouse.com.hk' then '[{"ownerId":"28996","primary":"true"}]'
              when email = 'gracew@charterhouse.com.hk' then '[{"ownerId":"28998","primary":"true"}]'
              when email = 'mattheww@charterhouse.com.hk' then '[{"ownerId":"28999","primary":"true"}]'
              when email = 'josephinec@charterhouse.com.hk' then '[{"ownerId":"29002","primary":"true"}]'
              when email = 'justinc@charterhouse.com.hk' then '[{"ownerId":"29004","primary":"true"}]'
              when email = 'zackm@charterhouse.com.hk' then '[{"ownerId":"29006","primary":"true"}]'
              when email = 'celial@charterhouse.com.hk' then '[{"ownerId":"29007","primary":"true"}]'
              when email = 'thomasl@charterhouse.com.hk' then '[{"ownerId":"29008","primary":"true"}]'
              when email = 'clairep@charterhouse.com.hk' then '[{"ownerId":"29011","primary":"true"}]'
              when email = 'catrina.mok@charterhouse.com.hk' then '[{"ownerId":"29017","primary":"true"}]'
              when email = 'philipq@charterhouse.com.hk' then '[{"ownerId":"29019","primary":"true"}]'
              when email = 'neha.bhardwaj@charterhouse.com.hk' then '[{"ownerId":"29023","primary":"true"}]'
              when email = 'anap@charterhouse.com.hk' then '[{"ownerId":"29024","primary":"true"}]'
              when email = 'shirleyf@charterhouse.com.hk' then '[{"ownerId":"29027","primary":"true"}]'
              when email = 'davids@charterhouse.com.hk' then '[{"ownerId":"29029","primary":"true"}]'
              when email = 'robertf@charterhouse.com.hk' then '[{"ownerId":"29030","primary":"true"}]'
              when email = 'rubyw@charterhouse.com.hk' then '[{"ownerId":"29032","primary":"true"}]'
              when email = 'michelley@charterhouse.com.hk' then '[{"ownerId":"29033","primary":"true"}]'
              when email = 'emilyb@charterhouse.com.hk' then '[{"ownerId":"29036","primary":"true"}]'
              when email = 'erica.wong@charterhouse.com.hk' then '[{"ownerId":"29037","primary":"true"}]'
              when email = 'richardm@charterhouse.com.hk' then '[{"ownerId":"29038","primary":"true"}]'
              when email = 'cifec@charterhouse.com.hk' then '[{"ownerId":"29039","primary":"true"}]'
              when email = 'petrac@charterhouse.com.hk' then '[{"ownerId":"28991","primary":"true"}]'
              when email = 'elizabethy@charterhouse.com.hk' then '[{"ownerId":"28995","primary":"true"}]'
              when email = 'kevinh@charterhouse.com.hk' then '[{"ownerId":"28997","primary":"true"}]'
              when email = 'support@core3.com.au' then '[{"ownerId":"29000","primary":"true"}]'
              when email = 'robinj@charterhouse.com.hk' then '[{"ownerId":"29001","primary":"true"}]'
              when email = 'jackiel@charterhouse.com.hk' then '[{"ownerId":"29003","primary":"true"}]'
              when email = 'stefanies@charterhouse.com.hk' then '[{"ownerId":"29005","primary":"true"}]'
              when email = 'sashas@charterhouse.com.hk' then '[{"ownerId":"29009","primary":"true"}]'
              when email = 'katiew@charterhouse.com.hk' then '[{"ownerId":"29010","primary":"true"}]'
              when email = 'sundarm@charterhouse.com.hk' then '[{"ownerId":"29012","primary":"true"}]'
              when email = 'josephinea@charterhouse.com.hk' then '[{"ownerId":"29013","primary":"true"}]'
              when email = 'moirac@charterhouse.com.hk' then '[{"ownerId":"29014","primary":"true"}]'
              when email = 'edmondc@charterhouse.com.hk' then '[{"ownerId":"29015","primary":"true"}]'
              when email = 'shelley.siu@charterhouse.com.hk' then '[{"ownerId":"29016","primary":"true"}]'
              when email = 'davidc@charterhouse.com.hk' then '[{"ownerId":"29018","primary":"true"}]'
              when email = 'adriang@charterhouse.com.hk' then '[{"ownerId":"29020","primary":"true"}]'
              when email = 'mandyl@charterhouse.com.hk' then '[{"ownerId":"29021","primary":"true"}]'
              when email = 'lucyp@charterhouse.com.hk' then '[{"ownerId":"29022","primary":"true"}]'
              when email = 'reinac@charterhouse.com.hk' then '[{"ownerId":"29025","primary":"true"}]'
              when email = 'emmaw@charterhouse.com.hk' then '[{"ownerId":"29026","primary":"true"}]'
              when email = 'emmal@charterhouse.com.hk' then '[{"ownerId":"29028","primary":"true"}]'
              when email = 'ronaldh@charterhouse.com.hk' then '[{"ownerId":"29031","primary":"true"}]'
              when email = 'silviat@charterhouse.com.hk' then '[{"ownerId":"29034","primary":"true"}]'
              when email = 'mandyc@charterhouse.com.hk' then '[{"ownerId":"29035","primary":"true"}]'
              when email = 'brentm@charterhouse.com.hk' then '[{"ownerId":"29040","primary":"true"}]'
              when email = 'fleurd@charterhouse.com.hk' then '[{"ownerId":"29041","primary":"true"}]'
              when email = 'lasanthar@charterhouse.com.hk' then '[{"ownerId":"29042","primary":"true"}]'
              when email = 'glenn@charterhouse.com.hk' then '[{"ownerId":"29044","primary":"true"}]'
              when email = 'veronicac@charterhouse.com.hk' then '[{"ownerId":"29046","primary":"true"}]'
              when email = 'nathank@charterhouse.com.hk' then '[{"ownerId":"29047","primary":"true"}]'
              when email = 'danvisl@charterhouse.com.hk' then '[{"ownerId":"29049","primary":"true"}]'
              when email = 'pattyl@charterhouse.com.hk' then '[{"ownerId":"29053","primary":"true"}]'
              when email = 'kellyy@charterhouse.com.hk' then '[{"ownerId":"29057","primary":"true"}]'
              when email = 'phoebel@charterhouse.com.hk' then '[{"ownerId":"29058","primary":"true"}]'
              when email = 'jennyt@charterhouse.com.hk' then '[{"ownerId":"29061","primary":"true"}]'
              when email = 'daniell@charterhouse.com.hk' then '[{"ownerId":"29063","primary":"true"}]'
              when email = 'amyl@charterhouse.com.hk' then '[{"ownerId":"29064","primary":"true"}]'
              when email = 'canif@charterhouse.com.hk' then '[{"ownerId":"29066","primary":"true"}]'
              when email = 'emilyh@charterhouse.com.hk' then '[{"ownerId":"29067","primary":"true"}]'
              when email = 'nestor.roldan@charterhouse.com.hk' then '[{"ownerId":"29068","primary":"true"}]'
              when email = 'willc@charterhouse.com.hk' then '[{"ownerId":"29071","primary":"true"}]'
              when email = 'bobby.lee@charterhouse.com.hk' then '[{"ownerId":"29072","primary":"true"}]'
              when email = 'christinec@charterhouse.com.hk' then '[{"ownerId":"29075","primary":"true"}]'
              when email = 'akinan@charterhouse.com.hk' then '[{"ownerId":"29076","primary":"true"}]'
              when email = 'michelle.chan@charterhouse.com.hk' then '[{"ownerId":"29077","primary":"true"}]'
              when email = 'katerina.tse@charterhouse.com.hk' then '[{"ownerId":"29081","primary":"true"}]'
              when email = 'clareb@charterhouse.com.hk' then '[{"ownerId":"29083","primary":"true"}]'
              when email = 'amyf@charterhouse.com.hk' then '[{"ownerId":"29085","primary":"true"}]'
              when email = 'evonk@charterhouse.com.hk' then '[{"ownerId":"29088","primary":"true"}]'
              when email = 'charterhousehk@charterhouse.com.hk' then '[{"ownerId":"29089","primary":"true"}]'
              when email = 'angel.yue@charterhouse.com.hk' then '[{"ownerId":"29092","primary":"true"}]'
              when email = 'fannyc@charterhouse.com.hk' then '[{"ownerId":"29093","primary":"true"}]'
              when email = 'emilyy@charterhouse.com.hk' then '[{"ownerId":"29043","primary":"true"}]'
              when email = 'philph@charterhouse.com.hk' then '[{"ownerId":"29045","primary":"true"}]'
              when email = 'junel@charterhouse.com.hk' then '[{"ownerId":"29048","primary":"true"}]'
              when email = 'catherinab@charterhouse.com.hk' then '[{"ownerId":"29050","primary":"true"}]'
              when email = 'joew@charterhouse.com.hk' then '[{"ownerId":"29051","primary":"true"}]'
              when email = 'sonnyp@charterhouse.com.hk' then '[{"ownerId":"29052","primary":"true"}]'
              when email = 'jacklinel@charterhouse.com.hk' then '[{"ownerId":"29054","primary":"true"}]'
              when email = 'elliep@charterhouse.com.hk' then '[{"ownerId":"29055","primary":"true"}]'
              when email = 'marco.lau@charterhouse.com.hk' then '[{"ownerId":"29056","primary":"true"}]'
              when email = 'williamy@charterhouse.com.hk' then '[{"ownerId":"29059","primary":"true"}]'
              when email = 'stephaniec@charterhouse.com.hk' then '[{"ownerId":"29060","primary":"true"}]'
              when email = 'kevinp@charterhouse.com.hk' then '[{"ownerId":"29062","primary":"true"}]'
              when email = 'joem@charterhouse.com.hk' then '[{"ownerId":"29065","primary":"true"}]'
              when email = 'andy.pong@charterhouse.com.hk' then '[{"ownerId":"29069","primary":"true"}]'
              when email = 'wing.au@charterhouse.com.hk' then '[{"ownerId":"29070","primary":"true"}]'
              when email = 'julia.wan@charterhouse.com.hk' then '[{"ownerId":"29073","primary":"true"}]'
              when email = 'Kenny.lee@charterhouse.com.hk' then '[{"ownerId":"29078","primary":"true"}]'
              when email = 'Joey.lai@charterhouse.com.hk' then '[{"ownerId":"29079","primary":"true"}]'
              when email = 'Tiffany.lai@charterhouse.com.hk' then '[{"ownerId":"29080","primary":"true"}]'
              when email = 'Garfield.yau@charterhouse.com.hk' then '[{"ownerId":"29082","primary":"true"}]'
              when email = 'rafaelw@charterhouse.com.hk' then '[{"ownerId":"29084","primary":"true"}]'
              when email = 'markc@charterhouse.com.hk' then '[{"ownerId":"29086","primary":"true"}]'
              when email = 'Karen.dancel@charterhouse.com.hk' then '[{"ownerId":"29087","primary":"true"}]'
              when email = 'marky@charterhouse.com.hk' then '[{"ownerId":"29090","primary":"true"}]'
              when email = 'michellel@charterhouse.com.hk' then '[{"ownerId":"29091","primary":"true"}]'
              when email = 'vickiel@charterhouse.com.hk' then '[{"ownerId":"29094","primary":"true"}]'
              when email = 'matthewd@charterhouse.com.hk' then '[{"ownerId":"29095","primary":"true"}]'
              when email = 'adac@charterhouse.com.hk' then '[{"ownerId":"29096","primary":"true"}]'
              when email = 'corrinad@charterhouse.com.hk' then '[{"ownerId":"29097","primary":"true"}]'
              when email = 'khimh@charterhouse.com.hk' then '[{"ownerId":"29098","primary":"true"}]'
              when email = 'mayap@charterhouse.com.hk' then '[{"ownerId":"29099","primary":"true"}]'
              when email = 'chrisw@charterhouse.com.hk' then '[{"ownerId":"29100","primary":"true"}]'
              when email = 'markb@charterhouse.com.hk' then '[{"ownerId":"29101","primary":"true"}]'
              when email = 'cindyw@charterhouse.com.hk' then '[{"ownerId":"29102","primary":"true"}]'
              when email = 'vanitas@charterhouse.com.hk' then '[{"ownerId":"29103","primary":"true"}]'
              when email = 'travis.leung@charterhouse.com.hk' then '[{"ownerId":"29104","primary":"true"}]'
              when email = 'scarlett.zhu@charterhouse.com.hk' then '[{"ownerId":"29105","primary":"true"}]'
              when email = 'sabrinah@charterhouse.com.hk' then '[{"ownerId":"29106","primary":"true"}]'
              when email = 'karens@charterhouse.com.hk' then '[{"ownerId":"29107","primary":"true"}]'
              when email = 'chasem@charterhouse.com.hk' then '[{"ownerId":"29108","primary":"true"}]'
              when email = 'aalokl@charterhouse.com.hk' then '[{"ownerId":"29109","primary":"true"}]'
              when email = 'regann@charterhouse.com.hk' then '[{"ownerId":"29110","primary":"true"}]'
              when email = 'ceceliay@charterhouse.com.hk' then '[{"ownerId":"29117","primary":"true"}]'
              when email = 'sysadmin@vincere.io' then '[{"ownerId":"-10","primary":"true"}]'
              when email = 'cherryt@charterhouse.com.hk' then '[{"ownerId":"29111","primary":"true"}]'
              when email = 'charterhouseprod@veryrealemail.com' then '[{"ownerId":"28953","primary":"true"}]'
              when email = 'enquiry@charterhouse.com.sg' then '[{"ownerId":"29112","primary":"true"}]'
              when email = 'maryw@charterhouse.com.hk' then '[{"ownerId":"29113","primary":"true"}]'
              when email = 'luke.phibbs@charterhouse.com.hk' then '[{"ownerId":"29074","primary":"true"}]'
              when email = 'noellec@charterhouse.com.hk' then '[{"ownerId":"29114","primary":"true"}]'
              when email = 'kalinann@charterhouse.com.hk' then '[{"ownerId":"29115","primary":"true"}]'
              when email = 'winky.cheung@charterhouse.com.hk' then '[{"ownerId":"28984","primary":"true"}]'
              when email = 'neonc@charterhouse.com.hk' then '[{"ownerId":"29118","primary":"true"}]'
              when email = 'winniek@charterhouse.com.hk' then '[{"ownerId":"29116","primary":"true"}]'
              when email = 'dollies@charterhouse.com.hk' then '[{"ownerId":"29119","primary":"true"}]'
              when email = 'roshanp@charterhouse.com.hk' then '[{"ownerId":"29120","primary":"true"}]'
              when email = 'mandyli@charterhouse.com.hk' then '[{"ownerId":"29121","primary":"true"}]'
              when email = 'patricial@charterhouse.com.hk' then '[{"ownerId":"29122","primary":"true"}]'
              when email = 'frontline@charterhouse.com.hk' then '[{"ownerId":"29123","primary":"true"}]'
              end as candidate_owner_json       
from t 
where email is not null

