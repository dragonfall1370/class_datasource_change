# NOTE:
# SET group_concat_max_len = 2147483647;
# select distinct postype from api_jobs a
# select sno, type, name from manage where type = 'jotype' 


select
        a.sno as 'position-externalId'
       ,a.company as 'company External ID' ,com.cname
       ,a.contact as 'position-contactId' , con.fname, con.lname
       ,o.email ,a.owner as 'position-owners'
       ,ind.name ,a.industryid as 'INDUSTRY' #--<<
       ,a.postitle as 'position-title'
       ,case a.posworkhr
              when 'fulltime' then 'PERMANENT'
              when 'parttime' then 'TEMPORARY'
        end as 'position-employmenttype'
       ,case type.name #a.postype
              when 'Temp/Contract' then 'CONTRACT'
              when 'Direct' then 'PERMANENT'
              when 'Temp/Contract to Direct' then 'TEMPORARY_TO_PERMANENT'
              when 'Internal Temp/Contract' then 'CONTRACT'
              when 'Internal Direct' then 'PERMANENT'
        end as 'position-type'
       ,a.no_of_pos 'position-headcount'
       ,a.posted_date as 'position-startDate'
       ,a.remove_date as 'position-endDate'
       ,id.note as 'position-internalDescription'
       ,pd.note as 'position-publicDescription'
       ,note.note as 'position-note'
       #as 'position-document'
       #notes as 'ACTIVITIES COMMENTS'
       #notes, tasklist as 'ACTIVITIES COMMENTS'
	#as 'DOCUMENTS'
# select * # select count(*)
from api_jobs a
left join staffoppr_cinfo com on com.sno = a.company
left join staffoppr_contact con on con.sno = a.contact
left join TRUONG_companyowneremail o on o.owner = a.owner #accessto
left join (select sno, type, name from manage where type = 'jotype') type on type.sno = a.postype
left join (select sno, type, name from manage where type = 'joindustry') ind on ind.sno = a.industryid
left join (
              -- JOB INTERNAL DESCRIPTION
              select sno
                     ,concat(
                             case when (a.prateopen_amt = '' OR a.prateopen_amt = '0' OR a.prateopen_amt is NULL) THEN '' ELSE concat('Rates > Rate: ',a.prateopen_amt,char(13)) END
                            ,case when (a.pperiod = '' OR a.pperiod = '0' OR a.pperiod is NULL) THEN '' ELSE concat('Per Month: ',a.pperiod,char(13)) END
                            ,case when (a.pcurrency = '' OR a.pcurrency = '0' OR a.pcurrency is NULL) THEN '' ELSE concat('Currency: ',a.pcurrency,char(13)) END
                            ,case when (a.job_loc_tax = '' OR a.job_loc_tax = '0' OR a.job_loc_tax is NULL) THEN '' ELSE concat('Taxable / Non-taxable: ',a.job_loc_tax,char(13)) END
                            ,case when (a.prateopen = '' OR a.prateopen = '0' OR a.prateopen is NULL) THEN '' ELSE concat('Regular pay Rate > Open: ',a.prateopen,char(13)) END
                            ,case when (a.prateopen_amt = '' OR a.prateopen_amt = '0' OR a.prateopen_amt is NULL) THEN '' ELSE concat('Regular pay Rate > Text Field next to Open: ',a.prateopen_amt,char(13)) END
                            ,case when (a.burden = '' OR a.burden = '0' OR a.burden is NULL) THEN '' ELSE concat('Bill Burden: ',a.burden,char(13)) END
                            ,case when (a.margin = '' OR a.margin = '0' OR a.margin is NULL) THEN '' ELSE concat('Margin: ',a.margin,char(13)) END
                            ,case when (a.markup = '' OR a.markup = '0' OR a.markup is NULL) THEN '' ELSE concat('Markup: ',a.markup,char(13)) END
                            ,case when (a.otrate = '' OR a.otrate = '0' OR a.otrate is NULL) THEN '' ELSE concat('Overtime Pay Rate: ',a.otrate,char(13)) END
                            ,case when (a.ot_period = '' OR a.ot_period = '0' OR a.ot_period is NULL) THEN '' ELSE concat('Overtime Pay Rate > Per Hour: ',a.ot_period,char(13)) END
                            ,case when (a.ot_currency = '' OR a.ot_currency = '0' OR a.ot_currency is NULL) THEN '' ELSE concat('Overtime Pay Rate > Currency : ',a.ot_currency,char(13)) END
                            ,case when (a.diem_billable  = '' OR a.diem_billable = '0' OR a.diem_billable is NULL) THEN '' ELSE concat('Overtime Pay Rate > Billable / Non Billable: ',a.diem_billable ,char(13)) END
                            ,case when (a.otbrate_amt = '' OR a.otbrate_amt = '0' OR a.otbrate_amt is NULL) THEN '' ELSE concat('Overtime Bill Rate: ',a.otbrate_amt,char(13)) END
                            ,case when (a.otbrate_period = '' OR a.otbrate_period = '0' OR a.otbrate_period is NULL) THEN '' ELSE concat('Overtime Bill Rate > Per Hour: ',a.otbrate_period,char(13)) END
                            ,case when (a.otbrate_curr = '' OR a.otbrate_curr = '0' OR a.otbrate_curr is NULL) THEN '' ELSE concat('Overtime Bill Rate > Currency: ',a.otbrate_curr,char(13)) END
                            ,case when (a.diem_taxable = '' OR a.diem_taxable  = '0' OR a.diem_taxable  is NULL) THEN '' ELSE concat('Overtime Bill Rate > Taxable / Non Taxable: ',a.diem_taxable ,char(13)) END
                            ,case when (a.double_prate_amt = '' OR a.double_prate_amt = '0' OR a.double_prate_amt is NULL) THEN '' ELSE concat('Double Time Pay Rate: ',a.double_prate_amt,char(13)) END
                            ,case when (a.double_prate_period = '' OR a.double_prate_period = '0' OR a.double_prate_period is NULL) THEN '' ELSE concat('Double Time Pay Rate > Per Hour: ',a.double_prate_period,char(13)) END
                            ,case when (a.double_prate_curr = '' OR a.double_prate_curr = '0' OR a.double_prate_curr is NULL) THEN '' ELSE concat('Double Time Pay Rate > Currency: ',a.double_prate_curr,char(13)) END
                            ,case when (a.diem_billable = '' OR a.diem_billable  = '0' OR a.diem_billable  is NULL) THEN '' ELSE concat('Double Time Pay Rate > Billable / Non Billable: ',a.diem_billable ,char(13)) END
                            ,case when (a.double_brate_amt = '' OR a.double_brate_amt = '0' OR a.double_brate_amt is NULL) THEN '' ELSE concat('Double Time Bill Rate: ',a.double_brate_amt,char(13)) END
                            ,case when (a.double_brate_period = '' OR a.double_brate_period = '0' OR a.double_brate_period is NULL) THEN '' ELSE concat('Double Time Bill Rate > Per Hour: ',a.double_brate_period,char(13)) END
                            ,case when (a.double_brate_curr = '' OR a.double_brate_curr = '0' OR a.double_brate_curr is NULL) THEN '' ELSE concat('Double Time Bill Rate > Currency: ',a.double_brate_curr,char(13)) END
                            ,case when (a.diem_taxable  = '' OR a.diem_taxable  = '0' OR a.diem_taxable  is NULL) THEN '' ELSE concat('Double Time Bill Rate > Taxable / Non Taxable: ',a.diem_taxable ,char(13)) END
                            ,case when (a.placement_fee = '' OR a.placement_fee = '0' OR a.placement_fee is NULL) THEN '' ELSE concat('Placement Fee: ',a.placement_fee,char(13)) END
                            ,case when (a.placement_curr = '' OR a.placement_curr = '0' OR a.placement_curr is NULL) THEN '' ELSE concat('Placement Fee > Currency: ',a.placement_curr,char(13)) END
                            ,case when (a.distributename = '' OR a.distributename = '0' OR a.distributename is NULL) THEN '' ELSE concat('Commission / Splits : ',a.distributename,char(13)) END
                            ,case when (a.payrollpid = '' OR a.payrollpid = '0' OR a.payrollpid is NULL) THEN '' ELSE concat('Payroll Provider ID: ',a.payrollpid,char(13)) END
                            #,case when (a.wcomp_code = '' OR a.wcomp_code = '0' OR a.wcomp_code is NULL) THEN '' ELSE concat('Workers Compensation Code: ',a.wcomp_code,char(13)) END
                            ,case when (a.pterms = '' OR a.pterms = '0' OR a.pterms is NULL) THEN '' ELSE concat('Payment Terms: ',a.pterms,char(13)) END
                            ,case when (a.tsapp = '' OR a.tsapp = '0' OR a.tsapp is NULL) THEN '' ELSE concat('Timesheet Approval: ',a.tsapp,char(13)) END
                            #,case when (a.meta_keywords = '' OR a.meta_keywords = '0' OR a.meta_keywords is NULL) THEN '' ELSE concat('Meta Keywords (for SEO): ',a.meta_keywords,char(13)) END
                            #,case when (a.meta_desc = '' OR a.meta_desc = '0' OR a.meta_desc is NULL) THEN '' ELSE concat('Meta Description (for SEO): ',a.meta_desc,char(13)) END
                            #,case when (a.skill_name = '' OR a.skill_name = '0' OR a.skill_name is NULL) THEN '' ELSE concat('Skill Name: ',a.skill_name,char(13)) END
                            #,case when (a.last_used = '' OR a.last_used = '0' OR a.last_used is NULL) THEN '' ELSE concat('Last Used: ',a.last_used,char(13)) END
                            #,case when (a.skill_level = '' OR a.skill_level = '0' OR a.skill_level is NULL) THEN '' ELSE concat('Skill Level: ',a.skill_level,char(13)) END
                            #,case when (a.expe = '' OR a.expe = '0' OR a.expe is NULL) THEN '' ELSE concat('Years of Experience: ',a.expe,char(13)) END
                            ,case when (a.city = '' OR a.city = '0' OR a.city is NULL) THEN '' ELSE concat('Relocation: ',a.city,char(13)) END
                            ,case when (a.state = '' OR a.state = '0' OR a.state is NULL) THEN '' ELSE concat('Relocation: ',a.state,char(13)) END
                            ,case when (a.country = '' OR a.country = '0' OR a.country is NULL) THEN '' ELSE concat('Relocation: ',a.country,char(13)) END
                      ) as 'note'
              from api_jobs_pref a
       ) id on id.sno = a.sno
left join (
              -- JOB PUBLIC DESCRIPTION
              select sno
                     ,concat(
               case when (a.posdesc = '' OR a.posdesc = '0' OR a.posdesc is NULL) THEN '' ELSE concat('Description: ',a.posdesc,char(13)) END
              ,case when (a.requirements = '' OR a.requirements = '0' OR a.requirements is NULL) THEN '' ELSE concat('Requirements: ',a.requirements,char(13)) END
              ,case when (a.education = '' OR a.education = '0' OR a.education is NULL) THEN '' ELSE concat('Education: ',a.education,char(13)) END
              ,case when (a.experience = '' OR a.experience = '0' OR a.experience is NULL) THEN '' ELSE concat('Years of Experience: ',a.experience,char(13)) END
                      ) as 'note'
              from api_jobs a
              where a.sno = 2553 a.posdesc like '%This manager level role is responsible for reporting, planning and analyzing financial performance%'
       ) pd on pd.sno = a.sno
left join (
              -- NOTE
              select a.sno
                     ,group_concat(
                             #--case when (a.posstatus = '' OR a.posstatus = '0' OR a.posstatus is NULL) THEN '' ELSE concat('Status: ',status.name,char(13)) END ###a.posstatus
                            #--case when (a.jostage = '' OR a.jostage = '0' OR a.jostage is NULL) THEN '' ELSE concat('Stage: ',jostage.name,char(13)) END ###a.jostage
                            case when (a.accessto = '' OR a.accessto = '0' OR a.accessto is NULL) THEN '' ELSE concat('Share: ',a.accessto,char(13)) END
                            #--,case when (a.sourcetype = '' OR a.sourcetype = '0' OR a.sourcetype is NULL) THEN '' ELSE concat('Source Type: ',source.name,char(13)) END ###a.sourcetype
              #Job Note > Reason
              #Job Note > Punch Method
              #Supervisor Email
                            ,case when (a.con_det = '' OR a.con_det = '0' OR a.con_det is NULL) THEN '' ELSE concat('Numero Contacto Empleado: ',a.con_det,'\n') END
                            #,case when (a. = '' OR a.con_det = '0' OR a.con_det is NULL) THEN '' ELSE concat('HRM Department: ',a.con_det,char(13)) END
                            #--,case when (a.catid = '' OR a.catid = '0' OR a.catid is NULL) THEN '' ELSE concat('Category: ',cat.name,char(13)) END ###a.catid
                            ,case when (a.refcode = '' OR a.refcode = '0' OR a.refcode is NULL) THEN '' ELSE concat('Ref. Code: ',a.refcode,char(13)) END
                            ,case when (a.closepos = '' OR a.closepos = '0' OR a.closepos is NULL) THEN '' ELSE concat('Filled: ',a.closepos,'\n') END
                            ,case when (a.joblocation = '' OR a.joblocation = '0' OR a.joblocation is NULL) THEN '' ELSE concat('Job Location: ',a.joblocation,char(13)) END
                            ,case when (a.posreportto = '' OR a.posreportto = '0' OR a.posreportto is NULL) THEN '' ELSE concat('Reports To: ',a.posreportto,'\n') END
                            #,case when (a.po_num = '' OR a.po_num = '0' OR a.po_num is NULL) THEN '' ELSE concat('PO Number: ',a.po_num,char(13)) END
                            ,case when (a.department = '' OR a.department = '0' OR a.department is NULL) THEN '' ELSE concat('Department: ',a.department,char(13)) END ###
                            ,case when (a.bill_address = '' OR a.bill_address = '0' OR a.bill_address is NULL) THEN '' ELSE concat('Billing Address: ',a.bill_address,'\n') END
                            ,case when (a.billingto = '' OR a.billingto = '0' OR a.billingto is NULL) THEN '' ELSE concat('Billing Contact: ',a.billingto,'\n') END
              #Job Note > Billing Terms
                            ,case when (a.service_terms = '' OR a.service_terms = '0' OR a.service_terms is NULL) THEN '' ELSE concat('Service Terms: ',a.service_terms,char(13)) END
                            #,case when (a.starthour = '' OR a.starthour = '0' OR a.starthour is NULL) THEN '' ELSE concat('Start Hours: ',a.starthour,char(13)) END
                            #,case when (a.endhour = '' OR a.endhour = '0' OR a.endhour is NULL) THEN '' ELSE concat('End Hours: ',a.endhour,char(13)) END
              #Job Note > Shift(s) / Scheduling
                            ,case when (a.conmethod = '' OR a.conmethod = '0' OR a.conmethod is NULL) THEN '' ELSE concat('Contact Method: ',a.conmethod,char(13)) END
                            ,case when (a.posjo = '' OR a.posjo = '0' OR a.posjo is NULL) THEN '' ELSE concat('Requirements: ',a.posjo,char(13)) END
                      SEPARATOR '\n') as 'note'
              # select * # select count(*) # select conmethod, bill_req
              from api_jobs a
                     #left join countries on countries.sno = c.country
                     left join (select sno, type, name from manage where type = 'jobstage' ) jostage on jostage.sno = a.jostage
                     left join (select sno, type, name from manage where type = 'candstatus' ) status on status.sno = a.posstatus
                     left join (select sno, type, name from manage where type = 'candsourcetype' ) source on source.sno = a.sourcetype
                     #left join (select sno, type, name from manage where type = 'contacttype' ) type on type.sno = c.ctype
                     left join (select sno, type, name from manage where type = 'jocategory' ) cat on cat.sno = a.catid
                     left join (select sno, deptname from department ) dept on dept.sno = a.department
                     #where a.postitle like '%Finance Manager%' #a.sno = 2339
                     group by a.sno
       ) note on note.sno = a.sno
where a.sno = 2340 or a.postitle like '%Finance Manager%'
