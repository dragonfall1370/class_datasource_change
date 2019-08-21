-- ----------------------------------- MAIN SCRIPT
select
	concat('BNS_',tc.id) as 'candidateExternalId', 'NOTES' as title
    , left(concat('Candidate External ID: BNS_',tc.id,char(10),
		 if(tc.phone_home = '' or tc.phone_home is NULL,'',Concat(char(10), 'Home Phone: ', tc.phone_home, char(10))),
         if(tc.phone_mobile = '' or tc.phone_mobile is NULL,'',Concat(char(10), 'Mobile Phone: ', tc.phone_mobile, char(10))),
         if(tc.phone_work = '' or tc.phone_work is NULL,'',Concat(char(10), 'Work Phone: ', tc.phone_work, char(10))),
		 if(tc.phone_other = '' or tc.phone_other is NULL,'',Concat(char(10), 'Other Phone: ', tc.phone_other, char(10))),
         if(tc.assigned_user_id = '' or tc.assigned_user_id is NULL,'',Concat(char(10), 'Candidate Owner: ', uiv.username, char(10))),
         -- if(tce.id = '' or tce.id is NULL,'',Concat(char(10), 'Email Address: ', tce.email_address, char(10))),
		 -- if(cl.locationName = '' or cl.locationName is null, '', concat(char(10),'Address: ',if(left(cl.locationName,2)=', ',right(cl.locationName,length(cl.locationName)-2),cl.locationName),char(10))),
		 -- if(cc.geslacht_c = ''  or cc.geslacht_c is NULL,'',Concat(char(10), 'Gender (Geslacht): ', cc.geslacht_c, char(10))),
		 -- if(tc.birthdate is NULL,'',Concat(char(10), 'Birthdate: ', tc.birthdate, char(10))),
         if(cc.her_publicatie_datum_c is NULL,'',Concat(char(10), 'Date of Publication (Her Publicatie Datum): ', cc.her_publicatie_datum_c, char(10))),
         -- if(cc.opleidingsniveau_c = '' or cc.opleidingsniveau_c is NULL,'',Concat(char(10), 'Education (Opleidingsniveau): ', cc.opleidingsniveau_c, char(10))),
         if(tc.date_entered is NULL,'',Concat(char(10), 'Date/time entered: ', tc.date_entered, char(10))),
         if(tc.date_modified is NULL,'',Concat(char(10), 'Date/Time modified: ', tc.date_modified, char(10))),
         if(tc.description = '' or tc.description is NULL,'',Concat(char(10), 'Description: ', char(10), tc.description))
         ),32000)
 as 'noteContent'
from Temp_Candidates tc left join contacts_cstm cc on tc.id = cc.id_c
                -- left join Candidate_Email_Addr cea on tc.id = cea.id
                left join user_info_view uiv on tc.assigned_user_id = uiv.id
                -- left join candidate_location_edited cl on tc.id = cl.id
                -- left join temp_can_emails1 tce on tc.id = tce.id
-- where tc.id is null -- and ac.contact_id = 'dd40aa66-be84-086b-fa6e-4d1900a3cc31'-- and ac.contact_id = '4f7765a8-8888-5312-2a73-4940f0f54d17'