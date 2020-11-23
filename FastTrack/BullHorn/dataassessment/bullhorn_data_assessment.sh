#!/bin/bash -e
#cleansing="| tail -n +3 | sed '/^\s*$/d' | sed '/^---/ d' | sed '/returned$/ d' | sed '/^Executed in / d' | sed '/)$/ d' "
#host=$(echo `host dmp.vinceredev.com` | grep -o '[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}') #DMP SERVER
#host=$(echo `host dmpfra.vinceredev.com` | grep -o '[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}') #DMP SERVER
#host=$(dig +short dmp.vinceredev.com)
#host=$(dig +short dmpfra.vinceredev.com)
host=$1
db=$2
user=$3
password=$4
TO=$5
pwd=$6

da=/tmp/$db"_Data_Assessment.txt"
fml=$db"_Field_Map_List.csv"
contact=$db"_Contacts_that_moved_to_another_Company.csv"
dupcon=$db"_DuplicatedContacts.csv"
dupcan=$db"_DuplicatedCandidates.csv"


echo "2.BULLHORN_fn_ConvertHTMLToText.sql" && sqlcmd -S "$host" -U "$user" -P "$password" -d "$db" -i $pwd/2.BULLHORN_fn_ConvertHTMLToText.sql -I
echo "3.dbo.udf_StripHTML_exptruong_optimized.sql" && sqlcmd -S "$host" -U "$user" -P "$password" -d "$db" -i $pwd/3.dbo.udf_StripHTML_exptruong_optimized.sql
echo "4.BULLHORN_countries.sql" && sqlcmd -S "$host" -U "$user" -P "$password" -d "$db" -i $pwd/4.BULLHORN_countries.sql | sed '/(1 rows affected)$/ d' | sed '/^\s*$/d'
echo "5.dbo.removeNullCharacters.sql" && sqlcmd -S "$host" -U "$user" -P "$password" -d "$db" -i $pwd/5.dbo.removeNullCharacters.sql | sed '/(1 rows affected)$/ d' | sed '/^\s*$/d'

echo "ALTER DATABASE [$2] SET COMPATIBILITY_LEVEL = 130;" && mssql -s "$host" -u "$user" -p "$password" -d "$db" -q "ALTER DATABASE [$2] SET COMPATIBILITY_LEVEL = 130;" | tail -n +3 | sed '/^\s*$/d' | sed '/^---/ d' | sed '/returned$/ d' | sed '/^Executed in / d' | sed '/)$/ d' > $da
echo "ALTER TABLE BULLHORN1.BH_UserMessage add email_content nvarchar(max)" && sqlcmd -S "$host" -U "$user" -P "$password" -d "$db" -Q "alter table BULLHORN1.BH_UserMessage add email_content nvarchar(max);" | tail -n +3 | sed '/^\s*$/d' | sed '/^---/ d' | sed '/returned$/ d' | sed '/^Executed in / d' | sed '/)$/ d' >> $da

echo DATA ASSESSMENT: >> $da
echo -e '\n'COMPANY: 			`sqlcmd -S "$host" -U "$user" -P "$password" -d "$db" -Q "select count(*) from bullhorn1.BH_ClientCorporation CC;" | sed '/^\s*$/d' | sed '/^---/ d' | sed '/returned$/ d' | sed '/^Executed in / d' | sed '/)$/ d'` >> $da
echo -e '\n'COMPANY - ARCHIVED: `sqlcmd -S "$host" -U "$user" -P "$password" -d "$db" -Q "select count(*) from bullhorn1.BH_ClientCorporation CC where CC.status = 'Archive';" | sed '/^\s*$/d' | sed '/^---/ d' | sed '/returned$/ d' | sed '/^Executed in / d' | sed '/)$/ d'` >> $da
echo -e '\n'COMPANY - STATUS: >> $da
								 sqlcmd -S "$host" -U "$user" -P "$password" -d "$db" -Q "select distinct status from bullhorn1.BH_ClientCorporation CC order by status asc;" | tail -n +3 | sed '/^\s*$/d' | sed '/returned$/d' | sed '/^Executed in /d' | sed '/)$/ d' >> $da


echo -e '\n'CONTACT: 			`sqlcmd -S "$host" -U "$user" -P "$password" -d "$db" -Q "select count(*) from bullhorn1.View_ClientContact Cl where Cl.isdeleted <> 1;" | sed '/^\s*$/d' | sed '/^---/ d' | sed '/returned$/ d' | sed '/^Executed in / d' | sed '/)$/ d'` >> $da
echo -e '\n'CONTACT - ARCHIVED: `sqlcmd -S "$host" -U "$user" -P "$password" -d "$db" -Q "select count(*) from bullhorn1.View_ClientContact Cl where Cl.isdeleted <> 1 and Cl.status = 'Archive';" | sed '/^\s*$/d' | sed '/^---/ d' | sed '/returned$/ d' | sed '/^Executed in / d' | sed '/)$/ d'` >> $da
echo -e '\n'CONTACT - STATUS: >> $da
								 sqlcmd -S "$host" -U "$user" -P "$password" -d "$db" -Q "select distinct status from bullhorn1.View_ClientContact Cl where Cl.isdeleted <> 1 order by status asc;" | tail -n +3 | sed '/^\s*$/d' | sed '/returned$/d' | sed '/^Executed in /d' | sed '/)$/ d' >> $da


echo -e '\n'JOB: 				`sqlcmd -S "$host" -U "$user" -P "$password" -d "$db" -Q "select count(*) from bullhorn1.BH_JobPosting a where a.isdeleted <> 1;" | sed '/^\s*$/d' | sed '/^---/ d' | sed '/returned$/ d' | sed '/^Executed in / d' | sed '/)$/ d'` >> $da
echo -e '\n'JOB - ARCHIVED: 	`sqlcmd -S "$host" -U "$user" -P "$password" -d "$db" -Q "select count(*) from bullhorn1.BH_JobPosting a where a.isdeleted <> 1 and a.status = 'Archive';" | sed '/^\s*$/d' | sed '/^---/ d' | sed '/returned$/ d' | sed '/^Executed in / d' | sed '/)$/ d'` >> $da
echo -e '\n'JOB - STATUS: >> $da
								 sqlcmd -S "$host" -U "$user" -P "$password" -d "$db" -Q "select distinct status from bullhorn1.BH_JobPosting a order by status asc;" | tail -n +3 | sed '/^\s*$/d' | sed '/returned$/d' | sed '/^Executed in /d' | sed '/)$/ d' >> $da
echo -e '\n'JOB TYPE - Kindly match the list bellow to PERMANENT, INTERIM_PROJECT_CONSULTING, TEMPORARY, CONTRACT, TEMPORARY_TO_PERMANENT: >> $da
sqlcmd -S "$host" -U "$user" -P "$password" -d "$db" -Q "select distinct employmentType from bullhorn1.BH_JobPosting a order by employmentType;" | tail -n +3 | sed '/^\s*$/d' | sed '/returned$/d' | sed '/^Executed in /d' | sed '/)$/ d' >> $da


echo -e '\n'CANDIDATE: 			`sqlcmd -S "$host" -U "$user" -P "$password" -d "$db" -Q "select count(*) from bullhorn1.Candidate C where C.isdeleted <> 1;" | sed '/^\s*$/d' | sed '/^---/ d' | sed '/returned$/ d' | sed '/^Executed in / d' | sed '/)$/ d'` >> $da
echo -e '\n'CANDIDATE - ARCHIVED: `sqlcmd -S "$host" -U "$user" -P "$password" -d "$db" -Q "select count(*) from bullhorn1.Candidate C where C.isdeleted <> 1 and C.status = 'Archive';" | sed '/^\s*$/d' | sed '/^---/ d' | sed '/returned$/ d' | sed '/^Executed in / d' | sed '/)$/ d'` >> $da
echo -e '\n'CANDIDATE - STATUS: >> $da
								 sqlcmd -S "$host" -U "$user" -P "$password" -d "$db" -Q "select distinct status from bullhorn1.Candidate C where C.isdeleted <> 1 order by status asc;" | tail -n +3 | sed '/^\s*$/d' | sed '/returned$/d' | sed '/^Executed in /d' | sed '/)$/ d' >> $da
echo -e '\n'CANDIDATE JOB TYPE \(employmentPreference\) - Kindly match the list bellow to PERMANENT, PROJECT_CONSULTING, TEMPORARY, CONTRACT, TEMP_TO_PERM: >> $da
#sqlcmd -S "$host" -U "$user" -P "$password" -d "$db" -Q "select distinct trim(employmentPreference) as employmentPreference from bullhorn1.Candidate a order by employmentPreference;" | tail -n +3 | sed '/^\s*$/d' | sed '/returned$/d' | sed '/^Executed in /d' | sed '/)$/ d'
sqlcmd -S "$host" -U "$user" -P "$password" -d "$db" -Q "SELECT distinct trim(employmentPreference.value) as employmentPreference FROM bullhorn1.Candidate m CROSS APPLY STRING_SPLIT(m.employmentPreference,',') AS employmentPreference where m.isdeleted <> 1 and m.employmentPreference <> '';" | tail -n +3 | sed '/^\s*$/d' | sed '/returned$/d' | sed '/^Executed in /d' | sed '/)$/ d' >> $da

#sqlcmd -S "$host" -U "$user" -P "$password" -d "$db" -Q "select distinct employeeType from bullhorn1.Candidate a order by employeeType;" | tail -n +3 | sed '/^\s*$/d' | sed '/returned$/d' | sed '/^Executed in /d' | sed '/)$/ d'

echo -e '\n'JOB APPLICATION: 	`sqlcmd -S "$host" -U "$user" -P "$password" -d "$db" -Q "select count(*) from bullhorn1.BH_JobResponse JR where JR.status <> '' and JR.status is not null;" | sed '/^\s*$/d' | sed '/^---/ d' | sed '/returned$/ d' | sed '/^Executed in / d' | sed '/)$/ d'` >> $da
echo -e '\n'JOB APPLICATION - Kindly match the list bellow to SHORTLISTED, SENT, FIRST_INTERVIEW, SECOND_INTERVIEW, OFFERED, PLACEMENT_PERMANENT, PLACEMENT_CONTRACT, PLACEMENT_TEMP: >> $da
sqlcmd -S "$host" -U "$user" -P "$password" -d "$db" -Q "select distinct JR.status from bullhorn1.BH_JobResponse JR order by status;" | tail -n +3 | sed '/^\s*$/d' | sed '/^---/ d' | sed '/returned$/ d' | sed '/^Executed in / d' | sed '/)$/ d' >> $da

echo -e '\n'PLACEMENT JOB TYPE \(employmentType\) - Kindly match the list bellow to PERMANENT, CONTRACT, TEMP_TO_PERM, TEMPORARY, PROJECT_CONSULTING: >> $da
sqlcmd -S "$host" -U "$user" -P "$password" -d "$db" -Q "select distinct employmentType from bullhorn1.BH_Placement a order by employmentType;" | tail -n +3 | sed '/^\s*$/d' | sed '/returned$/d' | sed '/^Executed in /d' | sed '/)$/ d' >> $da

cd /tmp
echo "================" >> $da
#echo "Field Map List: attached" >> $da
#echo "Contacts_moved_to_another_Company: attached" >> $da
echo DOCUMENTS: >> $da
echo -e $(~/get_file_type.sh $TO/User*/ | grep "Total of doc") >> $da

echo "Data Assessment: $db"_Data_Assessment.txt""
#echo "Field Map List: /tmp/$db"_Field_Map_List.csv"" && mssql -s "$host" -u "$user" -p "$password" -d "$db" -q "select fieldMapID, entity, columnName, display, editType, isRequired, isHidden, valueList, allowMultiple, description, hint, defaultValue, isHidden from bullhorn1.BH_FieldMapList" -f csv > /tmp/$db"_Field_Map_List.csv" || true
echo "Field Map List (with no-hidden customfield): $db"_Field_Map_List.csv"" && mssql -s "$host" -u "$user" -p "$password" -d "$db" -q "$(cat $pwd/1.FieldMapList.sql)" -f csv > $db"_Field_Map_List.csv" || true

echo "Contacts_moved_to_another_Company: $db"_Contacts_that_moved_to_another_Company.csv"" && mssql -s "$host" -u "$user" -p "$password" -d "$db" -q "$(cat $pwd/6.Job_investigate_Contacts_that_moved_to_another_Company_status_Archive.sql)" -f csv > $db"_Contacts_that_moved_to_another_Company.csv" || true

echo "Duplicated Contacts: $db"_DuplicatedContacts.csv"" && mssql -T 900000 -s "$host" -u "$user" -p "$password" -d "$db" -q "$(cat $pwd/2Contact_duplicated_EMAIL_with_no_of_jobs_v2.sql)" -f csv > $db"_DuplicatedContacts.csv" || true

echo "Duplicated Candidates: $db"_DuplicatedCandidates.csv"" && mssql -T 900000 -s "$host" -u "$user" -p "$password" -d "$db" -q "$(cat $pwd/4Candidate_duplicated_EMAIL.sql)" -f csv > $db"_DuplicatedCandidates.csv" || true

# MAIL
tar cvzf $db.tar.gz $db"_"*
mailx -s "Data Assessment - $db" -A $db.tar.gz -a "From: devops@vincere.io" "truong.pham@vincere.io" </dev/null
#mailx -s "Data Assessment - $db" -A $da -A $fml -A $contact -A $dupcon -A $dupcan -a "From: devops@vincere.io" "truong.pham@vincere.io" </dev/null
#mailx -s "Data Assessment - $db" -A $da -A $fml -A $contact -a "From: devops@vincere.io" "truong.pham@vincere.io,exptruong@gmail.com" </dev/null


# ERROR:
# /usr/local/lib/node_modules/sql-cli/node_modules/password-prompt/index.js:7
#   hide: (ask, options = {}) => read.raw(ask, false, options),
# SOLUTION: cp index.js /usr/local/lib/node_modules/sql-cli/node_modules/password-prompt/index.js




#mssql -s 192.168.20.132 -u sa -p 123qwe -d TestDatabase -q "UPDATE myWidechar SET myWidechar.Note = '$content' where myWidechar.PersonID = 4"
#mssql -s 192.168.20.132 -u sa -p 123qwe -d TestDatabase -q "DELETE FROM myWidechar where PersonID = 5"

### INSTALL MSSQL COMMAND ###
# sudo apt-get install -y npm
# sudo npm install -g sql-cli

# ISSUE: /usr/bin/env: ‘node’: No such file or directory
# sudo ln -s /usr/bin/nodejs /usr/bin/node

### INSTALL SQLCMD COMMAND ###
# sudo sh -c "curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -"
# sudo sh -c "echo deb [arch=amd64] https://packages.microsoft.com/ubuntu/16.04/prod xenial main >> /etc/apt/sources.list.d/sql-server.list"
# sudo apt-get update
# sudo apt-get install mssql-tools -y --allow-unauthenticated
# sudo ls /opt/mssql-tools/bin/sqlcmd*
# sudo ln -sfn /opt/mssql-tools/bin/sqlcmd /usr/bin/sqlcmd

