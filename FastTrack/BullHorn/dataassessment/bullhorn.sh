#!/bin/bash -e
#Author: Truong Pham

if [ $# != 3 ] ; then
      echo -e "WARNING: There should have 3 parameters. \nFor example: ./bullhorn.sh <tenant_name> <database_name> download/extractdb/restoredb/extractdoc/da/extractemail/all" 1>&2
        exit 1
fi

echo -e "Begin: " $(date +%Y.%m.%d-%H:%M:%S) "\n"

pwd=$(pwd)
#d=20201104
d=$(date +%Y%m%d)
s=$(grep -i link $pwd/bullhorn_ftp | cut -d':' -f2 | tr -d '[:space:]')
u=$(grep -i user $pwd/bullhorn_ftp | cut -d':' -f2 | tr -d '[:space:]')
p=$(grep -i ftp.*password $pwd/bullhorn_ftp | cut -d":" -f2 | tr -d '[:space:]')
echo FTP:$s
echo FTP User:$u
echo FTP Password:$p

tenant=$1 && echo -e "TENANT_NAME = $tenant"
host="10.0.1.65"
db=$2 && echo -e "DATABASE_NAME = $db"
user="sa"
password=123$%^qwe
data="D:\MSSQL\DATA"

#TO=/mnt/dmpfra/d/$1/$d
TO=/mnt/dmpfra/h/$1/$d
echo TO = $TO/
TO_windows=$(echo $TO/ | awk -F"/" '{print $4 ":\\" $5 "\\" $6 "\\"}')
echo TO_windows $TO_windows

dos2unix $pwd/bullhorn_ftp
u=$(grep -i user $pwd/bullhorn_ftp | cut -d':' -f2 | tr -d '[:space:]')

download() {
	cp $pwd/bullhorn_ftp $TO/bullhorn_ftp.txt
	echo "lftp -u $u,$p ftps://$s -e set ssl:verify-certificate/$s no; ls -alh; quit > $TO/bullhorn_download_logs"
	lftp -u $u,$p ftps://$s -e "set ssl:verify-certificate/$s no; ls -alh; quit" > $TO/bullhorn_download_logs
	lftp -u $u,$p ftps://$s -e "set ssl:verify-certificate/$s no; lcd $TO/; mirror -r . . ; quit" >> $TO/bullhorn_download_logs
}


extractdb() {
	cd $TO
	#password="$p"; 7z x -p$p `ls | grep 7z` -y
	echo -e 7z e -p$p `ls | grep 7z` -o$TO "BULL*" -y
}


restoredb() {
	cd $TO
	bak=$(ls | grep bak$)
	echo bak = $bak
	cer=$(ls | grep cer$ | cut -d"." -f1)
	echo cer = $cer
	pvk=$(ls | grep pvk$ | cut -d"." -f1)
	echo pvk = $pvk
	cer_pass=$(grep -i cert.*pass $pwd/bullhorn_ftp | cut -d":" -f2 | awk '{print $1}')
	echo cer_pass = $cer_pass
	restore="CREATE CERTIFICATE $cer FROM FILE = '$TO_windows$cer.cer' WITH PRIVATE KEY ( FILE = '$TO_windows$pvk.pvk', DECRYPTION BY PASSWORD = '$cer_pass');"
	echo restore = $restore

	cd $TO
	echo "Install certificate" && sqlcmd -S "$host" -U "$user" -P "$password" -d master -Q "$restore" || true

	echo "Restore DB" && sqlcmd -S "$host" -U "$user" -P "$password" -d master -Q "
	RESTORE DATABASE $db FROM DISK = '$TO_windows$bak' WITH  FILE = 1,
	MOVE N'BULLHORN_DATA' TO N'$data\\$db"_DATA.NDF"',
	MOVE N'BULLHORN_PRIMARY' TO N'$data\\$db"_PRIMARY.MDF"',
	MOVE N'BULLHORN_LOG' TO N'$data\\$db"_LOG.LDF"',
	MOVE N'BULLHORN_INDEX' TO N'$data\\$db"_INDEX.NDF"',
	MOVE N'BULLHORN_FTINDEX' TO N'$data\\$db"_FTINDEX.NDF"', RECOVERY, REPLACE, NOUNLOAD, STATS = 5"
}


da(){
	$pwd/bullhorn_data_assessment.sh $host $db $user $password $TO $pwd
}


extractdoc() {
        cd $TO
	mkdir -p $TO
        7z x -p$p `ls | grep 7z` -o$TO/doc/ "UserWorkFiles" -y
        7z x -p$p `ls | grep 7z` -o$TO/doc/ "UserAttachments" -y
}


extractemail() {
	echo "ALTER DATABASE [$db] SET COMPATIBILITY_LEVEL = 130;" && sqlcmd -S "$host" -U "$user" -P "$password" -d $db -Q "ALTER DATABASE [$db] SET COMPATIBILITY_LEVEL = 130;" || true
	echo "ALTER TABLE BULLHORN1.BH_UserMessage add email_content nvarchar(max)" && sqlcmd -S "$host" -U "$user" -P "$password" -d "$db" -Q "alter table BULLHORN1.BH_UserMessage add email_content nvarchar(max);" || true
	python $pwd/mssql_convert_email_BullHorn_v4.py $db
}


all(){
	stty size | perl -ale 'print "\033[1;32m-\033[m"x$F[1]' #GREEN line
	#echo -e "$(tput setaf 2)$(tput setab 7)DOWNLOAD$(tput sgr 0)\n" && download
	
	stty size | perl -ale 'print "\033[1;32m-\033[m"x$F[1]'
	echo -e "$(tput setaf 2)$(tput setab 7)EXTRACT DATABASE$(tput sgr 0)\n" && extractdb
	
	stty size | perl -ale 'print "\033[1;32m-\033[m"x$F[1]'
	echo -e "$(tput setaf 2)$(tput setab 7)RESTORE DATABASE$(tput sgr 0)\n" && restoredb
	
	stty size | perl -ale 'print "\033[1;32m-\033[m"x$F[1]'
	echo -e "$(tput setaf 2)$(tput setab 7)EXTRACT DOCUMENT$(tput sgr 0)\n" && extractdoc
	
	stty size | perl -ale 'print "\033[1;32m-\033[m"x$F[1]'
	echo -e "$(tput setaf 2)$(tput setab 7)DATA ASSESSMENT$(tput sgr 0)\n" && da
	
	stty size | perl -ale 'print "\033[1;32m-\033[m"x$F[1]'
	echo -e "$(tput setaf 2)$(tput setab 7)EXTRACT EMAIL$(tput sgr 0)\n" && extractemail
	
	stty size | perl -ale 'print "\033[1;31m-\033[m"x$F[1]' #RED line
}


case $3 in
 download ) download;;
 extractdb) extractdb;;
 restoredb) restoredb;;
 extractdoc) extractdoc;;
 da) da;;
 extractemail) extractemail;;
 all) all;;
esac



# SAMPLE
#lftp -u Jul_WeAreWiserLtd_14403_BULLHORN13414,w2020_268733_y8x3ifo3 ftps://ftp-uk.Bullhorn.com -e "set ssl:verify-certificate/ftp-uk.Bullhorn.com no; ls"

#restore="CREATE CERTIFICATE $cer FROM FILE = 'D:\bth\20200817\\$cer.cer' WITH PRIVATE KEY ( FILE = 'D:\bth\20200817\\$pvk.pvk', DECRYPTION BY PASSWORD = '$cer_pass');"

#echo "Create DB" && sqlcmd -S "$host" -U "$user" -P "$password" -d tempdb -Q "create database $db;"

#sqlcmd -S 10.0.1.65 -U sa -P 123$%^qwe -d master -Q "RESTORE DATABASE bth2 FROM DISK = 'D:\bth\20200817\BULLHORN7493_47650_FULL_CopyOnly_2016_2020.06.30_09.31.01.bak' WITH  FILE = 1,
#MOVE N'BULLHORN_DATA' TO N'D:\MSSQL\DATA\bth2_DATA.NDF',
#MOVE N'BULLHORN_PRIMARY' TO N'D:\MSSQL\DATA\bth2_PRIMARY.MDF',
#MOVE N'BULLHORN_LOG' TO N'D:\MSSQL\DATA\bth2_LOG.LDF',
#MOVE N'BULLHORN_INDEX' TO N'D:\MSSQL\DATA\bth2_INDEX.NDF',
#MOVE N'BULLHORN_FTINDEX' TO N'D:\MSSQL\DATA\bth2_FTINDEX.NDF', NOUNLOAD, STATS = 5"


