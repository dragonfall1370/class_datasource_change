#!/bin/bash -e

db=StaraniseDomain

#find /mnt/vmware/candidate/ -name *.rtf -type f > /tmp/DOCUMENT_CONTENT_FILE.txt
file=/tmp/DOCUMENT_CONTENT_FILE.txt

#mssql -s 192.168.20.132 -u sa -p 123qwe -d $db -q "drop table if exists dbo.DOCUMENT_CONTENT;"
#mssql -s 192.168.20.132 -u sa -p 123qwe -d $db -q "CREATE TABLE dbo.DOCUMENT_CONTENT (DOC_ID numeric(16), NOTE nvarchar(max) );"

for i in $(cat $file)
do
	doc_id=$(echo $i | rev | cut -d "." -f2 | cut -d "/" -f1 | rev)	
	#doc_id=$(cut -d "/" -f5 $i | cut -d "." -f1) # > /tmp/DOCUMENT_CONTENT_DOC_ID.txt

	sed "s/'/''/g" $i > /tmp/DOCUMENT_CONTENT_Note.txt
	note=$(cat /tmp/DOCUMENT_CONTENT_Note.txt)
	#note=$(for i in $(cat $file); do sed "s/'/''/g" $i ; done)

	echo "================================================================================"
	echo "DOC_ID: $doc_id"
	#echo "====================================================================================================================================================================================="
	#echo "NOTE: $note"

	sed -i "/$doc_id/d" $file

	mssql -s 192.168.20.132 -u sa -p 123qwe -d $db -q "INSERT INTO dbo.DOCUMENT_CONTENT (DOC_ID, NOTE) VALUES ('$doc_id','$note');"

done

#mssql -s 192.168.20.132 -u sa -p 123qwe -d TestDatabase -q "INSERT INTO dbo.myWidechar (PersonID, FirstName, LastName, BirthDate, AnnualSalary, Note) VALUES ('6','F','L','2000-01-01','80000','$content');"
#mssql -s 192.168.20.132 -u sa -p 123qwe -d TestDatabase -q "UPDATE myWidechar SET myWidechar.Note = '$content' where myWidechar.PersonID = 4"
#mssql -s 192.168.20.132 -u sa -p 123qwe -d TestDatabase -q "DELETE FROM myWidechar where PersonID = 5"

