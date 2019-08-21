#!/bin/bash -e

find $1 -name *.rtf -type f > /tmp/DOCUMENT_CONTENT_FILE.txt
file=/tmp/DOCUMENT_CONTENT_FILE.txt
#PGPASSWORD="dbapplication_user"
export PGPASSWORD=dbapplication_user
export user=dbapplication_user
host=$(echo `host dmp.vinceredev.com` | grep -o '[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}')
port=5432
db=staranise-review.vinceredev.com
prefix="General Notes"
#mssql -s 192.168.20.132 -u sa -p 123qwe -d $db -q "drop table if exists dbo.DOCUMENT_CONTENT;"
#mssql -s 192.168.20.132 -u sa -p 123qwe -d $db -q "CREATE TABLE dbo.DOCUMENT_CONTENT (DOC_ID numeric(16), NOTE nvarchar(max) );"

x=0
for i in $(cat $file)
do
	dos2unix $i 2>/dev/null
	doc_id=$(echo $i | rev | cut -d "." -f2 | cut -d "/" -f1 | rev)	
	#doc_id=$(cut -d "/" -f5 $i | cut -d "." -f1) # > /tmp/DOCUMENT_CONTENT_DOC_ID.txt

#	sed "s/'/''/g" $i | sed "s:﻿::g" | sed "s:^@::g" | sed "s:<A1>::g" | sed "s:<A6>::g" > /tmp/DOCUMENT_CONTENT_Note.txt
        sed "s/'/''/g" $i | sed "s:<feff>::g" | sed "s:^X::g" | sed "s:^@::g" | sed "s:<A1>::g" | sed "s:<A6>::g" | sed "s:¦       ^@^@::g" | sed "s:<80><99>::g" > /tmp/DOCUMENT_CONTENT_Note.txt
	dos2unix /tmp/DOCUMENT_CONTENT_Note.txt 2>/dev/null
	note=$(cat /tmp/DOCUMENT_CONTENT_Note.txt)
	#note=$(for i in $(cat $file); do sed "s/'/''/g" $i ; done)

	echo "======================="
	x=`expr $x + 1`
	echo NO.$x
	echo "DOC_ID: $doc_id"
	#echo "NOTE: $note"
        sed -i "/$doc_id/d" $file

	psql -h $host -U $user -p $port -d "$db" -c "INSERT INTO position_candidate_feedback (doc_id, user_account_id, comment_body) VALUES ('$doc_id', '-10', '$prefix: $note');"
        sed -i "/$doc_id/d" $file

done #>> /tmp/DOCUMENT_CONTENT_LOG.txt
