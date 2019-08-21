#!/usr/bin/expect
	spawn /bin/sh import_document_postgres.sh
	expect "Argument list too long"
	send "/bin/sh import_document_postgres.sh\r"
	expect "yamla#123"
	send "bye\r"
