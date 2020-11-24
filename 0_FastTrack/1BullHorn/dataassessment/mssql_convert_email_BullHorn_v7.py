#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import sys
import datetime
import pyodbc
import zlib
from bs4 import BeautifulSoup
import re


################## BEGIN ##################
usage = 'python script-name.py N (with N is number of processed rows)'
freshStart = True
processedRowsCount = 0
# if len(sys.argv) > 1:
	# if int(sys.argv[1]) < 1:
		# print(usage)
		# exit()
	# freshStart = False
	# processedRowsCount = int(sys.argv[1])

#print("###########################################")
print("################## BEGIN ##################")
#print("###########################################")

startTime = datetime.datetime.now()
print("Start: " + str(startTime))
#svr = sys.argv[1]
db = sys.argv[1]
#cn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + svr + ';DATABASE=' + db + ';UID=sa;PWD=123$%^qwe',autocommit=True)

#cn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=dmp.vinceredev.com;DATABASE=gummybear2;UID=sa;PWD=123$%^qwe',autocommit=True)
#cn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=pitch2;UID=sa;PWD=123$%^qwe',autocommit=True)
#cn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=10.0.1.65;DATABASE=itexperts;UID=sa;PWD=123$%^qwe',autocommit=True)
cn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=10.0.1.65;DATABASE=' + db + ';UID=sa;PWD=123$%^qwe',autocommit=True)
cur = cn.cursor()


print("Getting the total of messages...")

CONDITION = 'email_content is null and 0 < DATALENGTH(commentsCompressed) ' # and DATALENGTH(commetsCompressed) < 500000'
#CONDITION = '0 < DATALENGTH(commentsCompressed) and userMessageID in (528) ' # and DATALENGTH(commentsCompressed) < 500000'
# and userMessageID in (320950,320951)'
#condition = 'userMessageID not in (320950)'

sqlStatement = 'SELECT count(*) FROM BULLHORN1.BH_UserMessage WHERE ' + CONDITION
#sqlStatement = 'SELECT count(*) FROM BULLHORN1.BH_UserMessage WHERE email_content is null and DATALENGTH(commentsCompressed) > 0 '
cur.execute(sqlStatement)
total = cur.fetchone()[0]
print("Total messages: " + str(total))

print('Go through each batch, in which, for each row, de-compress the message, strip html tags, then update into BULLHORN1.BH_UserMessage table\n')
batchSize = 500000
batchCount = 0
rowsCountByBatch = 0
if freshStart == False:
	#re-calculate batchCount and rowsCountByBatch
	batchCount = processedRowsCount // batchSize + 1
	if processedRowsCount % batchSize == 0:
		#print(rowsCountByBatch)
		#print(batchSize)
		#print(batchCount)	
		rowsCountByBatch = batchSize * (batchCount)
	else:
		rowsCountByBatch = batchSize * (batchCount - 1)


while rowsCountByBatch < total:
	if freshStart == True:
		batchCount += 1
	if freshStart == True:
		sqlStatement = 'SELECT userMessageID, commentsCompressed FROM BULLHORN1.BH_UserMessage WHERE ' + CONDITION + 'order by userMessageID OFFSET ' + str(rowsCountByBatch) + ' ROWS FETCH NEXT ' + str(batchSize) + ' ROWS ONLY'
	else:
		sqlStatement = 'SELECT userMessageID, commentsCompressed FROM BULLHORN1.BH_UserMessage WHERE ' + CONDITION + 'order by userMessageID OFFSET ' + str(processedRowsCount) + ' ROWS FETCH NEXT ' + str(batchSize + rowsCountByBatch - processedRowsCount) + ' ROWS ONLY'
		freshStart = True
	print('\nProcessing batch #' + str(batchCount) + "\n")	
	cur.execute(sqlStatement)
	for msg in cur.fetchall(): #magic in the second parameter, use negative value for deflate format
		print("#" + str(processedRowsCount) + " => userMessageID = " + str(msg.userMessageID))
		decompressedContent = zlib.decompress(bytes(msg.commentsCompressed), -zlib.MAX_WBITS)
		#print(decompressedContent)
		#print(type(decompressedContent))
		#contentString = decompressedContent.decode('latin-1')
		#contentString = decompressedContent.decode('ISO-8859-1')
		#contentString = decompressedContent.decode('utf-8')
		#contentString = BeautifulSoup(decompressedContent,"html.parser") #<<<<<
		#print(contentString)
		#escapedContentString = contentString.text
		#print(escapedContentString)
		#emailcontent = re.sub("((?<![(,])'(?![,)]))", "''", escapedContentString)
		#emailcontent = re.sub('''(['"])''', r"""''""", escapedContentString)
		#emailcontent = re.sub('''(['"])''', r"""''""", escapedContentString)
		#emailcontent = re.sub('''(['"])''', r"""''""", contentString.text) #<<<<<
		try:
			contentString = BeautifulSoup(decompressedContent.decode('utf-8'),"html.parser")
			#print (contentString)
			emailcontent = re.sub('''(['"])''', r"""''""", contentString.text)
			#sqlStatement = 'insert into [dbo].[VCUserMessage] values(' + str(msg.userMessageID) + ', ' + """'""" + escapedContentString + "')"
			print ('>>>START<<<')
			#sqlStatement = "update BULLHORN1.BH_UserMessage set email_content = " + "Stuff('EMAIL: ' + '<br/>'                + Coalesce('From: ' + NULLIF(convert(nvarchar(max),externalFrom), '') + '<br/>', '')                + Coalesce('To: ' + NULLIF(convert(nvarchar(max),externalTo), '') + '<br/>', '')                + Coalesce('CC: ' + NULLIF(convert(nvarchar(max),externalCC), '') + '<br/>', '')                + Coalesce('BCC: ' + NULLIF(convert(nvarchar(max),externalBCC), '') + '<br/>', '')                + Coalesce('Subject: ' + NULLIF(convert(nvarchar(max),subject), '') + '<br/>', '')" + 			"+ Coalesce('Body: ' + NULLIF( replace( convert(nvarchar(max), [dbo].[udf_StripHTML]([dbo].[removeNullCharacters](N" + "'" + emailcontent + "'" + "))) ,char(10),'<br/>'), '') + '<br/>', 'Body: ' + '<br/>')" 			+ "+ Coalesce('Comments: ' + NULLIF( replace( convert(nvarchar(max),comments) ,char(10),'<br/>'), '') + '<br/>', '')              , 1, 0, '')" + " where userMessageID = " + str(msg.userMessageID)
			sqlStatement = "update BULLHORN1.BH_UserMessage set email_content = t.emailbody" + """
			  from (
			  select um.userMessageID
				   , [dbo].[RemoveNullCharacters]([dbo].[udf_StripHTML](replace(replace(replace(replace(replace(replace(
				   Stuff('EMAIL: ' + '<br/>'
                + Coalesce('From: ' + NULLIF(convert(nvarchar(max),um.externalFrom), '') + '<br/>', '')
                + Coalesce('To: ' + NULLIF(convert(nvarchar(max),um.externalTo), '') + '<br/>', '')
                + Coalesce('CC: ' + NULLIF(convert(nvarchar(max),um.externalCC), '') + '<br/>', '')
                + Coalesce('BCC: ' + NULLIF(convert(nvarchar(max),um.externalBCC), '') + '<br/>', '')
                + Coalesce('Subject: ' + NULLIF(convert(nvarchar(max),um.subject), '') + '<br/>', '')
                """ + "+ Coalesce('Body: ' + NULLIF( replace( trim(convert(nvarchar(max), N" + "'" + emailcontent + "'" + ")) ,char(10),'<br/>'), '') + '<br/>', 'Body: ' + '<br/>')" + """
				+ Coalesce('Comments: ' + NULLIF( replace( convert(nvarchar(max),um.comments) ,char(10),'<br/>'), '') + '<br/>', '')
				+ Coalesce('Attachments: ' + NULLIF(convert(nvarchar(max),umf.att), '') + '<br/>', '')
				, 1, 0, '')
				,'Â',''),'Â·',''),'v\:* {behavior:url(#default#VML);}',''),'o\:* {behavior:url(#default#VML);}',''),'w\:* {behavior:url(#default#VML);}',''),'.shape {behavior:url(#default#VML);}','')
				)) as emailbody
				from BULLHORN1.BH_UserMessage um
				left join (SELECT userMessageID, STRING_AGG(cast(name as nvarchar(max)),',' ) WITHIN GROUP (ORDER BY name) att from bullhorn1.BH_UserMessageFile GROUP BY userMessageID) umf on umf.userMessageID = um.userMessageID
				where um.isSenderDeleted = 0
				) t""" + " where t.userMessageID = BULLHORN1.BH_UserMessage.userMessageID and BULLHORN1.BH_UserMessage.userMessageID = " + str(msg.userMessageID)
			#print(sqlStatement)
			
			#sqlStatement = "update BULLHORN1.BH_UserMessage set email_content = " + "[dbo].[udf_StripHTML](N" + """'""" + emailcontent + "'" + ")" + " where userMessageID = " + str(msg.userMessageID)
			cur.execute(sqlStatement)
			
		except Exception as ex:
			print(ex)
			try:
				contentString = BeautifulSoup(decompressedContent,"html.parser")
				#print(contentString0)
				emailcontent = re.sub('''(['"])''', r"""''""", contentString.text)

				print ('>>>EXCEPTION1<<<')
				#sqlStatement = "update BULLHORN1.BH_UserMessage set email_content = " + "[dbo].[udf_StripHTML]([dbo].[removeNullCharacters](N" + """'""" + emailcontent + "'" + "))" + " where userMessageID = " + str(msg.userMessageID)
				sqlStatement = "update BULLHORN1.BH_UserMessage set email_content = t.emailbody" + """
			  from (
			  select um.userMessageID
				   , [dbo].[RemoveNullCharacters]([dbo].[udf_StripHTML](replace(replace(replace(replace(replace(replace(
				   Stuff('EMAIL: ' + '<br/>'
                + Coalesce('From: ' + NULLIF(convert(nvarchar(max),um.externalFrom), '') + '<br/>', '')
                + Coalesce('To: ' + NULLIF(convert(nvarchar(max),um.externalTo), '') + '<br/>', '')
                + Coalesce('CC: ' + NULLIF(convert(nvarchar(max),um.externalCC), '') + '<br/>', '')
                + Coalesce('BCC: ' + NULLIF(convert(nvarchar(max),um.externalBCC), '') + '<br/>', '')
                + Coalesce('Subject: ' + NULLIF(convert(nvarchar(max),um.subject), '') + '<br/>', '')
                """ + "+ Coalesce('Body: ' + NULLIF( replace( trim(convert(nvarchar(max), N" + "'" + emailcontent + "'" + ")) ,char(10),'<br/>'), '') + '<br/>', 'Body: ' + '<br/>')" + """
				+ Coalesce('Comments: ' + NULLIF( replace( convert(nvarchar(max),um.comments) ,char(10),'<br/>'), '') + '<br/>', '')
				+ Coalesce('Attachments: ' + NULLIF(convert(nvarchar(max),umf.att), '') + '<br/>', '')
				, 1, 0, '')
				,'Â',''),'Â·',''),'v\:* {behavior:url(#default#VML);}',''),'o\:* {behavior:url(#default#VML);}',''),'w\:* {behavior:url(#default#VML);}',''),'.shape {behavior:url(#default#VML);}','')
				)) as emailbody
				from BULLHORN1.BH_UserMessage um
				left join (SELECT userMessageID, STRING_AGG(cast(name as nvarchar(max)),',' ) WITHIN GROUP (ORDER BY name) att from bullhorn1.BH_UserMessageFile GROUP BY userMessageID) umf on umf.userMessageID = um.userMessageID
				where um.isSenderDeleted = 0
				) t""" + " where t.userMessageID = BULLHORN1.BH_UserMessage.userMessageID and BULLHORN1.BH_UserMessage.userMessageID = " + str(msg.userMessageID)
				#print(sqlStatement)

				#sqlStatement = "update BULLHORN1.BH_UserMessage set email_content = " + "[dbo].[udf_StripHTML](N" + """'""" + emailcontent + "'" + ")" + " where userMessageID = " + str(msg.userMessageID)
				cur.execute(sqlStatement)
				
			except Exception as ex:
				print(ex)
				#print(sql)
				#pass
				try:
					emailcontent = re.sub('''(['"])''', r"""''""", decompressedContent)

					print ('>>>EXCEPTION2<<<')
					#sqlStatement = "update BULLHORN1.BH_UserMessage set email_content = " + "[dbo].[udf_StripHTML]([dbo].[removeNullCharacters](N" + """'""" + emailcontent + "'" + "))" + " where userMessageID = " + str(msg.userMessageID)
					sqlStatement = "update BULLHORN1.BH_UserMessage set email_content = t.emailbody" + """
			  from (
			  select um.userMessageID
				   , [dbo].[RemoveNullCharacters]([dbo].[udf_StripHTML](replace(replace(replace(replace(replace(replace(
				   Stuff('EMAIL: ' + '<br/>'
                + Coalesce('From: ' + NULLIF(convert(nvarchar(max),um.externalFrom), '') + '<br/>', '')
                + Coalesce('To: ' + NULLIF(convert(nvarchar(max),um.externalTo), '') + '<br/>', '')
                + Coalesce('CC: ' + NULLIF(convert(nvarchar(max),um.externalCC), '') + '<br/>', '')
                + Coalesce('BCC: ' + NULLIF(convert(nvarchar(max),um.externalBCC), '') + '<br/>', '')
                + Coalesce('Subject: ' + NULLIF(convert(nvarchar(max),um.subject), '') + '<br/>', '')
                """ + "+ Coalesce('Body: ' + NULLIF( replace( trim(convert(nvarchar(max), N" + "'" + emailcontent + "'" + ")) ,char(10),'<br/>'), '') + '<br/>', 'Body: ' + '<br/>')" + """
				+ Coalesce('Comments: ' + NULLIF( replace( convert(nvarchar(max),um.comments) ,char(10),'<br/>'), '') + '<br/>', '')
				+ Coalesce('Attachments: ' + NULLIF(convert(nvarchar(max),umf.att), '') + '<br/>', '')
				, 1, 0, '')
				,'Â',''),'Â·',''),'v\:* {behavior:url(#default#VML);}',''),'o\:* {behavior:url(#default#VML);}',''),'w\:* {behavior:url(#default#VML);}',''),'.shape {behavior:url(#default#VML);}','')
				)) as emailbody
				from BULLHORN1.BH_UserMessage um
				left join (SELECT userMessageID, STRING_AGG(cast(name as nvarchar(max)),',' ) WITHIN GROUP (ORDER BY name) att from bullhorn1.BH_UserMessageFile GROUP BY userMessageID) umf on umf.userMessageID = um.userMessageID
				where um.isSenderDeleted = 0
				) t""" + " where t.userMessageID = BULLHORN1.BH_UserMessage.userMessageID and BULLHORN1.BH_UserMessage.userMessageID = " + str(msg.userMessageID)
					#print(sqlStatement)

					#sqlStatement = "update BULLHORN1.BH_UserMessage set email_content = " + "[dbo].[udf_StripHTML](N" + """'""" + emailcontent + "'" + ")" + " where userMessageID = " + str(msg.userMessageID)
					cur.execute(sqlStatement)
					
				except Exception as ex:
					print(ex)
					#print(sql)
					pass
		processedRowsCount += 1
		#print("Processing row #" + str(processedRowsCount) + " => userMessageID = " + str(msg.userMessageID))
		#cur.execute(sqlStatement)
	print('\nProcessed ' + str(processedRowsCount) + ' rows')
	print('\n')
	rowsCountByBatch += batchSize
#cn.commit()

# close connection
cur.close()
del cur
cn.close()
endTime = datetime.datetime.now()
print("\nEnd: " + str(endTime))
elaTime = endTime - startTime
print("Elapsed: " + str(elaTime))
