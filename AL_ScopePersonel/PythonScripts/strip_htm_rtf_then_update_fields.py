#!/usr/bin/python3

import sys
import datetime
import pyodbc
#import zlib
import striprtf
from html.parser import HTMLParser

class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.strict = False
        self.convert_charrefs= True
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()

def execute_sql_script_from_file(cur, scriptPath):
	sqlQuery = ''
	with open(scriptPath, 'r') as inp:
		for line in inp:
			if line == 'GO\n':
				cur.execute(sqlQuery)
				sqlQuery = ''
			elif 'PRINT' in line:
				#disp = line.split("'")[1]
				#print(disp, '\r')
				print(line)
			else:
				sqlQuery = sqlQuery + line
	inp.close()

def get_elapsed_time_tring(start, end):
	hours, rem = divmod(end - start, 3600)
	minutes, seconds = divmod(rem, 60)
	retVal = "{:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds)
	return retVal
#######################################################################################################################################################
## Begin script
#######################################################################################################################################################
usage = 'python script-name.py N (with N is number of processed rows)'
freshStart = True
processedRowsCount = 0
#136258
if len(sys.argv) > 1:
	if int(sys.argv[1]) < 1:
		print(usage)
		exit()
	freshStart = False
	processedRowsCount = int(sys.argv[1])

startTime = datetime.datetime.now()
print("Starting time: " + str(startTime))

#cn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=EISG;UID=sa;PWD=Olala3334',autocommit=True)
cn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=scope;UID=sa;PWD=123$%^qwe',autocommit=True)
#cn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=dmpfra.vinceredev.com;DATABASE=scope;UID=sa;PWD=123$%^qwe',autocommit=True)
#cn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=DESKTOP-VANR3JG\MSSQLDEV2017;DATABASE=scope;UID=sa;PWD=Olala3334',autocommit=True)
cur = cn.cursor()
##################

#tableToParse = '[dbo].[VC_ComActivityCommentNotebook]'
#tableToParse = '[dbo].[VC_ConActivityCommentNotebook]'
#tableToParse = '[dbo].[VC_CanActivityCommentNotebook]'
tableToParse = '[dbo].[VC_JobActivityCommentNotebook]'

columnToParse = '[Memo]'

idColumn = '[NotebookItemId]'

indicatorColumn1 = '[FileExtension]'

#tableToUpdate = '[dbo].[VC_ComActivityCommentNotebook]'
#tableToUpdate = '[dbo].[VC_ConActivityCommentNotebook]'
#tableToUpdate = '[dbo].[VC_CanActivityCommentNotebook]'
tableToUpdate = '[dbo].[VC_JobActivityCommentNotebook]'

columnToUpdate = '[NoteContent]'

sqlHtmlToTxtFunction = '[dbo].[ufn_ConvertHTMLToText]'
# get the total of user message that need to be processed
sqlStatement = "SELECT count(" + idColumn + ") FROM " + tableToParse + " WHERE LEN(TRIM(ISNULL(" + columnToParse + ", ''))) > 0"
##4debug1
#print(sqlStatement)

print("\nGetting the total of data rows that need to be processed. It may take few minutes to complete.")
cur.execute(sqlStatement)
total = cur.fetchone()[0]
print("\nThe total messages that need to be processed is: " + str(total))
# Using code below in case of wanting to create middle table for just storing parsing results, but not update back to origin table
#if freshStart == True:
	#print('\nCreating table VCUserMessage')
	# Drop table VCUserMessage if exists
	#sqlStatement = "DROP TABLE IF EXISTS [dbo].[VCUserMessage]"
	#cur.execute(sqlStatement)
	# Create table VCUserMessage
	#sqlStatement = '''CREATE TABLE [dbo].[VCUserMessage] (
	#	ID int NOT NULL PRIMARY KEY,
	#	Message varchar(max) NULL
	#)'''
	#cur.execute(sqlStatement)
## go through each batch, in which, for each row, de-compress the message, strip html tags, then insert into VCUserMessage table
batchSize = 1000
batchCount = 0
rowsCountByBatch = 0
if freshStart == False:
	#re-calculate batchCount and rowsCountByBatch
	batchCount = processedRowsCount // batchSize + 1
	if processedRowsCount % batchSize == 0:
		
		rowsCountByBatch = batchSize * (batchCount)
	else:
		rowsCountByBatch = batchSize * (batchCount - 1)
print('\nGo through each batch, in which, for each row, strip html tags or parse rtf to text base on file extension, then update to' + tableToUpdate + '.' + columnToUpdate + '\n')
while rowsCountByBatch < total:
	if freshStart == True:
		batchCount += 1
	if freshStart == True:
		sqlStatement = 'SELECT ' + idColumn + ', ' + indicatorColumn1 + ', TRIM(ISNULL(' + columnToParse + ", '')) as " + columnToParse + ' FROM ' + tableToParse + " WHERE LEN(TRIM(ISNULL(" + columnToParse + ", ''))) > 0 order by " + idColumn + ' OFFSET ' + str(rowsCountByBatch) + ' ROWS FETCH NEXT ' + str(batchSize) + ' ROWS ONLY'
	else:
		sqlStatement = 'SELECT ' + idColumn + ', ' + indicatorColumn1 + ', TRIM(ISNULL(' + columnToParse + ", '')) as " + columnToParse + ' FROM ' + tableToParse + " WHERE LEN(TRIM(ISNULL(" + columnToParse + ", ''))) > 0 order by " + idColumn + ' OFFSET ' + str(processedRowsCount) + ' ROWS FETCH NEXT ' + str(batchSize + rowsCountByBatch - processedRowsCount) + ' ROWS ONLY'
		freshStart = True
	print('\nProcessing batch #' + str(batchCount) + "\n")	
	cur.execute(sqlStatement)
	for r in cur.fetchall():
		#magic in the second parameter, use negative value for deflate format
		#Using code below in case of needed to de-compress input that was compressed by zlib
		#decompressedContent = zlib.decompress(bytes(r.commentsCompressed), -zlib.MAX_WBITS)
		#contentString = decompressedContent.decode('latin-1')
		#contentString = decompressedContent.decode('ISO-8859-1')
		#contentString = decompressedContent.decode('utf-8')
		#escapedContentString = strip_tags(contentString).translate(str.maketrans({"'":  r"''"}))
		#sqlStatement = 'insert into [dbo].[VCUserMessage] values(' + str(r.' + idColumn + ') + ', ' + """
		contentString = str(getattr(r, columnToParse.strip('][')))
		parsedContentString = contentString
		indicator1 = str(getattr(r, indicatorColumn1.strip('][')))
		isrtf = False
		if indicator1 == '.html':
			isrtf = False
		elif indicator1 == '.rtf':
			isrtf = True
		else:
			if contentString.startswith( '{\rtf', 0, 49 ) == True:
				isrtf = True
		if isrtf == True:
			parsedContentString = striprtf.rtf_to_text(contentString).translate(str.maketrans({"'":  r"''"}))
			sqlStatement = "update " + tableToUpdate + " set " + columnToUpdate + " = '" + parsedContentString + "' where " + idColumn + ' = ' + str(getattr(r, idColumn.strip('][')))
		else:
			parsedContentString = strip_tags(contentString).translate(str.maketrans({"'":  r"''"}))
			sqlStatement = "update " + tableToUpdate + " set " + columnToUpdate + " = " + sqlHtmlToTxtFunction  + "('" + parsedContentString + "') where " + idColumn + ' = ' + str(getattr(r, idColumn.strip('][')))
		processedRowsCount += 1
		print("Processing row #" + str(processedRowsCount) + " => " + idColumn + " = " + str(getattr(r, idColumn.strip(']['))))
		##4debug2
		#print(sqlStatement)
		cur.execute(sqlStatement)
	rowsCountByBatch += batchSize
	print('\nProcessed ' + str(processedRowsCount) + ' rows')
#cn.commit()
# close connection
cur.close()
del cur
cn.close()
endTime = datetime.datetime.now()
print("\nEnding time: " + str(endTime))
elaTime = endTime - startTime
print("\nElapsed time: " + str(elaTime))