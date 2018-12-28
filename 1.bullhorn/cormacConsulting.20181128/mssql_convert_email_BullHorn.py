#!/usr/bin/python3

import pyodbc
import zlib

#cn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=dmp.vinceredev.com;DATABASE=ro2;UID=sa;PWD=123$%^qwe',autocommit=True)
cn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=dmpfra.vinceredev.com;DATABASE=ro2;UID=sa;PWD=123$%^qwe',autocommit=True)

cursor = cn.cursor()
cursor.execute('alter table BULLHORN1.BH_UserMessage drop column email_content; ')
cursor.execute('alter table BULLHORN1.BH_UserMessage add email_content varchar(max); ')
cursor.execute('SELECT userMessageID, commentsCompressed FROM BULLHORN1.BH_UserMessage WHERE DATALENGTH(commentsCompressed) > 0 ')
#cursor.execute('SELECT userMessageID, commentsCompressed FROM BULLHORN1.BH_UserMessage WHERE DATALENGTH(commentsCompressed) > 0 and userMessageID in (10) ')

for msg in cursor.fetchall(): #magic in the second parameter, use negative value for deflate format
    decompressedMessageBody = zlib.decompress(bytes(msg.commentsCompressed), -zlib.MAX_WBITS)
    print(msg.userMessageID)
    #print(decompressedMessageBody)
    cursor.execute("update BULLHORN1.BH_UserMessage set email_content = ? where userMessageID=?", decompressedMessageBody.decode('latin-1'), msg.userMessageID)
    #cursor.execute("update BULLHORN1.BH_UserMessage set comments = ? where userMessageID=?", decompressedMessageBody.decode('ISO-8859-1'), msg.userMessageID)
    #cursor.execute("update BULLHORN1.BH_UserMessage set comments = ? where userMessageID=?", decompressedMessageBody.decode('utf-8'), msg.userMessageID)
    cursor.execute("update BULLHORN1.BH_UserMessage set email_content = [dbo].[udf_StripHTML](email_content);")
    #break
#cn.commit()
cn.close()

