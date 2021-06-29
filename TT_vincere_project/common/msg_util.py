import extract_msg
import glob
import imaplib
import email
import email.parser
from email.parser import HeaderParser
import re
import pathlib
import os
import win32com.client

def convert_msg_to_text(msg_file_path):
    try:
        msg = extract_msg.Message(msg_file_path)
        msg_from = 'From: {}'.format(msg.sender)
        msg_to = 'To: {}'.format(msg.to)
        msg_cc = 'Cc: {}'.format(msg.cc) if msg.cc else None
        msg_date = 'Date: {}'.format(msg.date)
        msg_subj = 'Subject: {}'.format(msg.subject)
        msg_message = '\n{}'.format(msg.body)
        msg_attachments_note = ''
        if msg.attachments:
            msg_attachments = msg.attachments
            msg_attachments_note = 'Attachments: {}'.format(', '.join(f.longFilename for f in msg_attachments))
        return '{}\n{}\n{}\n{}\n{}\n{}\n{}'.format(msg_from, msg_subj, msg_to, msg_cc, msg_date, msg_attachments_note, msg_message)
    except Exception as ex:
        print('{} {}'.format(msg_file_path, ex))


def convert_msg_to_text1(msg_file_path, attachement_folder_download, msgid):
    try:
        outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
        print("Parsing {}".format(msg_file_path))
        msg = outlook.OpenSharedItem(msg_file_path)

        # msg_from = 'From: {}'.format(msg.SenderName)
        msg_from = 'From: {}'.format('%s%s' % (msg.SenderName, ' <%s>' % msg.SenderEmailAddress if '@' in msg.SenderEmailAddress else ''))

        # msg_to = 'To: {}'.format(msg.To)
        msg_to = 'To: {}'.format('; '.join(['%s%s' % (r.Name, ' <%s>' % r.Address if '@' in r.Address else '') for r in msg.Recipients]))
        msg_cc = 'Cc: {}'.format(msg.CC) # if msg.CC else None
        msg_date = 'Date: {}'.format(msg.SentOn)
        msg_subj = 'Subject: {}'.format(msg.Subject)
        msg_message = '\n{}'.format(msg.Body)
        msg_attachments_note = ''
        count_attachments = msg.Attachments.Count if attachement_folder_download is not None else 0
        if count_attachments > 0:
            attch_folder_by_msgid = os.path.join(attachement_folder_download, msgid)
            pathlib.Path(attch_folder_by_msgid).mkdir(parents=True, exist_ok=True)
            try:
                msg_attachments_note = ', '.join(['{}_{}'.format(msgid, msg.Attachments.Item(item + 1).Filename) for item in range(count_attachments)])
            except Exception as _:
                pass
            for att in msg.Attachments:
                try:
                    att.SaveAsFile(os.path.join(attch_folder_by_msgid, '{}_{}'.format(msgid, att.Filename)))
                except Exception as _:
                    pass
        if msg_attachments_note != '':
            msg_attachments_note = 'Attachment: {}'.format(msg_attachments_note)
        del (msg)
        del (outlook)
        return '{}\n{}\n{}\n{}\n{}\n{}\n{}'.format(msg_from, msg_subj, msg_to, msg_cc, msg_date, msg_attachments_note, msg_message)
    except Exception as ex:
        print('msg_file_path: {}, exception: {}'.format(msg_file_path, ex))
        try:
            if 'msg' in locals() or 'msg' in globals():
                del (msg)
            if 'outlook' in locals() or 'outlook' in globals():
                del (outlook)
        except Exception as ex2:
            pass


# %% testing
# convert_msg_to_text(r'D:\vincere\Fwd FW Klickto - Database Filter.msg')
if False:
    # msg_header = msg.header
    # msg_from = re.search(r'From:[ a-zA-Z"0-9_,]*<[a-zA-Z0-9._]*@\w*.\w*.', msg_header._payload).group()

    msg = extract_msg.Message(r'E:\VC_Apis_PROD\ITRISFILES\ITRISDOCS\APPLICANT\A\1000OutlookMessageFile.msg')


    msg_from = 'From: {}'.format(msg.sender)
    msg_to = 'To: {}'.format(msg.to)
    msg_cc = 'Cc: {}'.format(msg.cc) if msg.cc else None
    msg_date = 'Date: {}'.format(msg.date)
    msg_subj = 'Subject: {}'.format(msg.subject)
    msg_message = '\n{}'.format(msg.body)
    msg_attachments_note = None

    if msg.attachments:
        msg_attachments = msg.attachments
        msg_attachments_note = 'Attachments: {}'.format(', '.join(f.longFilename for f in msg_attachments))

    file = msg.attachments[0]
    with open(file.longFilename, 'wb') as f:
        f.write(file.data)

    mailFile = open(r'd:\vincere\data_output\apis\data_input\physicalfile\Applicant\V\115OutlookMessageFile.msg')
    p = email.parser.Parser()
    msg = p.parse(mailFile)
    email.message_from_file(mailFile)

    outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
    msg = outlook.OpenSharedItem(r"d:\vincere\data_output\apis\data_input\physicalfile\Applicant\V\119OutlookMessageFile.msg")

    print(msg.SenderName)
    print(msg.Sender)
    print(msg.SentOn)
    print(msg.To)
    print(msg.CC)
    print(msg.Subject)
    print(msg.Body)

    if msg.Class == 43:
        if msg.SenderEmailType == 'EX':
            print
            msg.Sender.GetExchangeUser().PrimarySmtpAddress
            msg.Sender.EmailAddress
            msg.Sender.GetExchangeUser().AddressEntry.PrimarySmtpAddress
            msg.sender.GetExchangeUser()
            msg.sender.AddressEntryUserType
            msg.sender.GetExchangeUser().PrimarySmtpAddress
        else:
            print
            msg.SenderEmailAddress
        del (outlook, msg)

        recipients = msg.Recipients
        for r in recipients:
            print(r)
            print(r.AddressEntry.GetExchangeUser().PrimarySmtpAddress)


    import msg_parser.MsOxMessage

    json_string = msg_obj.get_message_as_json()

    msg_properties_dict = msg_obj.get_properties()

    import mailparser


    mail = mailparser.parse_from_file_msg(r'd:\vincere\data_output\apis\data_input\physicalfile\Applicant\V\140OutlookMessageFile.msg')


# %% test 2
if False:
    outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
    msg = outlook.OpenSharedItem(r'd:\vincere\data_output\apis\data_input\physicalfile\Applicant\V\14.msg')
    msg = outlook.OpenSharedItem(r'd:\vincere\data_output\apis\data_input\physicalfile\Applicant\V\140OutlookMessageFile.msg')

    msg_from = 'From: {}'.format(msg.SenderName)
    '%s%s' % (msg.SenderName, ' <%s>' % msg.SenderEmailAddress if '@' in msg.SenderEmailAddress else '')
    # msg.Sender.Name
    # msg.Sender.Address
    msg_to = 'To: {}'.format(msg.To)

    '; '.join(['%s%s' % (r.Name, ' <%s>' % r.Address if '@' in r.Address else '') for r in msg.Recipients])

    msg.Recipients[1].Name
    msg.Recipients[1].Address

    msg_cc = 'Cc: {}'.format(msg.CC)  # if msg.CC else None
    msg.Cc

    msg_date = 'Date: {}'.format(msg.SentOn)
    msg_subj = 'Subject: {}'.format(msg.Subject)
    msg_message = '\n{}'.format(msg.Body)
    msg_attachments_note = ''
    count_attachments = msg.Attachments.Count
    if count_attachments > 0:
        attch_folder_by_msgid = os.path.join(attachement_folder_download, msgid)
        pathlib.Path(attch_folder_by_msgid).mkdir(parents=True, exist_ok=True)
        msg_attachments_note = ', '.join(['{}_{}'.format(msgid, msg.Attachments.Item(item + 1).Filename) for item in range(count_attachments)])
        for att in msg.Attachments:
            att.SaveAsFile(os.path.join(attch_folder_by_msgid, '{}_{}'.format(msgid, att.Filename)))
    if msg_attachments_note != '':
        msg_attachments_note = 'Attachment: {}'.format(msg_attachments_note)

    print( '{}\n{}\n{}\n{}\n{}\n{}\n{}'.format(msg_from, msg_subj, msg_to, msg_cc, msg_date, msg_attachments_note, msg_message))


if False:
    import email.parser
    import os, sys

    mailFile = open(r'E:\VC_Apis_PROD\ITRISFILES\ITRISDOCS\APPLICANT\A\1000OutlookMessageFile.msg', "rb")
    p = email.parser.BytesParser()
    msg = p.parsebytes(mailFile.read())
    mailFile.close()

    partCounter = 1
    for part in msg.walk():
        if part.get_content_maintype() == "multipart":
            continue
        name = part.get_param("name")
        if name == None:
            name = "part-%i" % partCounter
        partCounter += 1
        # In real life, make sure that name is a reasonable filename
        # for your OS; otherwise, mangle it until it is!
        f = open(name, "wb")
        f.write(part.get_payload(decode=1))
        f.close()
        print(name)


if False:
    import email
    email.message_from_bytes(open(r'E:\VC_Apis_PROD\ITRISFILES\ITRISDOCS\APPLICANT\A\1000OutlookMessageFile.msg', "rb"))

    from email.message import EmailMessage

    msg = EmailMessage(r'E:\VC_Apis_PROD\ITRISFILES\ITRISDOCS\APPLICANT\A\1000OutlookMessageFile.msg')
    msg_body = msg.get_body()