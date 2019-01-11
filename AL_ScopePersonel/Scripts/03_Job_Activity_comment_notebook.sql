declare @dumyNoteContent nvarchar(max) = ''
--1	APP	Applicant
--2	CLNT	Client
--3	CCT	Contact
--4	PER	Person
--declare @objType int = 2

drop table if exists VC_JobActivityCommentNotebook

select
--top 1000
a.NotebookItemId
, c.NotebookType
, d.FolderName
, a.Subject
, g.LinkType
, a.CreatedOn
, b.CreatedBy
, iif(h.ObjectTypeId = 2, f.ObjectId, null) as ComObjId
, iif(h.ObjectTypeId = 3, f.ObjectId, null) as ConObjId
, iif(h.ObjectTypeId = 1, f.ObjectId, null) as CanObjId
, f.JobId
, f.ClientID as ComId
, e.FileExtension
, e.Memo
, @dumyNoteContent as NoteContent

into VC_JobActivityCommentNotebook

from (select NotebookItemId, NotebookTypeId, NotebookFolderId, Subject, CreatedUserId, CreatedOn from NotebookItems) a
left join (
	select UserId
	, lower(dbo.ufn_TrimSpecialCharacters_V2(isnull(nullif(trim(isnull(EmailAddress, '')), 'admin@company.com'), 'freddie@scopepersonnel.co.uk'), '')) CreatedBy
	from Users
) b on a.CreatedUserId = b.UserId
left join (select NotebookTypeId, NotebookType from NotebookTypes) c on a.NotebookTypeId = c.NotebookTypeId
left join (select NotebookFolderId, FolderName from NotebookFolders) d on a.NotebookFolderId = d.NotebookFolderId
left join (select NotebookItemId, Memo, FileExtension from NotebookItemContent) e on a.NotebookItemId = e.NotebookItemId
left join (select NotebookItemId, NotebookLinkTypeId, ObjectId, JobId, ClientID from NotebookLinks) f on a.NotebookItemId = f.NotebookItemId
left join (select NotebookLinkTypeId, Description as LinkType from NotebookLinkTypes) g on f.NotebookLinkTypeId = g.NotebookLinkTypeId
left join (select ObjectId, ObjectTypeId from Objects) h on f.ObjectId = h.ObjectID
--where h.ObjectTypeId = @objType
where f.JobId is not null
-- just fetchs notebook items that were created 3 months recently
-- TODO: comment this where condition when running script for Production data
and CreatedOn >= dateadd(month, -3, getdate())
------------------------------------------------------------------------------------------------------------------------

select * from VC_JobActivityCommentNotebook

----select top 100 * from VC_ComIdx
--select * from Tasks

--select
--TaskId
--, Notes as originNote
--, [dbo].[ufn_ParseRTF](Notes) as Notes
-- from TaskNotes

-- --{\rtf1\adeflang1025\ansi\ansicpg1252\uc1\adeff31507\deff0\stshfdbch31506\stshfloch31506\stshfhich31506\stshfbi31507\deflang2057\deflangfe2057\themelang2057\themelangfe0\themelangcs0{\fonttbl{\f0\fbidi \froman\fcharset0\fprq2{\*\panose 02020603050405020304}Times New Roman;}{\f34\fbidi \froman\fcharset0\fprq2{\*\panose 02040503050406030204}Cambria Math;}{\f37\fbidi \fswiss\fcharset0\fprq2{\*\panose 020f0502020204030204}Calibri;}{\f31500\fbidi \froman\fcharset0\fprq2{\*\panose 02020603050405020304}Times New Roman;}{\f31501\fbidi \froman\fcharset0\fprq2{\*\panose 02020603050405020304}Times New Roman;}{\f31502\fbidi \fswiss\fcharset0\fprq2{\*\panose 020f0302020204030204}Calibri Light;}{\f31503\fbidi \froman\fcharset0\fprq2{\*\panose 02020603050405020304}Times New Roman;}{\f31504\fbidi \froman\fcharset0\fprq2{\*\panose 02020603050405020304}Times New Roman;}{\f31505\fbidi \froman\fcharset0\fprq2{\*\panose 02020603050405020304}Times New Roman;}{\f31506\fbidi \fswiss\fcharset0\fprq2{\*\panose 020f0502020204030204}Calibri;}{\f31507\fbidi \froman\fcharset0\fprq2{\*\panose 02020603050405020304}Times New Roman;}{\f291\fbidi \froman\fcharset238\fprq2 Times New Roman CE;}{\f292\fbidi \froman\fcharset204\fprq2 Times New Roman Cyr;}{\f294\fbidi \froman\fcharset161\fprq2 Times New Roman Greek;}{\f295\fbidi \froman\fcharset162\fprq2 Times New Roman Tur;}{\f296\fbidi \froman\fcharset177\fprq2 Times New Roman (Hebrew);}{\f297\fbidi \froman\fcharset178\fprq2 Times New Roman (Arabic);}{\f298\fbidi \froman\fcharset186\fprq2 Times New Roman Baltic;}{\f299\fbidi \froman\fcharset163\fprq2 Times New Roman (Vietnamese);}{\f631\fbidi \froman\fcharset238\fprq2 Cambria Math CE;}{\f632\fbidi \froman\fcharset204\fprq2 Cambria Math Cyr;}{\f634\fbidi \froman\fcharset161\fprq2 Cambria Math Greek;}{\f635\fbidi \froman\fcharset162\fprq2 Cambria Math Tur;}{\f638\fbidi \froman\fcharset186\fprq2 Cambria Math Baltic;}{\f639\fbidi \froman\fcharset163\fprq2 Cambria Math (Vietnamese);}{\f661\fbidi \fswiss\fcharset238\fprq2 Calibri CE;}{\f662\fbidi \fswiss\fcharset204\fprq2 Calibri Cyr;}{\f664\fbidi \fswiss\fcharset161\fprq2 Calibri Greek;}{\f665\fbidi \fswiss\fcharset162\fprq2 Calibri Tur;}{\f668\fbidi \fswiss\fcharset186\fprq2 Calibri Baltic;}{\f669\fbidi \fswiss\fcharset163\fprq2 Calibri (Vietnamese);}{\f31508\fbidi \froman\fcharset238\fprq2 Times New Roman CE;}{\f31509\fbidi \froman\fcharset204\fprq2 Times New Roman Cyr;}{\f31511\fbidi \froman\fcharset161\fprq2 Times New Roman Greek;}{\f31512\fbidi \froman\fcharset162\fprq2 Times New Roman Tur;}{\f31513\fbidi \froman\fcharset177\fprq2 Times New Roman (Hebrew);}{\f31514\fbidi \froman\fcharset178\fprq2 Times New Roman (Arabic);}{\f31515\fbidi \froman\fcharset186\fprq2 Times New Roman Baltic;}{\f31516\fbidi \froman\fcharset163\fprq2 Times New Roman (Vietnamese);}{\f31518\fbidi \froman\fcharset238\fprq2 Times New Roman CE;}{\f31519\fbidi \froman\fcharset204\fprq2 Times New Roman Cyr;}{\f31521\fbidi \froman\fcharset161\fprq2 Times New Roman Greek;}{\f31522\fbidi \froman\fcharset162\fprq2 Times New Roman Tur;}{\f31523\fbidi \froman\fcharset177\fprq2 Times New Roman (Hebrew);}{\f31524\fbidi \froman\fcharset178\fprq2 Times New Roman (Arabic);}{\f31525\fbidi \froman\fcharset186\fprq2 Times New Roman Baltic;}{\f31526\fbidi \froman\fcharset163\fprq2 Times New Roman (Vietnamese);}{\f31528\fbidi \fswiss\fcharset238\fprq2 Calibri Light CE;}{\f31529\fbidi \fswiss\fcharset204\fprq2 Calibri Light Cyr;}{\f31531\fbidi \fswiss\fcharset161\fprq2 Calibri Light Greek;}{\f31532\fbidi \fswiss\fcharset162\fprq2 Calibri Light Tur;}{\f31535\fbidi \fswiss\fcharset186\fprq2 Calibri Light Baltic;}{\f31536\fbidi \fswiss\fcharset163\fprq2 Calibri Light (Vietnamese);}{\f31538\fbidi \froman\fcharset238\fprq2 Times New Roman CE;}{\f31539\fbidi \froman\fcharset204\fprq2 Times New Roman Cyr;}{\f31541\fbidi \froman\fcharset161\fprq2 Times New Roman Greek;}{\f31542\fbidi \froman\fcharset162\fprq2 Times New Roman Tur;}{\f31543\fbidi \froman\fcharset177\fprq2 Times New Roman (Hebrew);}{\f31544\fbidi \froman\fcharset178\fprq2 Times New Roman (Arabic);}{\f31545\fbidi \froman\fcharset186\fprq2 Times New Roman Baltic;}{\f31546\fbidi \froman\fcharset163\fprq2 Times New Roman (Vietnamese);}{\f31548\fbidi \froman\fcharset238\fprq2 Times New Roman CE;}{\f31549\fbidi \froman\fcharset204\fprq2 Times New Roman Cyr;}{\f31551\fbidi \froman\fcharset161\fprq2 Times New Roman Greek;}{\f31552\fbidi \froman\fcharset162\fprq2 Times New Roman Tur;}{\f31553\fbidi \froman\fcharset177\fprq2 Times New Roman (Hebrew);}{\f31554\fbidi \froman\fcharset178\fprq2 Times New Roman (Arabic);}{\f31555\fbidi \froman\fcharset186\fprq2 Times New Roman Baltic;}{\f31556\fbidi \froman\fcharset163\fprq2 Times New Roman (Vietnamese);}{\f31558\fbidi \froman\fcharset238\fprq2 Times New Roman CE;}{\f31559\fbidi \froman\fcharset204\fprq2 Times New Roman Cyr;}{\f31561\fbidi \froman\fcharset161\fprq2 Times New Roman Greek;}{\f31562\fbidi \froman\fcharset162\fprq2 Times New Roman Tur;}{\f31563\fbidi \froman\fcharset177\fprq2 Times New Roman (Hebrew);}{\f31564\fbidi \froman\fcharset178\fprq2 Times New Roman (Arabic);}{\f31565\fbidi \froman\fcharset186\fprq2 Times New Roman Baltic;}{\f31566\fbidi \froman\fcharset163\fprq2 Times New Roman (Vietnamese);}{\f31568\fbidi \fswiss\fcharset238\fprq2 Calibri CE;}{\f31569\fbidi \fswiss\fcharset204\fprq2 Calibri Cyr;}{\f31571\fbidi \fswiss\fcharset161\fprq2 Calibri Greek;}{\f31572\fbidi \fswiss\fcharset162\fprq2 Calibri Tur;}{\f31575\fbidi \fswiss\fcharset186\fprq2 Calibri Baltic;}{\f31576\fbidi \fswiss\fcharset163\fprq2 Calibri (Vietnamese);}{\f31578\fbidi \froman\fcharset238\fprq2 Times New Roman CE;}{\f31579\fbidi \froman\fcharset204\fprq2 Times New Roman Cyr;}{\f31581\fbidi \froman\fcharset161\fprq2 Times New Roman Greek;}{\f31582\fbidi \froman\fcharset162\fprq2 Times New Roman Tur;}{\f31583\fbidi \froman\fcharset177\fprq2 Times New Roman (Hebrew);}{\f31584\fbidi \froman\fcharset178\fprq2 Times New Roman (Arabic);}{\f31585\fbidi \froman\fcharset186\fprq2 Times New Roman Baltic;}{\f31586\fbidi \froman\fcharset163\fprq2 Times New Roman (Vietnamese);}}{\colortbl;\red0\green0\blue0;\red0\green0\blue255;\red0\green255\blue255;\red0\green255\blue0;\red255\green0\blue255;\red255\green0\blue0;\red255\green255\blue0;\red255\green255\blue255;\red0\green0\blue128;\red0\green128\blue128;\red0\green128\blue0;\red128\green0\blue128;\red128\green0\blue0;\red128\green128\blue0;\red128\green128\blue128;\red192\green192\blue192;\red5\green99\blue193;\red149\green79\blue114;}{\*\defchp \f31506\fs22\lang2057\langfe1033\langfenp1033 }{\*\defpap \ql \li0\ri0\widctlpar\wrapdefault\aspalpha\aspnum\faauto\adjustright\rin0\lin0\itap0 }\noqfpromote {\stylesheet{\ql \li0\ri0\widctlpar\wrapdefault\aspalpha\aspnum\faauto\adjustright\rin0\lin0\itap0 \rtlch\fcs1 \af31507\afs22\alang1025 \ltrch\fcs0 \f31506\fs22\lang2057\langfe1033\cgrid\langnp2057\langfenp1033 \snext0 \sqformat \spriority0 Normal;}{\*\cs10 \additive \ssemihidden \sunhideused \spriority1 Default Paragraph Font;}{\*\ts11\tsrowd\trftsWidthB3\trpaddl108\trpaddr108\trpaddfl3\trpaddft3\trpaddfb3\trpaddfr3\tblind0\tblindtype3\tsvertalt\tsbrdrt\tsbrdrl\tsbrdrb\tsbrdrr\tsbrdrdgl\tsbrdrdgr\tsbrdrh\tsbrdrv \ql \li0\ri0\widctlpar\wrapdefault\aspalpha\aspnum\faauto\adjustright\rin0\lin0\itap0 \rtlch\fcs1 \af31507\afs22\alang1025 \ltrch\fcs0 \f31506\fs22\lang2057\langfe1033\cgrid\langnp2057\langfenp1033 \snext11 \ssemihidden \sunhideused Normal Table;}{\*\cs15 \additive \rtlch\fcs1 \af0 \ltrch\fcs0 \ul\cf17 \sbasedon10 \ssemihidden \sunhideused \styrsid7497393 Hyperlink;}{\*\cs16 \additive \rtlch\fcs1 \af0 \ltrch\fcs0 \ul\cf18 \sbasedon10 \ssemihidden \sunhideused \styrsid7497393 FollowedHyperlink;}{\*\cs17 \additive \rtlch\fcs1 \af31507\afs22 \ltrch\fcs0 \f31506\fs22\cf0 \sbasedon10 \ssemihidden \spriority0 \spersonal \scompose \styrsid7497393 EmailStyle17;}}{\*\revtbl {Unknown;}}{\*\rsidtbl \rsid199431\rsid7497393}{\mmathPr\mmathFont34\mbrkBin0\mbrkBinSub0\msmallFrac0\mdispDef1\mlMargin0\mrMargin0\mdefJc1\mwrapIndent1440\mintLim0\mnaryLim1}{\*\xmlnstbl {\xmlns1 http://schemas.microsoft.com/office/word/2003/wordml}}\paperw12240\paperh15840\margl1440\margr1440\margt1440\margb1440\gutter0\ltrsect \widowctrl\ftnbj\aenddoc\trackmoves0\trackformatting1\donotembedsysfont1\relyonvml0\donotembedlingdata0\grfdocevents0\validatexml1\showplaceholdtext0\ignoremixedcontent0\saveinvalidxml0\showxmlerrors1\noxlattoyen\expshrtn\noultrlspc\dntblnsbdb\nospaceforul\formshade\horzdoc\dgmargin\dghspace180\dgvspace180\dghorigin150\dgvorigin0\dghshow1\dgvshow1\jexpand\viewkind5\viewscale100\pgbrdrhead\pgbrdrfoot\splytwnine\ftnlytwnine\htmautsp\nolnhtadjtbl\useltbaln\alntblind\lytcalctblwd\lyttblrtgr\lnbrkrule\nobrkwrptbl\snaptogridincell\allowfieldendsel\wrppunct\asianbrkrule\newtblstyruls\nogrowautofit\usenormstyforlist\noindnmbrts\felnbrelev\nocxsptable\indrlsweleven\noafcnsttbl\afelev\utinl\hwelev\spltpgpar\notcvasp\notbrkcnstfrctbl\notvatxbx\krnprsnet\cachedcolbal \nouicompat \fet0{\*\wgrffmtfilter 2450}\nofeaturethrottle1\ilfomacatclnup0\ltrpar \sectd \ltrsect\linex0\endnhere\sectdefaultcl\sftnbj {\*\pnseclvl1\pnucrm\pnstart1\pnindent720\pnhang {\pntxta .}}{\*\pnseclvl2\pnucltr\pnstart1\pnindent720\pnhang {\pntxta .}}{\*\pnseclvl3\pndec\pnstart1\pnindent720\pnhang {\pntxta .}}{\*\pnseclvl4\pnlcltr\pnstart1\pnindent720\pnhang {\pntxta )}}{\*\pnseclvl5\pndec\pnstart1\pnindent720\pnhang {\pntxtb (}{\pntxta )}}{\*\pnseclvl6\pnlcltr\pnstart1\pnindent720\pnhang {\pntxtb (}{\pntxta )}}{\*\pnseclvl7\pnlcrm\pnstart1\pnindent720\pnhang {\pntxtb (}{\pntxta )}}{\*\pnseclvl8\pnlcltr\pnstart1\pnindent720\pnhang {\pntxtb (}{\pntxta )}}{\*\pnseclvl9\pnlcrm\pnstart1\pnindent720\pnhang {\pntxtb (}{\pntxta )}}\pard\plain \ltrpar\ql \li0\ri0\widctlpar\wrapdefault\aspalpha\aspnum\faauto\adjustright\rin0\lin0\itap0\pararsid7497393 \rtlch\fcs1 \af31507\afs22\alang1025 \ltrch\fcs0 \f31506\fs22\lang2057\langfe1033\cgrid\langnp2057\langfenp1033 {\rtlch\fcs1 \af31507 \ltrch\fcs0 \cf0\insrsid7497393\charrsid7497393 
----\par }}

----select * from NotebookTypeGroups
--select * from NotebookTypes
--select * from NotebookFolders
--select * from NotebookLinkTypes
----select * from NotebookTypeSectors
----select * from NotebookTypeTemplates
----select * from NotebookTypeUsers

--select top 100 * from NotebookLinks
--select top 100 * from NotebookItems
--select top 100 * from NotebookItemContent

--select count(NotebookItemId) from NotebookItemContent -- 906201
--select count(NotebookLinkId) from NotebookLinks -- 3160695