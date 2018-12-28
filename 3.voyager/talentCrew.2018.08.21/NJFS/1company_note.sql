
with
ctt0 (intCompanyTierId,note) as (
			    select intCompanyTierId,
			            Stuff( 
			              + Coalesce('Location Phone: ' + NULLIF(cast(vchValue as varchar(max)), '') + char(10), '')
			              + Coalesce('Location OK to SMS Flag: ' + NULLIF(cast(bitOKToSMS as varchar(max)), '') + char(10), '')
                                   + Coalesce('Location Send by Default Flag: ' + NULLIF(cast(bitSendByDefault as varchar(max)), '') + char(10), '')
                                   + Coalesce('Location Ext: ' + NULLIF(cast(vchExtension as varchar(max)), '') + char(10), '')
			            , 1, 0, '') as 'note'
			    from dCompanyTierTelecom 
       )
, ctt (intCompanyTierId,note) as (
       SELECT    intCompanyTierId
                , STUFF((SELECT char(10) + note from ctt0 WHERE intCompanyTierId = a.intCompanyTierId FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS note
        FROM (select intCompanyTierId from ctt0) AS a GROUP BY a.intCompanyTierId )
--select * from ctt where intCompanyTierId = 13
--select intCompanyTierId from ctt group by intCompanyTierId having count(*) > 1 
--select count(*) from ctt

, ct0 (intCompanyId,note) as ( 
			    select a.intCompanyId,
			            Stuff( 
                                   + Coalesce('Location Description: ' + NULLIF(cast(a.vchDescription as varchar(max)), '') + char(10), '')
                                   + Coalesce('Location OK to Mailshot Flag: ' + NULLIF(cast(a.bitOKToMailshot as varchar(max)), '') + char(10), '')
                                   + Coalesce('Location Notes: ' + NULLIF(cast(a.vchNote as varchar(max)), '') + char(10), '')
                                   + Coalesce( char(10 ) + NULLIF(cast(ctt.note as varchar(max)), '') + char(10), '')
			            , 1, 0, '') as 'note'			
                       from dCompanyTier a
                       left join ctt on ctt.intCompanyTierId = a.intCompanyTierId
       )
--select intCompanyId from ct0 group by intCompanyId having count(*) > 1
--select * from ct0 where intCompanyId = 14
--select intCompanyId, intCompanyTierId, vchDescription,bitOKToMailshot,vchNote   from dCompanyTier where intCompanyId = 14


, ct (intCompanyId,note) as (
       SELECT intCompanyId
                , STUFF((
                     SELECT char(10) + note 
                     from ct0 
                     WHERE intCompanyId = a.intCompanyId FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS note
        FROM (select intCompanyId from ct0) AS a GROUP BY a.intCompanyId )
--select intCompanyTierId from ct group by intCompanyTierId having count(*) > 1 
--select count(*) from ct where intCompanyId = 14

-- select intCompanyId, * from ct where intCompanyId = 14
			
			
select
  C.intCompanyId
, Stuff( 
       Coalesce('ID: ' + NULLIF(cast(C.intCompanyId as varchar(max)), '') + char(10), '')
+ Coalesce('Active Flag: ' + NULLIF(cast(C.bitActive as varchar(max)), '') + char(10), '')
+ Coalesce('Type: ' + NULLIF(cast(C.tintCompanyTypeId as varchar(max)), '') + char(10), '')
+ Coalesce('Notes: ' + NULLIF(cast(C.vchNote as varchar(max)), '') + char(10), '')
--+ Coalesce('Location Phone: ' + NULLIF(cast(C.vchValue as varchar(max)), '') + char(10), '')
--+ Coalesce('Location Description: ' + NULLIF(cast(C.vchDescription as varchar(max)), '') + char(10), '')
--+ Coalesce('Location OK to Mailshot Flag: ' + NULLIF(cast(C.bitOKToMailshot as varchar(max)), '') + char(10), '')
--+ Coalesce('Location OK to SMS Flag: ' + NULLIF(cast(C.bitOKToSMS as varchar(max)), '') + char(10), '')
--+ Coalesce('Location Send by Default Flag: ' + NULLIF(cast(C.bitSendByDefault as varchar(max)), '') + char(10), '')
--+ Coalesce('Location Ext: ' + NULLIF(cast(C.vchExtension as varchar(max)), '') + char(10), '')
+ Coalesce('Preferred Telecom: ' + NULLIF(cast(C.intPreferredTelecomId as varchar(max)), '') + char(10), '')
--+ Coalesce('Location Notes: ' + NULLIF(cast(C.vchNote as varchar(max)), '') + char(10), '')
+ Coalesce('Company Financials > Registered No: ' + NULLIF(cast(C.vchCompanyRegNo as varchar(max)), '') + char(10), '')
+ Coalesce('Company Financials > VAT No: ' + NULLIF(cast(C.vchVATNumber as varchar(max)), '') + char(10), '')
+ Coalesce('Company Financials > VAT Code: ' + NULLIF(cast(C.tintVATCodeId as varchar(max)), '') + char(10), '')
+ Coalesce('Company Financials > Default Currency: ' + NULLIF(cast(C.tintChargeCurrencyId as varchar(max)), '') + char(10), '')
+ Coalesce('Company Financials > Territory: ' + NULLIF(cast(C.sintTerritoryId as varchar(max)), '') + char(10), '')
+ Coalesce('Company Valid From: ' + NULLIF(cast(p.dValidFromDate as varchar(max)), '') + char(10), '')
+ Coalesce('Company Valid To Date: ' + NULLIF(cast(p.dValidToDate as varchar(max)), '') + char(10), '')
+ Coalesce('Company Origin: ' + NULLIF(cast(p.decValue as varchar(max)), '') + char(10), '')
+ Coalesce('Company Currency: ' + NULLIF(cast(p.tintValueCurrencyId as varchar(max)), '') + char(10), '')
+ Coalesce('Company Balance: ' + NULLIF(cast(p.decActualValue as varchar(max)), '') + char(10), '')
+ Coalesce('Company PO Description: ' + NULLIF(cast(p.vchDescription as varchar(max)), '') + char(10), '')
+ Coalesce('Company Invoice Location: ' + NULLIF(cast(C.intInvoiceCompanyTierId as varchar(max)), '') + char(10), '')
+ Coalesce('Company Invoie Contact: ' + NULLIF(cast(C.intInvoiceContactId as varchar(max)), '') + char(10), '')
+ Coalesce('Company Back Office Sector: ' + NULLIF(cast(C.tintBackOfficeSectorId as varchar(max)), '') + char(10), '')
+ Coalesce( char(10) + NULLIF(cast(ct.note as varchar(max)), '') + char(10), '')
       , 1, 0, '') as 'note'
-- select count(*)
from dCompany C --1145
left join ct on ct.intCompanyId = C.intCompanyId
left join dPONumber p on p.intCompanyId = C.intCompanyId
			