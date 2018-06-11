
with t as (
select
                convert(varchar(max),C.candidateID) as 'candidate-externalId'
		, Coalesce(NULLIF(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
              , Coalesce(NULLIF(replace(C.LastName,'?',''), ''), concat('Lastname-',C.candidateID)) as 'contact-lastName'                
		, C.address1 as 'candidate-address'
		, C.city as 'candidate-city'
		, CASE WHEN (tc.abbreviation = 'NONE' OR tc.abbreviation in ('NULL','ZR') ) THEN '' ELSE tc.abbreviation END as 'candidate-Country'
		, C.zip as 'candidate-zipCode'
		, C.state as 'candiadte-state'
		, case
                        when C.ethnicity like 'Afghan%' then 'AF'
                        when C.ethnicity like 'African%' then 'ZA'
                        when C.ethnicity like 'Afrikaa%' then 'ZA'
                        when C.ethnicity like 'Albania%' then 'AL'
                        when C.ethnicity like 'Algeria%' then 'DZ'
                        when C.ethnicity like 'America%' then 'US'
                        when C.ethnicity like 'Andorra%' then 'AD'
                        when C.ethnicity like 'Argenti%' then 'AR'
                        when C.ethnicity like 'Armenia%' then 'AM'
                        when C.ethnicity like 'Austral%' then 'AU'
                        when C.ethnicity like 'Austria%' then 'AT'
                        when C.ethnicity like 'Azerbai%' then 'AZ'
                        when C.ethnicity like 'Azeri%' then 'AZ'
                        when C.ethnicity like 'Bahamia%' then ''
                        when C.ethnicity like 'Bahrain%' then 'BH'
                        when C.ethnicity like 'Banglad%' then 'BD'
                        when C.ethnicity like 'Barbadi%' then 'BB'
                        when C.ethnicity like 'Batswan%' then 'BW'
                        when C.ethnicity like 'Belarus%' then 'BY'
                        when C.ethnicity like 'Belgian%' then 'BE'
                        when C.ethnicity like 'Benines%' then 'BJ'
                        when C.ethnicity like 'Bolivia%' then 'BO'
                        when C.ethnicity like 'Bosnian%' then ''
                        when C.ethnicity like 'Brazili%' then 'BR'
                        when C.ethnicity like 'British%' then 'GB'
                        when C.ethnicity like 'Bulgari%' then 'BG'
                        when C.ethnicity like 'Cambodi%' then 'KH'
                        when C.ethnicity like 'Cameroo%' then 'CM'
                        when C.ethnicity like 'Canadia%' then 'CA'
                        when C.ethnicity like 'CAR%' then ''
                        when C.ethnicity like 'Chilean%' then 'CL'
                        when C.ethnicity like 'Chinese%' then 'MO'
                        when C.ethnicity like 'Colombi%' then ''
                        when C.ethnicity like 'Congole%' then ''
                        when C.ethnicity like 'Costa%' then 'CR'
                        when C.ethnicity like 'Croatia%' then 'HR'
                        when C.ethnicity like 'Cypriot%' then 'CY'
                        when C.ethnicity like 'Czech%' then 'CZ'
                        when C.ethnicity like 'Danish%' then ''
                        when C.ethnicity like 'Djibout%' then 'DJ'
                        when C.ethnicity like 'Dominic%' then ''
                        when C.ethnicity like 'Dutch%' then 'NL'
                        when C.ethnicity like 'East%' then 'ZA'
                        when C.ethnicity like 'Ecuador%' then 'EC'
                        when C.ethnicity like 'Egyptia%' then 'EG'
                        when C.ethnicity like 'Emirati%' then 'AE'
                        when C.ethnicity like 'Eritrea%' then 'ER'
                        when C.ethnicity like 'Estonia%' then 'EE'
                        when C.ethnicity like 'Ethiopi%' then 'ET'
                        when C.ethnicity like 'Fijian%' then 'FJ'
                        when C.ethnicity like 'Filipin%' then 'PH'
                        when C.ethnicity like 'Finnish%' then ''
                        when C.ethnicity like 'French%' then 'FR'
                        when C.ethnicity like 'Gabones%' then 'GA'
                        when C.ethnicity like 'German%' then 'DE'
                        when C.ethnicity like 'Ghanaia%' then 'GH'
                        when C.ethnicity like 'Greek%' then 'GR'
                        when C.ethnicity like 'Grenadi%' then 'GD'
                        when C.ethnicity like 'Guatema%' then 'GT'
                        when C.ethnicity like 'Guinea%' then 'GW'
                        when C.ethnicity like 'Hondura%' then 'HN'
                        when C.ethnicity like 'Hungari%' then 'HU'
                        when C.ethnicity like 'Iceland%' then 'IS'
                        when C.ethnicity like 'Indian%' then 'IN'
                        when C.ethnicity like 'Indones%' then 'ID'
                        when C.ethnicity like 'Iranian%' then 'IR'
                        when C.ethnicity like 'Iraqi%' then 'IQ'
                        when C.ethnicity like 'Irish%' then 'IE'
                        when C.ethnicity like 'Israeli%' then 'IL'
                        when C.ethnicity like 'Italian%' then 'IT'
                        when C.ethnicity like 'Ivoiria%' then ''
                        when C.ethnicity like 'Japanes%' then 'JP'
                        when C.ethnicity like 'Jordani%' then 'JO'
                        when C.ethnicity like 'Kazakh%' then 'KZ'
                        when C.ethnicity like 'Kenyan%' then 'KE'
                        when C.ethnicity like 'Korean%' then 'KR'
                        when C.ethnicity like 'Kuwaiti%' then 'KW'
                        when C.ethnicity like 'Kyrgyz%' then 'KG'
                        when C.ethnicity like 'Latvian%' then 'LV'
                        when C.ethnicity like 'Lebanes%' then 'LB'
                        when C.ethnicity like 'Libyan%' then ''
                        when C.ethnicity like 'Lithuan%' then 'LT'
                        when C.ethnicity like 'Luxembo%' then 'LU'
                        when C.ethnicity like 'Macedon%' then 'MK'
                        when C.ethnicity like 'Malagas%' then 'MG'
                        when C.ethnicity like 'Malaysi%' then 'MY'
                        when C.ethnicity like 'Malian%' then 'ML'
                        when C.ethnicity like 'Maltese%' then 'MT'
                        when C.ethnicity like 'Maurita%' then 'MR'
                        when C.ethnicity like 'Mauriti%' then 'MU'
                        when C.ethnicity like 'Mexican%' then 'MX'
                        when C.ethnicity like 'Moldova%' then 'MD'
                        when C.ethnicity like 'Montene%' then ''
                        when C.ethnicity like 'Morocca%' then 'MA'
                        when C.ethnicity like 'Namibia%' then 'NA'
                        when C.ethnicity like 'Nigeria%' then 'NG'
                        when C.ethnicity like 'Norwegi%' then 'NO'
                        when C.ethnicity like 'Omani%' then 'OM RO'
                        when C.ethnicity like 'Pakista%' then 'PK'
                        when C.ethnicity like 'Panaman%' then 'PA'
                        when C.ethnicity like 'Peruvia%' then 'PE'
                        when C.ethnicity like 'Polish%' then 'PL'
                        when C.ethnicity like 'Portuge%' then 'PT'
                        when C.ethnicity like 'Qatari%' then 'QA'
                        when C.ethnicity like 'Romania%' then 'RO'
                        when C.ethnicity like 'Russian%' then 'RU'
                        when C.ethnicity like 'Saudi%' then 'SA'
                        when C.ethnicity like 'Senegal%' then 'SN'
                        when C.ethnicity like 'Serb%' then ''
                        when C.ethnicity like 'Singapo%' then 'SG'
                        when C.ethnicity like 'Slovak%' then 'SK'
                        when C.ethnicity like 'Slovene%' then 'SI'
                        when C.ethnicity like 'Slovoki%' then ''
                        when C.ethnicity like 'Spanish%' then 'ES'
                        when C.ethnicity like 'Sri%' then 'LK'
                        when C.ethnicity like 'Sudanes%' then 'SD'
                        when C.ethnicity like 'Swedish%' then 'CH'
                        when C.ethnicity like 'Swiss%' then 'CH'
                        when C.ethnicity like 'Syrian%' then ''
                        when C.ethnicity like 'Taiwane%' then 'TW'
                        when C.ethnicity like 'Thai%' then 'TH'
                        when C.ethnicity like 'Tibetan%' then ''
                        when C.ethnicity like 'Togoles%' then 'TG'
                        when C.ethnicity like 'Trinida%' then 'TT'
                        when C.ethnicity like 'Tunisia%' then 'TN'
                        when C.ethnicity like 'Turkish%' then ''
                        when C.ethnicity like 'Tuvalua%' then ''
                        when C.ethnicity like 'Ukrania%' then 'UA'
                        when C.ethnicity like 'Undiscl%' then ''
                        when C.ethnicity like 'Unknown%' then ''
                        when C.ethnicity like 'Uzbek%' then 'UZ'
                        when C.ethnicity like 'Venezue%' then 'VE'
                        when C.ethnicity like 'Vietnam%' then 'VN'
                        when C.ethnicity like 'Zealand%' then 'NZ'
                        when C.ethnicity like 'Zimbabw%' then 'ZW'
                        when C.ethnicity like '%UNITED%ARAB%' then 'AE'
                        when C.ethnicity like '%UAE%' then 'AE'
                        when C.ethnicity like '%U.A.E%' then 'AE'
                        when C.ethnicity like '%UNITED%KINGDOM%' then 'GB'
                        when C.ethnicity like '%UNITED%STATES%' then 'US'
                        when C.ethnicity like '%US%' then 'US'
		end as 'candidate-citizenship'
	from bullhorn1.Candidate C
       left join tmp_country tc ON c.countryID = tc.code
       where C.isPrimaryOwner = 1
       --and c.countryID <> ''
       --and c.userID in (76938, 100453, 120112)
)       

--select [candidate-externalId], [candidate-Country]  from t where [candidate-Country] is not null and [candidate-Country] <> '' and [candidate-externalId] in ('57','67');
select *  from t where [candidate-Country] is not null and [candidate-Country] <> '' and [candidate-externalId] in ('57','67');