
select    can.id as vincereID,can.external_id,can.first_name as FirstName, middle_name as MiddleName, can.last_name as LastName, can.nickname as NickName
        , note
-- select count(*) -- select *
from candidate can
where note ilike '%LTD Company Country: Hong Kong SAR%'
limit 300


--SELECT SUBSTRING( string , LEN(string) -  CHARINDEX('/',REVERSE(string)) + 2  , LEN(string)  ) FROM SAMPLE;


select  'update candidate set note = replace(note,'''
        ,concat('LTD Company Country: ',t.code)
        , ''','''
        ,concat('LTD Company Country: ',t.country) 
        , ''');'
from tmp_country t



update candidate set note = replace(note,'LTD Company Country: 1','LTD Company Country: United States');
update candidate set note = replace(note,'LTD Company Country: 2185','LTD Company Country: Afghanistan');
update candidate set note = replace(note,'LTD Company Country: 2186','LTD Company Country: Albania');
update candidate set note = replace(note,'LTD Company Country: 2187','LTD Company Country: Algeria');
update candidate set note = replace(note,'LTD Company Country: 2188','LTD Company Country: Andorra');
update candidate set note = replace(note,'LTD Company Country: 2189','LTD Company Country: Angola');
update candidate set note = replace(note,'LTD Company Country: 2190','LTD Company Country: Antartica');
update candidate set note = replace(note,'LTD Company Country: 2191','LTD Company Country: Antigua and Barbuda');
update candidate set note = replace(note,'LTD Company Country: 2192','LTD Company Country: Argentina');
update candidate set note = replace(note,'LTD Company Country: 2193','LTD Company Country: Armenia');
update candidate set note = replace(note,'LTD Company Country: 2194','LTD Company Country: Australia');
update candidate set note = replace(note,'LTD Company Country: 2195','LTD Company Country: Austria');
update candidate set note = replace(note,'LTD Company Country: 2196','LTD Company Country: Azerbaijan');
update candidate set note = replace(note,'LTD Company Country: 2197','LTD Company Country: Bahamas');
update candidate set note = replace(note,'LTD Company Country: 2198','LTD Company Country: Bahrain');
update candidate set note = replace(note,'LTD Company Country: 2199','LTD Company Country: Bangladesh');
update candidate set note = replace(note,'LTD Company Country: 2200','LTD Company Country: Barbados');
update candidate set note = replace(note,'LTD Company Country: 2201','LTD Company Country: Belarus');
update candidate set note = replace(note,'LTD Company Country: 2202','LTD Company Country: Belgium');
update candidate set note = replace(note,'LTD Company Country: 2203','LTD Company Country: Belize');
update candidate set note = replace(note,'LTD Company Country: 2204','LTD Company Country: Benin');
update candidate set note = replace(note,'LTD Company Country: 2205','LTD Company Country: Bhutan');
update candidate set note = replace(note,'LTD Company Country: 2206','LTD Company Country: Bolivia');
update candidate set note = replace(note,'LTD Company Country: 2207','LTD Company Country: Bosnia Hercegovina');
update candidate set note = replace(note,'LTD Company Country: 2208','LTD Company Country: Botswana');
update candidate set note = replace(note,'LTD Company Country: 2209','LTD Company Country: Brazil');
update candidate set note = replace(note,'LTD Company Country: 2210','LTD Company Country: Brunei Darussalam');
update candidate set note = replace(note,'LTD Company Country: 2211','LTD Company Country: Bulgaria');
update candidate set note = replace(note,'LTD Company Country: 2212','LTD Company Country: Burkina Faso');
update candidate set note = replace(note,'LTD Company Country: 2213','LTD Company Country: Burundi');
update candidate set note = replace(note,'LTD Company Country: 2214','LTD Company Country: Cambodia');
update candidate set note = replace(note,'LTD Company Country: 2215','LTD Company Country: Cameroon');
update candidate set note = replace(note,'LTD Company Country: 2216','LTD Company Country: Canada');
update candidate set note = replace(note,'LTD Company Country: 2217','LTD Company Country: Cape Verde');
update candidate set note = replace(note,'LTD Company Country: 2218','LTD Company Country: Central African Republic');
update candidate set note = replace(note,'LTD Company Country: 2219','LTD Company Country: Chad');
update candidate set note = replace(note,'LTD Company Country: 2220','LTD Company Country: Chile');
update candidate set note = replace(note,'LTD Company Country: 2221','LTD Company Country: China');
update candidate set note = replace(note,'LTD Company Country: 2222','LTD Company Country: Columbia');
update candidate set note = replace(note,'LTD Company Country: 2223','LTD Company Country: Comoros');
update candidate set note = replace(note,'LTD Company Country: 2226','LTD Company Country: Costa Rica');
update candidate set note = replace(note,'LTD Company Country: 2227','LTD Company Country: Cote d''Ivoire');
update candidate set note = replace(note,'LTD Company Country: 2228','LTD Company Country: Croatia');
update candidate set note = replace(note,'LTD Company Country: 2229','LTD Company Country: Cuba');
update candidate set note = replace(note,'LTD Company Country: 2230','LTD Company Country: Cyprus');
update candidate set note = replace(note,'LTD Company Country: 2231','LTD Company Country: Czech Republic');
update candidate set note = replace(note,'LTD Company Country: 2232','LTD Company Country: Denmark');
update candidate set note = replace(note,'LTD Company Country: 2233','LTD Company Country: Djibouti');
update candidate set note = replace(note,'LTD Company Country: 2234','LTD Company Country: Dominica');
update candidate set note = replace(note,'LTD Company Country: 2235','LTD Company Country: Dominican Republic');
update candidate set note = replace(note,'LTD Company Country: 2236','LTD Company Country: Ecuador');
update candidate set note = replace(note,'LTD Company Country: 2237','LTD Company Country: Egypt');
update candidate set note = replace(note,'LTD Company Country: 2238','LTD Company Country: El Salvador');
update candidate set note = replace(note,'LTD Company Country: 2239','LTD Company Country: Equatorial Guinea');
update candidate set note = replace(note,'LTD Company Country: 2240','LTD Company Country: Eritrea');
update candidate set note = replace(note,'LTD Company Country: 2241','LTD Company Country: Estonia');
update candidate set note = replace(note,'LTD Company Country: 2242','LTD Company Country: Ethiopia');
update candidate set note = replace(note,'LTD Company Country: 2243','LTD Company Country: Fiji');
update candidate set note = replace(note,'LTD Company Country: 2244','LTD Company Country: Finland');
update candidate set note = replace(note,'LTD Company Country: 2245','LTD Company Country: France');
update candidate set note = replace(note,'LTD Company Country: 2246','LTD Company Country: Gabon');
update candidate set note = replace(note,'LTD Company Country: 2248','LTD Company Country: Georgia');
update candidate set note = replace(note,'LTD Company Country: 2249','LTD Company Country: Germany');
update candidate set note = replace(note,'LTD Company Country: 2250','LTD Company Country: Ghana');
update candidate set note = replace(note,'LTD Company Country: 2251','LTD Company Country: Greece');
update candidate set note = replace(note,'LTD Company Country: 2252','LTD Company Country: Greenland');
update candidate set note = replace(note,'LTD Company Country: 2253','LTD Company Country: Grenada');
update candidate set note = replace(note,'LTD Company Country: 2255','LTD Company Country: Guinea');
update candidate set note = replace(note,'LTD Company Country: 2256','LTD Company Country: Guinea-Bissau');
update candidate set note = replace(note,'LTD Company Country: 2257','LTD Company Country: Guyana');
update candidate set note = replace(note,'LTD Company Country: 2258','LTD Company Country: Haiti');
update candidate set note = replace(note,'LTD Company Country: 2259','LTD Company Country: Honduras');
update candidate set note = replace(note,'LTD Company Country: 2260','LTD Company Country: Hungary');
update candidate set note = replace(note,'LTD Company Country: 2261','LTD Company Country: Iceland');
update candidate set note = replace(note,'LTD Company Country: 2262','LTD Company Country: India');
update candidate set note = replace(note,'LTD Company Country: 2263','LTD Company Country: Indonesia');
update candidate set note = replace(note,'LTD Company Country: 2264','LTD Company Country: Iran');
update candidate set note = replace(note,'LTD Company Country: 2265','LTD Company Country: Iraq');
update candidate set note = replace(note,'LTD Company Country: 2266','LTD Company Country: Ireland');
update candidate set note = replace(note,'LTD Company Country: 2267','LTD Company Country: Israel');
update candidate set note = replace(note,'LTD Company Country: 2268','LTD Company Country: Italy');
update candidate set note = replace(note,'LTD Company Country: 2269','LTD Company Country: Jamaica');
update candidate set note = replace(note,'LTD Company Country: 2270','LTD Company Country: Japan');
update candidate set note = replace(note,'LTD Company Country: 2271','LTD Company Country: Jordan');
update candidate set note = replace(note,'LTD Company Country: 2272','LTD Company Country: Kazakhstan');
update candidate set note = replace(note,'LTD Company Country: 2273','LTD Company Country: Kenya');
update candidate set note = replace(note,'LTD Company Country: 2274','LTD Company Country: Korea; Democratic People''s Republic Of (North)');
update candidate set note = replace(note,'LTD Company Country: 2275','LTD Company Country: Korea; Republic Of (South)');
update candidate set note = replace(note,'LTD Company Country: 2276','LTD Company Country: Kuwait');
update candidate set note = replace(note,'LTD Company Country: 2277','LTD Company Country: Kyrgyzstan');
update candidate set note = replace(note,'LTD Company Country: 2278','LTD Company Country: Lao People''s Democratic Republic');
update candidate set note = replace(note,'LTD Company Country: 2279','LTD Company Country: Latvia');
update candidate set note = replace(note,'LTD Company Country: 2280','LTD Company Country: Lebanon');
update candidate set note = replace(note,'LTD Company Country: 2281','LTD Company Country: Lesotho');
update candidate set note = replace(note,'LTD Company Country: 2282','LTD Company Country: Liberia');
update candidate set note = replace(note,'LTD Company Country: 2284','LTD Company Country: Liechtenstein');
update candidate set note = replace(note,'LTD Company Country: 2285','LTD Company Country: Lithuania');
update candidate set note = replace(note,'LTD Company Country: 2286','LTD Company Country: Luxembourg');
update candidate set note = replace(note,'LTD Company Country: 2287','LTD Company Country: Macau');
update candidate set note = replace(note,'LTD Company Country: 2288','LTD Company Country: Macedonia');
update candidate set note = replace(note,'LTD Company Country: 2289','LTD Company Country: Madagascar');
update candidate set note = replace(note,'LTD Company Country: 2290','LTD Company Country: Malawi');
update candidate set note = replace(note,'LTD Company Country: 2291','LTD Company Country: Malaysia');
update candidate set note = replace(note,'LTD Company Country: 2292','LTD Company Country: Mali');
update candidate set note = replace(note,'LTD Company Country: 2293','LTD Company Country: Malta');
update candidate set note = replace(note,'LTD Company Country: 2294','LTD Company Country: Mauritania');
update candidate set note = replace(note,'LTD Company Country: 2295','LTD Company Country: Mauritius');
update candidate set note = replace(note,'LTD Company Country: 2296','LTD Company Country: Mexico');
update candidate set note = replace(note,'LTD Company Country: 2297','LTD Company Country: Micronesia; Federated States of');
update candidate set note = replace(note,'LTD Company Country: 2299','LTD Company Country: Monaco');
update candidate set note = replace(note,'LTD Company Country: 2300','LTD Company Country: Mongolia');
update candidate set note = replace(note,'LTD Company Country: 2301','LTD Company Country: Morocco');
update candidate set note = replace(note,'LTD Company Country: 2302','LTD Company Country: Mozambique');
update candidate set note = replace(note,'LTD Company Country: 2303','LTD Company Country: Myanmar');
update candidate set note = replace(note,'LTD Company Country: 2304','LTD Company Country: Namibia');
update candidate set note = replace(note,'LTD Company Country: 2305','LTD Company Country: Nepal');
update candidate set note = replace(note,'LTD Company Country: 2306','LTD Company Country: Netherlands');
update candidate set note = replace(note,'LTD Company Country: 2307','LTD Company Country: New Zealand');
update candidate set note = replace(note,'LTD Company Country: 2308','LTD Company Country: Nicaragua');
update candidate set note = replace(note,'LTD Company Country: 2309','LTD Company Country: Niger');
update candidate set note = replace(note,'LTD Company Country: 2310','LTD Company Country: Nigeria');
update candidate set note = replace(note,'LTD Company Country: 2311','LTD Company Country: Norway');
update candidate set note = replace(note,'LTD Company Country: 2312','LTD Company Country: Oman');
update candidate set note = replace(note,'LTD Company Country: 2313','LTD Company Country: Pakistan');
update candidate set note = replace(note,'LTD Company Country: 2314','LTD Company Country: Palau');
update candidate set note = replace(note,'LTD Company Country: 2315','LTD Company Country: Panama');
update candidate set note = replace(note,'LTD Company Country: 2316','LTD Company Country: Papua New Guinea');
update candidate set note = replace(note,'LTD Company Country: 2317','LTD Company Country: Paraguay');
update candidate set note = replace(note,'LTD Company Country: 2318','LTD Company Country: Peru');
update candidate set note = replace(note,'LTD Company Country: 2319','LTD Company Country: Philippines');
update candidate set note = replace(note,'LTD Company Country: 2320','LTD Company Country: Poland');
update candidate set note = replace(note,'LTD Company Country: 2321','LTD Company Country: Portugal');
update candidate set note = replace(note,'LTD Company Country: 2322','LTD Company Country: Qatar');
update candidate set note = replace(note,'LTD Company Country: 2323','LTD Company Country: Romania');
update candidate set note = replace(note,'LTD Company Country: 2324','LTD Company Country: Russian Federation');
update candidate set note = replace(note,'LTD Company Country: 2325','LTD Company Country: Rwanda');
update candidate set note = replace(note,'LTD Company Country: 2326','LTD Company Country: Saint Lucia');
update candidate set note = replace(note,'LTD Company Country: 2327','LTD Company Country: San Marino');
update candidate set note = replace(note,'LTD Company Country: 2328','LTD Company Country: Saudi Arabia');
update candidate set note = replace(note,'LTD Company Country: 2329','LTD Company Country: Senegal');
update candidate set note = replace(note,'LTD Company Country: 2331','LTD Company Country: Seychelles');
update candidate set note = replace(note,'LTD Company Country: 2332','LTD Company Country: Sierra Leone');
update candidate set note = replace(note,'LTD Company Country: 2333','LTD Company Country: Singapore');
update candidate set note = replace(note,'LTD Company Country: 2334','LTD Company Country: Slovakia');
update candidate set note = replace(note,'LTD Company Country: 2335','LTD Company Country: Slovenia');
update candidate set note = replace(note,'LTD Company Country: 2336','LTD Company Country: Solomon Islands');
update candidate set note = replace(note,'LTD Company Country: 2337','LTD Company Country: Somalia');
update candidate set note = replace(note,'LTD Company Country: 2338','LTD Company Country: South Africa');
update candidate set note = replace(note,'LTD Company Country: 2339','LTD Company Country: Spain');
update candidate set note = replace(note,'LTD Company Country: 2340','LTD Company Country: Sri Lanka');
update candidate set note = replace(note,'LTD Company Country: 2341','LTD Company Country: Sudan');
update candidate set note = replace(note,'LTD Company Country: 2342','LTD Company Country: Suriname');
update candidate set note = replace(note,'LTD Company Country: 2343','LTD Company Country: Swaziland');
update candidate set note = replace(note,'LTD Company Country: 2344','LTD Company Country: Sweden');
update candidate set note = replace(note,'LTD Company Country: 2345','LTD Company Country: Switzerland');
update candidate set note = replace(note,'LTD Company Country: 2348','LTD Company Country: Tajikistan');
update candidate set note = replace(note,'LTD Company Country: 2349','LTD Company Country: Tanzania');
update candidate set note = replace(note,'LTD Company Country: 2350','LTD Company Country: Thailand');
update candidate set note = replace(note,'LTD Company Country: 2351','LTD Company Country: Togo');
update candidate set note = replace(note,'LTD Company Country: 2352','LTD Company Country: Trinidad and Tobago');
update candidate set note = replace(note,'LTD Company Country: 2353','LTD Company Country: Tunisia');
update candidate set note = replace(note,'LTD Company Country: 2354','LTD Company Country: Turkey; Republic of');
update candidate set note = replace(note,'LTD Company Country: 2355','LTD Company Country: Turkmenistan');
update candidate set note = replace(note,'LTD Company Country: 2356','LTD Company Country: Uganda');
update candidate set note = replace(note,'LTD Company Country: 2357','LTD Company Country: Ukraine');
update candidate set note = replace(note,'LTD Company Country: 2358','LTD Company Country: United Arab Emirates');
update candidate set note = replace(note,'LTD Company Country: 2359','LTD Company Country: United Kingdom');
update candidate set note = replace(note,'LTD Company Country: 2360','LTD Company Country: Uruguay');
update candidate set note = replace(note,'LTD Company Country: 2361','LTD Company Country: Uzbekistan');
update candidate set note = replace(note,'LTD Company Country: 2362','LTD Company Country: Vatican City');
update candidate set note = replace(note,'LTD Company Country: 2363','LTD Company Country: Venezuela');
update candidate set note = replace(note,'LTD Company Country: 2364','LTD Company Country: Vietnam');
update candidate set note = replace(note,'LTD Company Country: 2367','LTD Company Country: Yugoslavia');
update candidate set note = replace(note,'LTD Company Country: 2368','LTD Company Country: Zaire');
update candidate set note = replace(note,'LTD Company Country: 2369','LTD Company Country: Zambia');
update candidate set note = replace(note,'LTD Company Country: 2370','LTD Company Country: Zimbabwe');
update candidate set note = replace(note,'LTD Company Country: 2371','LTD Company Country: Guatemala');
update candidate set note = replace(note,'LTD Company Country: 2372','LTD Company Country: Bermuda');
update candidate set note = replace(note,'LTD Company Country: 2373','LTD Company Country: Aruba');
update candidate set note = replace(note,'LTD Company Country: 2374','LTD Company Country: Puerto Rico');
update candidate set note = replace(note,'LTD Company Country: 2375','LTD Company Country: Taiwan');
update candidate set note = replace(note,'LTD Company Country: 2376','LTD Company Country: Guam');
update candidate set note = replace(note,'LTD Company Country: 2377','LTD Company Country: Hong Kong SAR');
update candidate set note = replace(note,'LTD Company Country: 2378','LTD Company Country: None Specified');
--update candidate set note = replace(note,'LTD Company Country: None Specified','');
update candidate set note = replace(note,'LTD Company Country: 2379','LTD Company Country: Cayman Islands');