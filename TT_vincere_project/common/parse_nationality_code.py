
import pandas as pd
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 500)
pd.set_option('display.width', 1000)
import re
from bs4 import BeautifulSoup
html = """
<select id="nationality" name="nationality" class="chosen-select form-control" style="display: none;">
                                <option value="">Please select</option>
                                
                                    
                                        <option value="AF">
                                            Afghan
                                        </option>
                                    
                                
                                    
                                
                                    
                                        <option value="AL">
                                            Albanian
                                        </option>
                                    
                                
                                    
                                        <option value="DZ">
                                            Algerian
                                        </option>
                                    
                                
                                    
                                        <option value="AS">
                                            Samoan
                                        </option>
                                    
                                
                                    
                                        <option value="AD">
                                            Andorran
                                        </option>
                                    
                                
                                    
                                        <option value="AO">
                                            Angolan
                                        </option>
                                    
                                
                                    
                                        <option value="AI">
                                            Anguillian
                                        </option>
                                    
                                
                                    
                                        <option value="AQ">
                                            Antarctican
                                        </option>
                                    
                                
                                    
                                        <option value="AG">
                                            Antiguan, Barbudan
                                        </option>
                                    
                                
                                    
                                        <option value="AR">
                                            Argentinean
                                        </option>
                                    
                                
                                    
                                        <option value="AM">
                                            Armenian
                                        </option>
                                    
                                
                                    
                                        <option value="AW">
                                            Aruban
                                        </option>
                                    
                                
                                    
                                        <option value="AU">
                                            Australian
                                        </option>
                                    
                                
                                    
                                        <option value="AT">
                                            Austrian
                                        </option>
                                    
                                
                                    
                                        <option value="AZ">
                                            Azerbaijani
                                        </option>
                                    
                                
                                    
                                        <option value="BS">
                                            Bahamian
                                        </option>
                                    
                                
                                    
                                        <option value="BH">
                                            Bahraini
                                        </option>
                                    
                                
                                    
                                        <option value="BD">
                                            Bangladeshi
                                        </option>
                                    
                                
                                    
                                        <option value="BB">
                                            Barbadian
                                        </option>
                                    
                                
                                    
                                        <option value="BY">
                                            Belarusian/Belarusian
                                        </option>
                                    
                                
                                    
                                        <option value="BE">
                                            Belgian
                                        </option>
                                    
                                
                                    
                                        <option value="BZ">
                                            Belizean
                                        </option>
                                    
                                
                                    
                                        <option value="BJ">
                                            Beninese
                                        </option>
                                    
                                
                                    
                                        <option value="BM">
                                            Bermudian
                                        </option>
                                    
                                
                                    
                                        <option value="BT">
                                            Bhutanese
                                        </option>
                                    
                                
                                    
                                        <option value="BO">
                                            Bolivian
                                        </option>
                                    
                                
                                    
                                
                                    
                                        <option value="BA">
                                            Bosnian
                                        </option>
                                    
                                
                                    
                                        <option value="BW">
                                            Batswana
                                        </option>
                                    
                                
                                    
                                
                                    
                                        <option value="BR">
                                            Brazilian
                                        </option>
                                    
                                
                                    
                                        <option value="BN">
                                            Bruneian
                                        </option>
                                    
                                
                                    
                                        <option value="BG">
                                            Bulgarian
                                        </option>
                                    
                                
                                    
                                        <option value="BF">
                                            Burkinese
                                        </option>
                                    
                                
                                    
                                        <option value="BI">
                                            Burundian
                                        </option>
                                    
                                
                                    
                                        <option value="KH">
                                            Cambodian
                                        </option>
                                    
                                
                                    
                                        <option value="CM">
                                            Cameroonian
                                        </option>
                                    
                                
                                    
                                        <option value="CA">
                                            Canadian
                                        </option>
                                    
                                
                                    
                                        <option value="CV">
                                            Cape Verdean
                                        </option>
                                    
                                
                                    
                                        <option value="KY">
                                            Caymanian
                                        </option>
                                    
                                
                                    
                                        <option value="CF">
                                            Central African
                                        </option>
                                    
                                
                                    
                                
                                    
                                        <option value="TD">
                                            Chadian
                                        </option>
                                    
                                
                                    
                                        <option value="CL">
                                            Chilean
                                        </option>
                                    
                                
                                    
                                        <option value="CN">
                                            Chinese
                                        </option>
                                    
                                
                                    
                                        <option value="CX">
                                            Christmas Islanders
                                        </option>
                                    
                                
                                    
                                        <option value="CC">
                                            Cocossian
                                        </option>
                                    
                                
                                    
                                        <option value="CO">
                                            Colombian
                                        </option>
                                    
                                
                                    
                                        <option value="KM">
                                            Comoran
                                        </option>
                                    
                                
                                    
                                        <option value="CG">
                                            Congolese
                                        </option>
                                    
                                
                                    
                                        <option value="CK">
                                            Cook Islander
                                        </option>
                                    
                                
                                    
                                        <option value="CR">
                                            Costa Rican
                                        </option>
                                    
                                
                                    
                                        <option value="HR">
                                            Croat/Croatian
                                        </option>
                                    
                                
                                    
                                        <option value="CU">
                                            Cuban
                                        </option>
                                    
                                
                                    
                                
                                    
                                        <option value="CY">
                                            Cypriot
                                        </option>
                                    
                                
                                    
                                        <option value="CZ">
                                            Czech
                                        </option>
                                    
                                
                                    
                                        <option value="CI">
                                            Ivorian/Ivoirian
                                        </option>
                                    
                                
                                    
                                        <option value="DK">
                                            Danish
                                        </option>
                                    
                                
                                    
                                        <option value="DJ">
                                            Djiboutian
                                        </option>
                                    
                                
                                    
                                        <option value="DM">
                                            Dominican
                                        </option>
                                    
                                
                                    
                                        <option value="DO">
                                            Dominican
                                        </option>
                                    
                                
                                    
                                        <option value="EC">
                                            Ecuadorean
                                        </option>
                                    
                                
                                    
                                        <option value="EG">
                                            Egyptian
                                        </option>
                                    
                                
                                    
                                        <option value="SV">
                                            Salvadorian
                                        </option>
                                    
                                
                                    
                                        <option value="GQ">
                                            Equatoguinean/Equatorial Guinean
                                        </option>
                                    
                                
                                    
                                        <option value="ER">
                                            Eritrean
                                        </option>
                                    
                                
                                    
                                        <option value="EE">
                                            Estonian
                                        </option>
                                    
                                
                                    
                                        <option value="ET">
                                            Ethiopian
                                        </option>
                                    
                                
                                    
                                
                                    
                                        <option value="FK">
                                            Falkland Islander
                                        </option>
                                    
                                
                                    
                                        <option value="FO">
                                            Faroese
                                        </option>
                                    
                                
                                    
                                        <option value="FJ">
                                            Fijian
                                        </option>
                                    
                                
                                    
                                        <option value="FI">
                                            Finnish
                                        </option>
                                    
                                
                                    
                                        <option value="FR">
                                            French
                                        </option>
                                    
                                
                                    
                                
                                    
                                        <option value="PF">
                                            French Polynesian
                                        </option>
                                    
                                
                                    
                                
                                    
                                        <option value="GA">
                                            Gabonese
                                        </option>
                                    
                                
                                    
                                        <option value="GM">
                                            Gambian
                                        </option>
                                    
                                
                                    
                                        <option value="GE">
                                            Georgian
                                        </option>
                                    
                                
                                    
                                        <option value="DE">
                                            German
                                        </option>
                                    
                                
                                    
                                        <option value="GH">
                                            Ghanaian
                                        </option>
                                    
                                
                                    
                                        <option value="GI">
                                            Gibraltarian
                                        </option>
                                    
                                
                                    
                                        <option value="GR">
                                            Greek
                                        </option>
                                    
                                
                                    
                                        <option value="GL">
                                            Greenlander/Greenlandic
                                        </option>
                                    
                                
                                    
                                        <option value="GD">
                                            Grenadian
                                        </option>
                                    
                                
                                    
                                
                                    
                                        <option value="GU">
                                            Guamanian
                                        </option>
                                    
                                
                                    
                                        <option value="GT">
                                            Guatemalan
                                        </option>
                                    
                                
                                    
                                
                                    
                                        <option value="GN">
                                            Guinean
                                        </option>
                                    
                                
                                    
                                        <option value="GW">
                                            Bissau-Guinean
                                        </option>
                                    
                                
                                    
                                        <option value="GY">
                                            Guyanese
                                        </option>
                                    
                                
                                    
                                        <option value="HT">
                                            Haitian
                                        </option>
                                    
                                
                                    
                                
                                    
                                        <option value="HN">
                                            Honduran
                                        </option>
                                    
                                
                                    
                                        <option value="HK">
                                            Hong Konger
                                        </option>
                                    
                                
                                    
                                        <option value="HU">
                                            Hungarian
                                        </option>
                                    
                                
                                    
                                        <option value="IS">
                                            Icelandic
                                        </option>
                                    
                                
                                    
                                        <option value="IN">
                                            Indian
                                        </option>
                                    
                                
                                    
                                        <option value="ID">
                                            Indonesian
                                        </option>
                                    
                                
                                    
                                        <option value="IR">
                                            Iranian
                                        </option>
                                    
                                
                                    
                                        <option value="IQ">
                                            Iraqi
                                        </option>
                                    
                                
                                    
                                        <option value="IE">
                                            Irish
                                        </option>
                                    
                                
                                    
                                        <option value="IM">
                                            Manx
                                        </option>
                                    
                                
                                    
                                        <option value="IL">
                                            Israeli
                                        </option>
                                    
                                
                                    
                                        <option value="IT">
                                            Italian
                                        </option>
                                    
                                
                                    
                                        <option value="JM">
                                            Jamaican
                                        </option>
                                    
                                
                                    
                                        <option value="JP">
                                            Japanese
                                        </option>
                                    
                                
                                    
                                
                                    
                                        <option value="JO">
                                            Jordanian
                                        </option>
                                    
                                
                                    
                                        <option value="KZ">
                                            Kazakh
                                        </option>
                                    
                                
                                    
                                        <option value="KE">
                                            Kenyan
                                        </option>
                                    
                                
                                    
                                        <option value="KI">
                                            I-Kiribati
                                        </option>
                                    
                                
                                    
                                        <option value="KW">
                                            Kuwaiti
                                        </option>
                                    
                                
                                    
                                        <option value="KG">
                                            Kyrgyz/Kyrgyzstani
                                        </option>
                                    
                                
                                    
                                        <option value="LA">
                                            Laotian
                                        </option>
                                    
                                
                                    
                                        <option value="LV">
                                            Latvian
                                        </option>
                                    
                                
                                    
                                        <option value="LB">
                                            Lebanese
                                        </option>
                                    
                                
                                    
                                        <option value="LS">
                                            Mosotho/Basotho
                                        </option>
                                    
                                
                                    
                                        <option value="LR">
                                            Liberian
                                        </option>
                                    
                                
                                    
                                        <option value="LY">
                                            Libyan
                                        </option>
                                    
                                
                                    
                                        <option value="LI">
                                            Liechtensteiner/Liechtensteinerin
                                        </option>
                                    
                                
                                    
                                        <option value="LT">
                                            Lithuanian
                                        </option>
                                    
                                
                                    
                                        <option value="LU">
                                            Luxembourgers
                                        </option>
                                    
                                
                                    
                                        <option value="MO">
                                            Macanese
                                        </option>
                                    
                                
                                    
                                        <option value="MK">
                                            Macedonian
                                        </option>
                                    
                                
                                    
                                        <option value="MG">
                                            Malagasy/Madagascan
                                        </option>
                                    
                                
                                    
                                        <option value="MW">
                                            Malawian
                                        </option>
                                    
                                
                                    
                                        <option value="MY">
                                            Malaysian
                                        </option>
                                    
                                
                                    
                                        <option value="MV">
                                            Maldivian
                                        </option>
                                    
                                
                                    
                                        <option value="ML">
                                            Malian
                                        </option>
                                    
                                
                                    
                                        <option value="MT">
                                            Maltese
                                        </option>
                                    
                                
                                    
                                        <option value="MH">
                                            Marshallese
                                        </option>
                                    
                                
                                    
                                
                                    
                                        <option value="MR">
                                            Mauritanian
                                        </option>
                                    
                                
                                    
                                        <option value="MU">
                                            Mauritian
                                        </option>
                                    
                                
                                    
                                        <option value="YT">
                                            Mahoran
                                        </option>
                                    
                                
                                    
                                        <option value="MX">
                                            Mexican
                                        </option>
                                    
                                
                                    
                                        <option value="FM">
                                            Micronesian
                                        </option>
                                    
                                
                                    
                                
                                    
                                        <option value="MD">
                                            Moldovan
                                        </option>
                                    
                                
                                    
                                        <option value="MC">
                                            Monégasque/Monacan
                                        </option>
                                    
                                
                                    
                                        <option value="MN">
                                            Mongolian
                                        </option>
                                    
                                
                                    
                                        <option value="ME">
                                            Montenegrin
                                        </option>
                                    
                                
                                    
                                        <option value="MS">
                                            Montserratians
                                        </option>
                                    
                                
                                    
                                        <option value="MA">
                                            Moroccan
                                        </option>
                                    
                                
                                    
                                        <option value="MZ">
                                            Mozambican
                                        </option>
                                    
                                
                                    
                                        <option value="MM">
                                            Burmese
                                        </option>
                                    
                                
                                    
                                        <option value="NA">
                                            Namibian
                                        </option>
                                    
                                
                                    
                                        <option value="NR">
                                            Nauruan
                                        </option>
                                    
                                
                                    
                                        <option value="NP">
                                            Nepalese
                                        </option>
                                    
                                
                                    
                                        <option value="NL">
                                            Dutch
                                        </option>
                                    
                                
                                    
                                        <option value="NC">
                                            New Caledonia
                                        </option>
                                    
                                
                                    
                                        <option value="NZ">
                                            New Zealander/Kiwi 
                                        </option>
                                    
                                
                                    
                                        <option value="NI">
                                            Nicaraguan
                                        </option>
                                    
                                
                                    
                                        <option value="NE">
                                            Nigerien
                                        </option>
                                    
                                
                                    
                                        <option value="NG">
                                            Nigerian
                                        </option>
                                    
                                
                                    
                                        <option value="NU">
                                            Niuean
                                        </option>
                                    
                                
                                    
                                        <option value="NF">
                                            Norfolk Islander
                                        </option>
                                    
                                
                                    
                                
                                    
                                        <option value="KP">
                                            North Korean
                                        </option>
                                    
                                
                                    
                                        <option value="MP">
                                            Northern Mariana Islander
                                        </option>
                                    
                                
                                    
                                        <option value="NO">
                                            Norwegian
                                        </option>
                                    
                                
                                    
                                
                                    
                                        <option value="OM">
                                            Omani
                                        </option>
                                    
                                
                                    
                                
                                    
                                        <option value="PK">
                                            Pakistani
                                        </option>
                                    
                                
                                    
                                        <option value="PW">
                                            Palauan
                                        </option>
                                    
                                
                                    
                                        <option value="PS">
                                            Palestinian
                                        </option>
                                    
                                
                                    
                                        <option value="PA">
                                            Panamanian
                                        </option>
                                    
                                
                                    
                                        <option value="PG">
                                            Papua New Guinean/Guinean
                                        </option>
                                    
                                
                                    
                                        <option value="PY">
                                            Paraguayan
                                        </option>
                                    
                                
                                    
                                        <option value="PE">
                                            Peruvian
                                        </option>
                                    
                                
                                    
                                        <option value="PH">
                                            Filipino
                                        </option>
                                    
                                
                                    
                                        <option value="PN">
                                            Pitcairn Islander
                                        </option>
                                    
                                
                                    
                                        <option value="PL">
                                            Polish
                                        </option>
                                    
                                
                                    
                                        <option value="PT">
                                            Portuguese
                                        </option>
                                    
                                
                                    
                                        <option value="PR">
                                            Puerto Rican
                                        </option>
                                    
                                
                                    
                                        <option value="QA">
                                            Qatari
                                        </option>
                                    
                                
                                    
                                        <option value="RO">
                                            Romanian
                                        </option>
                                    
                                
                                    
                                        <option value="RU">
                                            Russian
                                        </option>
                                    
                                
                                    
                                        <option value="RW">
                                            Rwandan
                                        </option>
                                    
                                
                                    
                                
                                    
                                        <option value="BL">
                                            French
                                        </option>
                                    
                                
                                    
                                        <option value="SH">
                                            Saint Helenian
                                        </option>
                                    
                                
                                    
                                        <option value="KN">
                                            Kittitian
                                        </option>
                                    
                                
                                    
                                        <option value="LC">
                                            Saint Lucian
                                        </option>
                                    
                                
                                    
                                        <option value="MF">
                                            St.Martiner/St.Maartener
                                        </option>
                                    
                                
                                    
                                        <option value="PM">
                                            French
                                        </option>
                                    
                                
                                    
                                        <option value="VC">
                                            Vincentian
                                        </option>
                                    
                                
                                    
                                        <option value="WS">
                                            Samoan
                                        </option>
                                    
                                
                                    
                                        <option value="SM">
                                            Sammarinese
                                        </option>
                                    
                                
                                    
                                        <option value="ST">
                                            Santomean
                                        </option>
                                    
                                
                                    
                                        <option value="SA">
                                            Saudi Arabian
                                        </option>
                                    
                                
                                    
                                        <option value="SN">
                                            Senegalese
                                        </option>
                                    
                                
                                    
                                        <option value="RS">
                                            Serbian
                                        </option>
                                    
                                
                                    
                                        <option value="SC">
                                            Seychellois
                                        </option>
                                    
                                
                                    
                                        <option value="SL">
                                            Sierra Leonian
                                        </option>
                                    
                                
                                    
                                        <option value="SG">
                                            Singaporean
                                        </option>
                                    
                                
                                    
                                
                                    
                                        <option value="SK">
                                            Slovak
                                        </option>
                                    
                                
                                    
                                        <option value="SI">
                                            Slovene/Slovenian
                                        </option>
                                    
                                
                                    
                                        <option value="SB">
                                            Solomon Islander
                                        </option>
                                    
                                
                                    
                                        <option value="SO">
                                            Somali
                                        </option>
                                    
                                
                                    
                                        <option value="ZA">
                                            South African
                                        </option>
                                    
                                
                                    
                                
                                    
                                        <option value="KR">
                                            South Korean
                                        </option>
                                    
                                
                                    
                                        <option value="ES">
                                            Spanish
                                        </option>
                                    
                                
                                    
                                        <option value="LK">
                                            Sri Lankan
                                        </option>
                                    
                                
                                    
                                        <option value="SD">
                                            Sudanese
                                        </option>
                                    
                                
                                    
                                        <option value="SR">
                                            Surinamese
                                        </option>
                                    
                                
                                    
                                
                                    
                                        <option value="SZ">
                                            Swazi
                                        </option>
                                    
                                
                                    
                                        <option value="SE">
                                            Swedish
                                        </option>
                                    
                                
                                    
                                        <option value="CH">
                                            Swiss
                                        </option>
                                    
                                
                                    
                                        <option value="SY">
                                            Syrian
                                        </option>
                                    
                                
                                    
                                        <option value="TW">
                                            Taiwanese
                                        </option>
                                    
                                
                                    
                                        <option value="TJ">
                                            Tajik/Tadjik
                                        </option>
                                    
                                
                                    
                                        <option value="TZ">
                                            Tanzanian
                                        </option>
                                    
                                
                                    
                                        <option value="TH">
                                            Thai
                                        </option>
                                    
                                
                                    
                                        <option value="TL">
                                            East Timorese
                                        </option>
                                    
                                
                                    
                                        <option value="TG">
                                            Togolese
                                        </option>
                                    
                                
                                    
                                        <option value="TK">
                                            Tokelauan
                                        </option>
                                    
                                
                                    
                                        <option value="TO">
                                            Tongan
                                        </option>
                                    
                                
                                    
                                        <option value="TT">
                                            TrinidadianTobagan/Tobagonian
                                        </option>
                                    
                                
                                    
                                        <option value="TN">
                                            Tunisian
                                        </option>
                                    
                                
                                    
                                        <option value="TR">
                                            Turkish
                                        </option>
                                    
                                
                                    
                                        <option value="TM">
                                            Turkmen/Turkoman
                                        </option>
                                    
                                
                                    
                                        <option value="TC">
                                            Turks and Caicos Islander/Wallies
                                        </option>
                                    
                                
                                    
                                        <option value="TV">
                                            Tuvaluan
                                        </option>
                                    
                                
                                    
                                        <option value="UG">
                                            Ugandan
                                        </option>
                                    
                                
                                    
                                        <option value="UA">
                                            Ukrainian
                                        </option>
                                    
                                
                                    
                                        <option value="AE">
                                            UAE/Emirates/Emirati
                                        </option>
                                    
                                
                                    
                                        <option value="GB">
                                            UK/British
                                        </option>
                                    
                                
                                    
                                        <option value="US">
                                            US
                                        </option>
                                    
                                
                                    
                                
                                    
                                        <option value="UY">
                                            Uruguayan
                                        </option>
                                    
                                
                                    
                                        <option value="UZ">
                                            Uzbek
                                        </option>
                                    
                                
                                    
                                        <option value="VU">
                                            Vanuatuan
                                        </option>
                                    
                                
                                    
                                
                                    
                                        <option value="VE">
                                            Venezuelan
                                        </option>
                                    
                                
                                    
                                        <option value="VN">
                                            Vietnamese
                                        </option>
                                    
                                
                                    
                                        <option value="VG">
                                            Virgin Islander
                                        </option>
                                    
                                
                                    
                                
                                    
                                        <option value="WF">
                                            Wallisian/Futunan
                                        </option>
                                    
                                
                                    
                                        <option value="EH">
                                            Western Saharan
                                        </option>
                                    
                                
                                    
                                        <option value="YE">
                                            Yemeni
                                        </option>
                                    
                                
                                    
                                        <option value="ZM">
                                            Zambian
                                        </option>
                                    
                                
                                    
                                        <option value="ZW">
                                            Zimbabwean
                                        </option>
                                    
                                
                                    
                                
                            </select>
"""
soup = BeautifulSoup(html, features="lxml")
# subject_options = [i.findAll('option') for i in soup.findAll('select', attrs = {'name': 'countryCode'} )]

subject_options = soup.findAll(
    lambda t: t.name == 'option' and t.parent.attrs.get('name') == 'countryCode'
)

country = pd.DataFrame(subject_options, columns=['option'])
country['country_code']=country['option'].map(lambda x: re.search(r"""value="(\w+)">""", str(x)).group(1) if re.search(r"""value="(\w+)">""", str(x)) is not None else None)
country['country_name']=country['option'].map(lambda x: re.search(r"""value="\w+">(.*)<\/option>""", str(x)).group(1) if re.search(r"""value="\w+">(.*)<\/option>""", str(x)) is not None else None)
country['country_name_lower'] = country['country_name'].map(lambda x: x.lower().strip())