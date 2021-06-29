
import pandas as pd
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 500)
pd.set_option('display.width', 1000)
import re
from bs4 import BeautifulSoup
html = """
<select name="genderTitle" id="genderTitle" onchange="" class="form-control" title="">
                                <option value="Mr.">Mr.</option>
                                <option value="Mrs.">Mrs.</option>
                                <option value="Ms.">Ms.</option>
                                <option value="Miss.">Miss.</option>
                                <option value="Dr.">Dr.</option>
                                <option value="Captain">Captain</option>
                            </select>
"""
soup = BeautifulSoup(html, features="lxml")
# subject_options = [i.findAll('option') for i in soup.findAll('select', attrs = {'name': 'countryCode'} )]

subject_options = soup.findAll(
    lambda t: t.name == 'option' and t.parent.attrs.get('name') == 'genderTitle'
)
print (subject_options)
gender_title = pd.DataFrame(subject_options, columns=['option'])
gender_title['gender_code']=gender_title['option'].map(lambda x: re.search(r"""value="(.+)">""", str(x)).group(1) if re.search(r"""value="(.+)">""", str(x)) is not None else None)
gender_title['gender_display']=gender_title['option'].map(lambda x: re.search(r"""value=".+">(.*)<\/option>""", str(x)).group(1) if re.search(r"""value=".+">(.*)<\/option>""", str(x)) is not None else None)
gender_title['gender_display_lower'] = gender_title['gender_display'].map(lambda x: x.lower().strip().replace('.',''))