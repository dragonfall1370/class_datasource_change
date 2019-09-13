
import pandas as pd
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 500)
pd.set_option('display.width', 1000)
import re
from bs4 import BeautifulSoup
html = """
<ul id="j-11" class="ui-menu ui-widget ui-widget-content" role="menu" tabindex="0" style="max-height: 25rem; overflow-y: auto; display: block;" aria-activedescendant="ui-id-108"><li class="ui-menu-item" id="ui-id-100" tabindex="-1" role="menuitem"><a href="#">Applied</a></li><li class="ui-menu-item" id="ui-id-101" tabindex="-1" role="menuitem"><a href="#">1st Contact Attempt</a></li><li class="ui-menu-item" id="ui-id-102" tabindex="-1" role="menuitem"><a href="#">2nd Contact Attempt</a></li><li class="ui-menu-item" id="ui-id-103" tabindex="-1" role="menuitem"><a href="#">3rd Contact Attempt</a></li><li class="ui-menu-item" id="ui-id-104" tabindex="-1" role="menuitem"><a href="#">Reviewed</a></li><li class="ui-menu-item" id="ui-id-105" tabindex="-1" role="menuitem"><a href="#">A Possibility</a></li><li class="ui-menu-item" id="ui-id-106" tabindex="-1" role="menuitem"><a href="#">Shortlist</a></li><li class="ui-menu-item" id="ui-id-107" tabindex="-1" role="menuitem"><a href="#">Submitted to Client</a></li><li class="ui-menu-item" id="ui-id-108" tabindex="-1" role="menuitem"><a href="#">Candidate completing test</a></li><li class="ui-menu-item" id="ui-id-109" tabindex="-1" role="menuitem"><a href="#">Client Interview</a></li><li class="ui-menu-item" id="ui-id-110" tabindex="-1" role="menuitem"><a href="#">Offered</a></li><li class="ui-menu-item" id="ui-id-111" tabindex="-1" role="menuitem"><a href="#">Placed</a></li><li class="ui-menu-item" id="ui-id-112" tabindex="-1" role="menuitem"><a href="#">Unable To Reach Candidate</a></li><li class="ui-menu-item" id="ui-id-113" tabindex="-1" role="menuitem"><a href="#">Unsuitable-General</a></li><li class="ui-menu-item" id="ui-id-114" tabindex="-1" role="menuitem"><a href="#">Unsuitable-$$$ Too High</a></li><li class="ui-menu-item" id="ui-id-115" tabindex="-1" role="menuitem"><a href="#">Unsuitable-Location</a></li><li class="ui-menu-item" id="ui-id-116" tabindex="-1" role="menuitem"><a href="#">Unsuitable-Availability</a></li><li class="ui-menu-item" id="ui-id-117" tabindex="-1" role="menuitem"><a href="#">Unsuitable-Already forward</a></li><li class="ui-menu-item" id="ui-id-118" tabindex="-1" role="menuitem"><a href="#">Withdrew Application</a></li><li class="ui-menu-item" id="ui-id-119" tabindex="-1" role="menuitem"><a href="#">Unsuccessful-After Meeting</a></li><li class="ui-menu-item" id="ui-id-120" tabindex="-1" role="menuitem"><a href="#">Poor Resume-Delete</a></li><li class="ui-menu-item" id="ui-id-121" tabindex="-1" role="menuitem"><a href="#">Unsuccessful-After Client reviewed</a></li><li class="ui-menu-item" id="ui-id-122" tabindex="-1" role="menuitem"><a href="#">Unsuccessful - After Test</a></li><li class="ui-menu-item" id="ui-id-123" tabindex="-1" role="menuitem"><a href="#">Unsuccessful-After Interview</a></li><li class="ui-menu-item" id="ui-id-124" tabindex="-1" role="menuitem"><a href="#">Reference Checking</a></li><li class="ui-menu-item" id="ui-id-125" tabindex="-1" role="menuitem"><a href="#">Role Withdrawn by Client</a></li><li class="ui-menu-item" id="ui-id-126" tabindex="-1" role="menuitem"><a href="#">Role now Filled</a></li><li class="ui-menu-item" id="ui-id-127" tabindex="-1" role="menuitem"><a href="#">Be AWARE-Read notes</a></li><li class="ui-menu-item" id="ui-id-128" tabindex="-1" role="menuitem"><a href="#">Citizen</a></li><li class="ui-menu-item" id="ui-id-129" tabindex="-1" role="menuitem"><a href="#">Awaiting Citizen</a></li><li class="ui-menu-item" id="ui-id-130" tabindex="-1" role="menuitem"><a href="#">PR -4 Years Or Less</a></li></ul>
"""
soup = BeautifulSoup(html, features="lxml")
# subject_options = [i.findAll('option') for i in soup.findAll('select', attrs = {'name': 'countryCode'} )]

subject_options = soup.findAll(
    lambda t: t.name == 'li'
)

partern = r">([-a-zA-Z0-9 \$]+)<"
country = pd.DataFrame(subject_options, columns=['li'])
country['value'] = country['li'].map(lambda x: re.search(partern, str(x)).group(1) if re.search(partern, str(x)) else None)
country.to_csv("delete.csv", index=False)