# -*- coding: UTF-8 -*-
"""
https://gist.github.com/mciantyre/32ff2c2d5cd9515c1ee7
"""
from bs4 import BeautifulSoup
import re
import csv

with open('25k______2.kml', 'r',  encoding="utf8") as f:
    s = BeautifulSoup(f, 'xml')
    pm = s.find_all('Placemark')
    with open('out.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow('id,longitude,latitude'.split(',') )
        for idx, rs in enumerate(pm):
            print('-------------------------------------------------------------------------')
            # print(rs)
            raw = ('%s, %s' % (rs.find_all('value'), rs.find_all('coordinates')))
            id = re.findall(r'>(\d*?)<', raw)
            lnglat = re.findall(r'ates>(.*),0<\/coord', raw)
            writer.writerow(('%s,%s' % (id[0], lnglat[0])).split(','))

            print('-------------------------------------------------------------------------')
            # if idx==10:
            #     break
