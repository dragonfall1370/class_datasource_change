"""
https://gist.github.com/mciantyre/32ff2c2d5cd9515c1ee7
"""
from bs4 import BeautifulSoup
import csv

def process_coordinate_string(str):
    """
    Take the coordinate string from the KML file, and break it up into [Lat,Lon,Lat,Lon...] for a CSV row
    """
    ret = []
    comma_split = str.split(',') if str is not None else ''
    return [comma_split[1], comma_split[0]]

def main():
    """
    Open the KML. Read the KML. Open a CSV file. Process a coordinate string to be a CSV row.
    """
    with open('ex.kml', 'r') as f:
        s = BeautifulSoup(f, 'xml')
        with open('out.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for coords in s.find_all('coordinates'):
                writer.writerow(process_coordinate_string(coords.string))
            for coords in s.find_all('name'):
                writer.writerow(process_coordinate_string(coords.string))

if __name__ == "main":
    with open('ex.kml', 'r') as f:
        s = BeautifulSoup(f, 'xml')
        pm = s.find_all('Placemark')
        for rs in pm:
            print('-------------------------------------------------------------------------')
            # print(rs)
            rs.find_all('name')
            print('-------------------------------------------------------------------------')
