"""
https://github.com/googlemaps/google-maps-services-python
"""
import googlemaps

# gmaps = googlemaps.Client(key="AIzaSyB-BusfjoV9eTd8xc3XDP-43lgDf618S5s")
gmaps = googlemaps.Client(key="AIzaSyB3sfjY5f3S71g_5QOJbqpqaLB6zrcJBnQ")

# Geocoding an address
geocode_result = gmaps.geocode('17th Floor City Tower,40 Basinghall Street,London')

geocode_result[0]['geometry']['location']['lat']
geocode_result[0]['geometry']['location']['lng']
