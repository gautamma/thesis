#Libraries and files
import ee
ee.Initialize()
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
polygons = gpd.read_file(r'/Users/gautammathur/Downloads/Irrigation_CT/Irri_CT_P3.shp')
#Ammending Irrigation column to extract irrigation
irr = polygons.plot3 #change, depending on plot1, plot2, plot3
polygons["Irr"] = irr.map(lambda irr: int(irr[8:9])) #slicing information specific to file. Change if using script for something else. 
#Making GID
gid = pd.Series(range(len(polygons)))
def make_id(s):
    return int("3" + str(s)) #Replace whether it is P1, P2, or P3
polygons["GID"] = gid.apply(lambda s: make_id(s))
#Changing LineString to Polygons using shapely coordinates
s = polygons.geometry
s = s.map(lambda s: Polygon(s.coords))
polygons['geometry'] = s
#output list for later
zonaldicts = []
#turn every shapely polygon into an ee polygon (probably should've used a map function for this)
def make_ee_poly(poly):
    coords = []
    for i in poly.exterior.coords:
        coords.append([*i])
    return ee.Geometry.Polygon(coords)
eepolys = polygons.geometry
eepolys = eepolys.map( lambda eepolys: make_ee_poly(eepolys))
polygons["eepoly"] = eepolys

#Use ee polys to make image collections, and extract zonal stats. Ammend start/end dates, bands, and parent image collection if necessary
for eepol in eepolys:
    harmon = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED").filterDate("2022-11-01", "2023-06-30").filterBounds(eepol).select(['B2', "B3", "B4", "B5", "B6", "B7", "B8", "B8A"]).filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 30))
    def zstats(img):
        dict = img.reduceRegion(ee.Reducer.mean(), eepol, 10)
        date = img.date().format("YYYY-MM-dd")  # Get date as a string
        return img.set(dict).set("date", date)
    withMean = harmon.map(zstats)
    zonalbands = {}
    for band in ['B2', "B3", "B4", "B5", "B6", "B7", "B8", "B8A", "date"]:
        zonalbands[band] = pd.Series(withMean.aggregate_array(band).getInfo())
    zonaldicts.append(zonalbands)

#Add details from parent dataframe to zonal stats dicts
for i in range(len(zonaldicts)):
    zonaldicts[i]['Irr'] = pd.Series([polygons.Irr[i]]*len(zonaldicts[i]['B2']))
    zonaldicts[i]['SDate'] = pd.Series([polygons.sowing[i]]*len(zonaldicts[i]['B2']))
    zonaldicts[i]['Farmer'] = pd.Series([polygons.fName[i]]*len(zonaldicts[i]['B2']))
    zonaldicts[i]['GID'] = pd.Series([polygons.GID[i]]*len(zonaldicts[i]['B2']))

#Turn list of dicts into a pandas series, so that dicts can be turned into DFs fast. Use broadcasting to apply pd.concat on everything at the same 
#time, making final zonal stats df
zonaldicts = pd.Series(zonaldicts)
zonaldicts = zonaldicts.apply(lambda s: pd.DataFrame(s))
finaltable = pd.concat(list(zonaldicts))

    



