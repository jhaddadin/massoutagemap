import cartoframes
import geopandas as gpd
import requests
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe

def get_file_contents(filename):
    try:
        with open(filename, 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        print("'%s' file not found" % filename)

filename = 'carto_api_key.txt'
APIKEY = get_file_contents(filename)

cc = cartoframes.CartoContext(base_url='https://jhaddadin.carto.com/',
                              api_key=APIKEY)

data = requests.get("http://mema.mapsonline.net/power_outage_public.geojson")
outages = gpd.GeoDataFrame(data.json())

outages_df = gpd.GeoDataFrame.from_features(outages['features'])

twoproviders_dict = {"NORTHAMPTON":"National Grid / Eversource - Western MA",
                     "SHUTESBURY":"National Grid / Eversource - Western MA",
                     "LEOMINSTER":"National Grid / UNITIL",
                     "WILBRAHAM":"National Grid / Eversource - Western MA",
                     "HANCOCK":"National Grid / Eversource - Western MA",
                     "BELLINGHAM":"National Grid / Eversource - Eastern MA",
                     "HAWLEY":"National Grid / Eversource - Western MA",
                     "EASTON":"National Grid / Eversource - Easton MA",
                     "HEATH":"National Grid / Eversource - Western MA",
                     "BELCHERTOWN":"National Grid / Eversource - Western MA",
                     "CHARLEMONT":"National Grid / Eversource - Western MA",
                     "ERVING":"National Grid / Eversource - Western MA",
                     "SHIRLEY":"National Grid / UNITIL",
                     "SCITUATE":"National Grid / Eversource - Eastern MA",
                     "SHARON":"Eversource - Eastern MA / National Grid",
                     "WESTPORT":"Eversource - Eastern MA / National Grid",
                     "WENDELL":"National Grid / Eversource - Western MA",
                     "CHESHIRE":"National Grid / Eversource - Western MA",
                     "GRANBY":"National Grid / Eversource - Western MA",
                     "LENOX":"National Grid / Eversource - Western MA"}

twoproviders_full = outages_df[outages_df.town.isin(twoproviders_dict.keys())]
oneprovider_full = outages_df[~outages_df.town.isin(twoproviders_dict.keys())]

oneprovider = oneprovider_full[['county', 'last_update', 'town', 'total_cust', 'no_power', 'pct_nopow', 'utility', 'notes']]
twoproviders = twoproviders_full.groupby(['county', 'last_update', 'town', 'total_cust'], as_index=False)['no_power', 'pct_nopow'].sum()

for index, row in twoproviders.iterrows():
    utility = twoproviders_dict[row['town']]
    notes = "; ".join(twoproviders_full[twoproviders_full['town'] == row['town']]['notes'].unique())
    twoproviders.at[index,'utility'] = utility
    twoproviders.at[index,'notes'] = notes

alloutages = oneprovider.append(twoproviders, ignore_index=True)

towns = gpd.read_file('shapefile/mass_cities_towns.shp')
towns = towns[['TOWN_ID', 'TOWN', 'geometry']]

alloutages_poly = towns.merge(alloutages, left_on='TOWN', right_on='town')
alloutages_poly = alloutages_poly[['town', 'county', 'no_power', 'total_cust', 'pct_nopow', 'last_update', 'utility', 'notes', 'geometry']]

for index, row in alloutages_poly.iterrows():
    display_num = float(row['pct_nopow'] * 100)
    alloutages_poly.at[index,'pct_display'] =  "%.1f" % round(display_num,1)

cc.write(alloutages_poly,
          encode_geom=True,
          table_name='mass_outages',
          overwrite=True)

#### Update Google spreadsheet

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('My Project-cd8f0fca74a6.json', scope)
gc = gspread.authorize(credentials)
wks = gc.open("Massachusetts power outage data").sheet1

alloutages_google = alloutages_poly[['town', 'pct_display', 'no_power', 'total_cust']]

for index, row in alloutages_google.iterrows():
    pct_display_str = str(row['pct_display']) + "%"
    alloutages_google.at[index,'pct_display_str'] =  pct_display_str

alloutages_google = alloutages_google[['town', 'pct_display_str', 'no_power', 'total_cust']]

alloutages_google = alloutages_google.rename(columns={'town': 'City/Town',
                                                      'pct_display_str': '% without power',
                                                      'no_power': 'Outages',
                                                      'total_cust': 'Total customers'})

alloutages_google = alloutages_google.append({'City/Town': 'State',
                                    '% without power': '',
                                    'Outages': alloutages_google['Outages'].sum(),
                                    'Total customers': ''}, ignore_index=True)

alloutages_google = alloutages_google.sort_values(by='Outages', ascending=False)
set_with_dataframe(wks, alloutages_google)