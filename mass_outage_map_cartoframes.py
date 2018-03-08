
# coding: utf-8

# In[1]:


import cartoframes
import geopandas as gpd
import requests
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe


# In[2]:


def get_file_contents(filename):
    try:
        with open(filename, 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        print("'%s' file not found" % filename)


# In[3]:


filename = 'carto_api_key.txt'
APIKEY = get_file_contents(filename)


# In[4]:


cc = cartoframes.CartoContext(base_url='https://jhaddadin.carto.com/',
                              api_key=APIKEY)


# In[5]:


data = requests.get("http://mema.mapsonline.net/power_outage_public.geojson")
outages = gpd.GeoDataFrame(data.json())


# In[6]:


outages_df = gpd.GeoDataFrame.from_features(outages['features'])


# In[7]:


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


# In[8]:


twoproviders_full = outages_df[outages_df.town.isin(twoproviders_dict.keys())]
oneprovider_full = outages_df[~outages_df.town.isin(twoproviders_dict.keys())]


# In[9]:


oneprovider = oneprovider_full[['county', 'last_update', 'town', 'total_cust', 'no_power', 'pct_nopow', 'utility', 'notes']]


# In[10]:


twoproviders = twoproviders_full.groupby(['county', 'last_update', 'town', 'total_cust'], as_index=False)['no_power', 'pct_nopow'].sum()


# In[11]:


for index, row in twoproviders.iterrows():
    utility = twoproviders_dict[row['town']]
    notes = "; ".join(twoproviders_full[twoproviders_full['town'] == row['town']]['notes'].unique())
    twoproviders.at[index,'utility'] = utility
    twoproviders.at[index,'notes'] = notes


# In[12]:


alloutages = oneprovider.append(twoproviders, ignore_index=True)


# In[13]:


towns = gpd.read_file('C:/Data/massoutagemap/shapefile/mass_cities_towns.shp')
towns = towns[['TOWN_ID', 'TOWN', 'geometry']]


# In[14]:


alloutages_poly = towns.merge(alloutages, left_on='TOWN', right_on='town')
alloutages_poly = alloutages_poly[['town', 'county', 'no_power', 'total_cust', 'pct_nopow', 'last_update', 'utility', 'notes', 'geometry']]


# In[15]:


for index, row in alloutages_poly.iterrows():
    display_num = float(row['pct_nopow'] * 100)
    alloutages_poly.at[index,'pct_display'] =  "%.1f" % round(display_num,1)


# In[16]:


cc.write(alloutages_poly,
          encode_geom=True,
          table_name='mass_outages',
          overwrite=True)


# #### Update Google spreadsheet

# In[17]:


scope = ['https://spreadsheets.google.com/feeds']
credentials = ServiceAccountCredentials.from_json_keyfile_name('My Project-cd8f0fca74a6.json', scope)
gc = gspread.authorize(credentials)
wks = gc.open("Massachusetts power outage data").sheet1


# In[18]:


alloutages_google = alloutages_poly[['town', 'pct_display', 'no_power', 'total_cust']]


# In[19]:


for index, row in alloutages_google.iterrows():
    pct_display_str = str(row['pct_display']) + "%"
    alloutages_google.at[index,'pct_display_str'] =  pct_display_str


# In[20]:


alloutages_google = alloutages_google[['town', 'pct_display_str', 'no_power', 'total_cust']]


# In[21]:


alloutages_google = alloutages_google.rename(columns={'town': 'City/Town',
                                                      'pct_display_str': '% without power',
                                                      'no_power': 'Outages',
                                                      'total_cust': 'Total customers'})


# In[22]:


alloutages_google = alloutages_google.append({'City/Town': 'State',
                                    '% without power': '',
                                    'Outages': alloutages_google['Outages'].sum(),
                                    'Total customers': ''}, ignore_index=True)


# In[23]:


alloutages_google = alloutages_google.sort_values(by='Outages', ascending=False)


# In[24]:


set_with_dataframe(wks, alloutages_google)

