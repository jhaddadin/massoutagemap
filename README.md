<h1>Massachusetts power outage map</h1>
This python script uses power outage data reported to the Massachusetts Emergency Management Agency by National Grid, Unitil and Eversource to power a live map of outages across Massachusetts. MEMA's power outage data is updated every 15 minutes. The script is hosted on the Heroku cloud platform, using a scheduler that executes the code at regular intervals.

The map is hosted on Carto. It can be embedded by iframe, or directly using Carto's javascript tools. This script also feeds the data into a Google Spreadsheet, which can be used to populate a Google Fusion Table,  Tableau project, etc. See a live example online at [metrowestdailynews.com](https://www.metrowestdailynews.com/news/20180310/live-map-massachusetts-power-outages).

Before using the script, it's necessary to obtain OAuth2 credentials from Google and save the json file containing your credentials into the same folder as the script.
