This script scrapes power outage data reported to the Massachusetts Emergency Management Agency by National Grid, Unitil and Eversource and feeds the data into a Google Spreadsheet. The spreadsheet can be used to populate a Fusion Table or Tableau project to update a live chloropleth map of power outages by city and town.

The project uses Selenium to load a web browser and navigate to the power outage data on the MEMA website. It uses BeautifulSoup to parse the HTML. Power outage figures are saved into a pandas dataframe, then uploaded to the Google spreadsheet.

Before using the script, it's necessary to obtain OAuth2 credentials from Google and save the json file containing your credentials into the same folder as the script. You will also need ChromeDriver.exe to use Selenium with Google Chrome.