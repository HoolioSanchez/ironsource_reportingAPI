#%%
from request import ironsource
from apps import apps
import pandas as pd
import numpy as np
from credentials import credentials
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

start_date = '2019-06-01'
end_date = '2019-06-30'
placementAdRevFilename = 'export (68).csv'
days_in_month = 30
irn = ironsource()
#%%
def getAllAppData(start_date, end_date, breakdown):
    tempDataFrame = pd.DataFrame()
    for app in apps: 
        data = irn.getMediationData(start_date, end_date, apps[app], breakdown)
        tempDataFrame = tempDataFrame.append(data, ignore_index=True)
    return tempDataFrame

#%%
col_target = 'data'
def extractDataFromDFColumn(dataframe):
    col = [item for sublist in dataframe['data'] for item in sublist]

    lens = dataframe[col_target].apply(len)
    vals = range(dataframe.shape[0])
    ilocations = np.repeat(vals, lens)

    cols = [i for i,c in enumerate(dataframe.columns) if c != col_target]
    new_df = dataframe.iloc[ilocations, cols].copy()
    new_df[col_target] = col

    #Cleaning columns
#     new_df['Impressions'] = [imp['impressions'] for imp in new_df.data]
    new_df['clicks'] = [imp['clicks'] for imp in new_df.data]
    new_df['clickThroughRate'] = [imp['clickThroughRate'] for imp in new_df.data]
    # new_df['VideoCompletions'] = [imp['videoCompletions'] for imp in new_df.data]
    new_df['engagedUsers'] = [imp['engagedUsers'] for imp in new_df.data]
    new_df['engagementRate'] = [imp['engagementRate'] for imp in new_df.data]
    new_df['impressionsPerEngagedUser'] = [imp['impressionsPerEngagedUser'] for imp in new_df.data]

    new_df = new_df.drop('data', axis = 1)

    return new_df

#%%
data = getAllAppData(start_date, end_date, breakdown = 'placement')

#%%
placementRevenue = pd.DataFrame.from_csv(placementAdRevFilename)
placementRevenue = placementRevenue.reset_index()

cleanData = extractDataFromDFColumn(data)
cleanData = cleanData.drop('bundleId', axis = 1)
cleanData = cleanData.reset_index()

#%%
cleanDF = cleanData
cleanDF['Date'] = pd.to_datetime(cleanDF['date'])
cleanDF = cleanDF.set_index('Date')
cleanDF['appkey'] = cleanDF['appKey']

#%%
monthlyDF = cleanDF.groupby(['appName','appkey','placement']).resample('m').mean()
monthly_sum = cleanDF.groupby(['appName','appkey','placement']).resample('m').sum()
monthlyDF.reset_index(inplace=True)
monthly_sum.reset_index(inplace=True)
monthlyDF['placement'] = monthlyDF['placement'].str.lower()
monthly_sum['placement'] = monthly_sum['placement'].str.lower()

monthlyDF['avg_engagedUsers'] = monthlyDF['engagedUsers']
monthlyDF['avg_clicks'] = monthlyDF['clicks']

monthlyDF.drop('engagedUsers', axis = 1, inplace=True)
monthlyDF.drop('clicks', axis = 1, inplace=True)

#%%
monthlyDF['engagedUsers'] = monthly_sum['engagedUsers']
monthlyDF['clicks'] = monthly_sum['clicks']


#%%
placementDF = pd.merge(placementRevenue,monthlyDF, on=['placement','appkey'])
#%%
# placementDF['AppName'] = [app for item in placementDF['appkey'] for app in apps if item == apps[app]]

placementDF.head()
#%%
finalDF = placementDF[['appName', 'placement','market','revenue','clicks','clickThroughRate','engagedUsers','engagementRate','Impression','impressionsPerEngagedUser','eCPMs']]

#%%
finalDF['ARPDEU'] = (finalDF['revenue']/days_in_month)/finalDF['avg_engagedUsers']

#%%
jsonData = finalDF.to_json(orient='records')
cleanJson = json.loads(jsonData)

#%%
print(cleanJson)

#%%
config = credentials['googleSheets']
scope = ['https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive']

googlecredentials = ServiceAccountCredentials.from_json_keyfile_name(config, scope)
gc = gspread.authorize(googlecredentials)
wks = gc.open('Ironsource Placement Report').sheet1

def addRowToGoogleSheets(data):
    wks.append_row(data, value_input_option='USER_ENTERED' )
#%%
def filterRows(index):
    rows = [index['appName'], index['placement'], index['market'], index['revenue'], index['eCPMs'], index['ARPDEU'], index['Impression'], index['clicks'], index['clickThroughRate'], index['engagedUsers'], index['engagementRate'], index['impressionsPerEngagedUser']]
    return rows

def appendRow(data):
    for index in data: 
        rows = filterRows(index)
        addRowToGoogleSheets(rows)

#%%
finalDF.to_csv('placementData.csv')
# appendRow(cleanJson)
#%%
