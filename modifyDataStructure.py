from math import isnan
import pandas as pd
import numpy as np
def startupInfo(collectionDetails,Dataframe):

    columns = [list(key.keys())[0] for key in collectionDetails]
    recordList=[]
    for index, row in Dataframe[columns].iterrows():

        # Convert each row in the dataframe in to a dict
        record = row.to_dict()
        record['ID']=record['startupID']
        # For cases where lat and lon is not null, add a new element called location
        if not isnan(record["Lat"]) and not isnan(record["Lon"]):
            record['Location'] = [record["Lat"], record["Lon"]]

        # Delete lat and lon for all the cases. They wont feature in the database
        del record['Lon']
        del record['Lat']
        # record_clean = filter(lambda k: not isnan(record[k]), record)
        # record = {k: record[k] for k in record if not isnan(record[k])}

        recordList.append(record)
    return recordList

def fundingInfo(collectionDetails,Dataframe):
    columns = [list(key.keys())[0] for key in collectionDetails]
    fundingCols = ['startupName'] + ['startupID'] + columns
    fundingInfo = Dataframe[Dataframe['roundDate'].notnull()][fundingCols]

    roundInfoList = []
    for index, row in fundingInfo.iterrows():
        for roundData in range(len(row['roundDate'])):
            roundInfo = []
            roundInfo.append(row['startupName'])
            roundInfo.append(row['startupID'])
            for col in columns:
                if type(row[col]) is list:

                    if roundData < len(row[col]):
                        info = row[col][roundData]
                    else:
                        info = np.nan
                else:
                    info = np.nan

                roundInfo.append(info)
            roundInfoList.append(pd.Series(roundInfo, index=fundingCols))

    roundInfoDF = pd.DataFrame(roundInfoList)
    groupedRoundInfo = roundInfoDF.groupby(['startupID', 'roundDate'])

    roundGroupedList = []
    counter = 0
    for key, item in groupedRoundInfo:
        value = item.groupby(['startupID']).agg({
            'investorName': lambda x: list(x),
            'roundInvestmentAmount': 'first',
            'roundDate': 'first',
            'startupName': 'first',
            'equityValuation':'first'
        }).reset_index(['startupID'])
        counter += 1
        value['ID'] = 'round' + str("%04d" % counter)
        roundGroupDict = dict([(key, val[0]) for key, val in value.items()])
        roundGroupedList.append(roundGroupDict)
    return roundGroupedList



# Dictionary having the modification function
modifyCollection = {"startupInfo": startupInfo, "fundingInfo": fundingInfo}