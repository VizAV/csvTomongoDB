from utils import convertDictToDataTypes,getValidatorColumns
from datetime import datetime
import pandas as pd
import json
import numpy as np
import ast
import pymongo
from modifyDataStructure import modifyCollection
from math import isnan
class inputData:
    # Dataframe
    def __init__(self,config):

        self.db = self.initiateDB(config['mongodb']['db'])

        self.Dataframe = self.readDataFile(config['dataFile']['path'],config['dataFile']['filename'])

        self.validator = self.readValidatorFile(config['validatorFile']['path'],config['validatorFile']['filename'])


    def readValidatorFile(self,path,filename):
        try:
            File = json.load(open(path+filename))
            #convert the text into datatype
            validatorDict=convertDictToDataTypes(File)

            print("Read validator File")
            print()
            return validatorDict
        except FileNotFoundError:
            raise




    def readDataFile(self,path,filename):
        try:
            dataFrame = pd.read_csv(path+filename, encoding='ISO-8859-1')


            print("Read input data file")
            print()
            return dataFrame
        except FileNotFoundError:
            raise



    def treatColsDataType(self):

        # Get the columns from the validator file but only consider the ones that are available in the dataframe
        columns = [col for col in getValidatorColumns(self.validator) if list(col.keys())[0] in self.Dataframe]

        print("The following columns were present in the input datafile. ")
        print()
        for index,col in enumerate(columns):
            print(index+1,'.',list(col.keys())[0])
        print()
        print(" Treating the columns. ")
        print()
        # self.treatEmptyCols(columns)

        # Convert the strings to list inside the class
        self.convertStringsToLists(columns)

        # Convert the strings to datetime inside the class
        self.convertStringsToTimeStamp(columns)

        # Convert the strings to datetime inside the class
        self.convertStringsToListofTimestamps(columns)

    # def treatEmptyCols(self,columns):
    #
    #     listCol = [key for key, val in columns.items() if
    #                val["type"] == list and val["element"]["type"] is not datetime]
    #     for col in listCol:
    #         self.DataFrame[col].fillna('[]',inplace=True)

    def convertStringsToLists(self,allColumns):
        listCol = [list(col.keys())[0]
                    for col in allColumns
                        if list(col.values())[0]["type"] == list and
                           list(col.values())[0]["element"]["type"] is not datetime]

        for col in listCol:
            # for each row split string to list for rows that are not null. ast.literal eval will fail for null values

            self.Dataframe[col] = self.Dataframe[col].apply(lambda x: ast.literal_eval(x) if pd.notnull(x) else x)
    def convertStringsToTimeStamp(self,allColumns):
        # # Get the columns that have list as the type in the collection
        dateCol = [list(col.keys())[0]
                    for col in allColumns
                        if list(col.values())[0]["type"] == datetime]
        # For each column in the list whose type is 'date'
        for col in dateCol:
            for index, row in self.Dataframe[self.Dataframe[col].notnull()].iterrows():
                self.Dataframe.loc[index, col] = datetime.strptime(row[col][:10], '%Y-%m-%d')

    def convertStringsToListofTimestamps(self,allColumns):
        # # Get the columns that have list of datetimes as the type in the collection
        listDatecol = [list(col.keys())[0]
                    for col in allColumns
                        if list(col.values())[0]["type"] == list and
                           list(col.values())[0]["element"]["type"] is datetime]

        for col in listDatecol:

            self.Dataframe[col] = self.Dataframe[col].str.replace("'', ", "").str.replace(", ''", "").str.replace(
                "''", "").str.replace(
                "Timestamp\(", "").str.replace("\)", "").str.replace("\[\]", "").replace('', np.nan)

            self.Dataframe[col] = self.Dataframe[col].apply(lambda x: ast.literal_eval(x) if pd.notnull(x) else x)

            for index, row in self.Dataframe[self.Dataframe[col].notnull()].iterrows():
                # del self.treatedData.loc[index, col][:]
                # self.treatedData.loc[index, col]=self.treatedData.loc[index, col].apply(list)
                for elem in range(len(row[col])):
                    self.Dataframe.loc[index, col][elem] = datetime.strptime(row[col][elem][:10], '%Y-%m-%d')

    def updateDatabase(self,colName,inputList):
        print("Pushing ",colName," info into Database...")
        print()
        for inputDict in inputList:
            self.db[colName].update({"_id": inputDict["ID"]}, {'$set': inputDict}, upsert=True)

    def initiateDB(self,dbName):
        # Connect to DB. Work on the exception handlers. They dont seem to work
        print("Connecting to database...")
        print()
        try:
            conn = pymongo.MongoClient()
            print("Connected successfully!!!")
            print()
        except pymongo.errors.ConnectionFailure as e:
            print("Could not connect to MongoDB: %s" % e)
            print()

        return conn[dbName]



    def modifyInfo(self,collectionName,collectionDetails):

        recordList = modifyCollection[collectionName](collectionDetails,self.Dataframe)

        return recordList


    # def modifyStartupInfo(self):
    #     cols = [key for key, val in outputDataFormat().startupInfo.items()]
    #     recordList=[]
    #     for index, row in self.DataFrame[cols].iterrows():
    #
    #         # Convert each row in the dataframe in to a dict
    #         record = row.to_dict()
    #         record['ID']=record['startupID']
    #         # For cases where lat and lon is not null, add a new element called location
    #         if not isnan(record["Lat"]) and not isnan(record["Lon"]):
    #             record['Location'] = [record["Lat"], record["Lon"]]
    #
    #         # Delete lat and lon for all the cases. They wont feature in the database
    #         del record['Lon']
    #         del record['Lat']
    #         # record_clean = filter(lambda k: not isnan(record[k]), record)
    #         # record = {k: record[k] for k in record if not isnan(record[k])}
    #
    #         recordList.append(record)
    #     return recordList
    #
    # def modifyFundingInfo(self):
    #     cols = [key for key, val in outputDataFormat().fundingInfo.items()]
    #
    #     # Strip down the dataframe in to another with each round info as a row
    #
    #     fundingCols = ['startupName'] + ['startupID'] + cols
    #     fundingInfo = self.DataFrame[self.DataFrame['roundDate'].notnull()][fundingCols]
    #
    #     roundInfoList = []
    #     for index, row in fundingInfo.iterrows():
    #         for roundData in range(len(row['roundDate'])):
    #             roundInfo = []
    #             roundInfo.append(row['startupName'])
    #             roundInfo.append(row['startupID'])
    #             for col in cols:
    #                 if type(row[col]) is list:
    #
    #                     if roundData < len(row[col]):
    #                         info = row[col][roundData]
    #                     else:
    #                         info = np.nan
    #                 else:
    #                     info = np.nan
    #
    #                 roundInfo.append(info)
    #             roundInfoList.append(pd.Series(roundInfo, index=fundingCols))
    #
    #     roundInfoDF = pd.DataFrame(roundInfoList)
    #     groupedRoundInfo = roundInfoDF.groupby(['startupID', 'roundDate'])
    #
    #     roundGroupedList = []
    #     counter = 0
    #     for key, item in groupedRoundInfo:
    #         value = item.groupby(['startupID']).agg({
    #             'investorName': lambda x: list(x),
    #             'roundInvestmentAmount': 'first',
    #             'roundDate': 'first',
    #             'startupName': 'first',
    #             'equityValuation':'first'
    #         }).reset_index(['startupID'])
    #         counter += 1
    #         value['ID'] = 'round' + str("%04d" % counter)
    #         roundGroupDict = dict([(key, val[0]) for key, val in value.items()])
    #         roundGroupedList.append(roundGroupDict)
    #     return roundGroupedList
    #







