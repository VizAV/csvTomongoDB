from inputData import inputData

import json

def main():

    #read the config file
    config = json.load(open('config.json'))

    #initalte the class
    try:
        dataClass=inputData(config)
    except FileNotFoundError as e:
        print(e.__str__())
        exit()





    # Treating the file to adhere to the given formats
    dataClass.treatColsDataType()

    # for col in dataClass.Dataframe.columns:
    #     print(col, type(dataClass.Dataframe.loc[0,col]))

    # For each collection modify the startup Details
    for collections in dataClass.validator['collections']:
        collectionName = list(collections.keys())[0]
        collectionDetails = list(collections.values())[0]

        #Modify database
        infoDict = dataClass.modifyInfo(collectionName,collectionDetails)

        dataClass.updateDatabase(collectionName,infoDict)

    #Validate and filter the non conforming data. Let us not bother too much now
    # startupData.validatedData()

    #change the structure of startupInfo

    # startupInfoList = startupData.modifyStartupInfo()
    #
    # insert data in collection
    # startupData.updateDatabase('startupInfo',startupInfoList)

    # change the structure of startupInfo
    # fundingInfoList = startupData.modifyFundingInfo()

    # insert data in collection
    # startupData.updateDatabase('fundingInfo', fundingInfoList)

    print("Finished updating the database")
    print()

if __name__=='__main__':
    main()