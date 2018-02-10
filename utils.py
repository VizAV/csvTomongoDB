from datetime import datetime
import json
def convertDictToDataTypes(validator):

    for collections in validator['collections']:
        collectionData = list(collections.values())[0]
        for columns in collectionData:
            columnData = list(columns.values())[0]
            columnData["type"] =eval(columnData["type"])
            if columnData["type"]==list:
                columnData["element"]["type"]=eval(columnData["element"]["type"])
    return validator
def getValidatorColumns(validator):
    validatorColumns=[]
    for collection in validator['collections']:
        for columns in list(collection.values())[0]:
            validatorColumns.append(columns)
        # for keyColumn, valueColumn in valueCollection.items():
        #     validatorColumns.update({keyColumn:valueColumn})

    return validatorColumns

# def getValidatorDict():
def main():
    validator = json.load(open('/home/ynos/mongo_validator/data/validator.json'))
    validator = convertDictToDataTypes(validator)
    print(validator)
    validatorColumns = getValidatorColumns(validator)
    print(validatorColumns)
if __name__=='__main__':
    main()



