import pandas as pd
import json
import urllib.request
from datetime import datetime
from io import StringIO

def _getEndpoints():
    urlNodeEnpoint = 'https://species.vamdc.org/web-service/api/v12.07/nodes'
    urlSpeciesEntpoint = 'https://species.vamdc.org/web-service/api/v12.07/species'
    return urlNodeEnpoint, urlSpeciesEntpoint


def getNodeHavingSpecies():
    urlNodeEnpoint, _ =_getEndpoints()
    responseNode = urllib.request.urlopen(urlNodeEnpoint)
    json_list = responseNode.read()

    data = json.loads(json_list)
    df_nodes = pd.json_normalize(data)
    return df_nodes


def getAllSpecies():
    _, urlSpeciesEndpoint = _getEndpoints()
    AllSpeciedDF, df_nodes = _getChemicalInfoFromEnpoint(urlSpeciesEndpoint)
    return AllSpeciedDF, df_nodes


def getSpeciesWithSearchCriteria(text_search = None, stoichiometric_formula = None, mass_min = None, mass_max = None, charge_min = None, charge_max = None, type = None, ivo_identifier = None, inchikey = None, name = None, structural_formula = None):
    
    _, urlSpeciesEndpoint = _getEndpoints()
    
    urlSuffix = "?"
    urlSuffix = (urlSuffix+"text_search="+text_search+"&") if text_search is not None else urlSuffix
    urlSuffix = (urlSuffix+"stoichiometric_formula="+stoichiometric_formula+"&") if stoichiometric_formula is not None else urlSuffix
    urlSuffix = (urlSuffix+"ivo_identifier="+ivo_identifier+"&") if ivo_identifier is not None else urlSuffix
    urlSuffix = (urlSuffix+"inchikey="+inchikey+"&") if inchikey is not None else urlSuffix
    urlSuffix = (urlSuffix+"name="+name+"&") if name is not None else urlSuffix
    urlSuffix = (urlSuffix+"structural_formula="+structural_formula+"&") if structural_formula is not None else urlSuffix

    if type == "molecule" or type == "atom":
        urlSuffix = urlSuffix + "type="+type + "&"


    # testing the difference mass_max-mass_min to be positive 
    maxMinDifference = mass_max-mass_min if (mass_max is not None and mass_min is not None) else 0
    if maxMinDifference < 0:
        raise Exception("The difference (mass_max - mass_min) must be positive")

    # testing the difference charge_max-charge_min to be positive 
    maxMinDifference = charge_max - charge_min if (charge_max is not None and charge_min is not None) else 0
    if maxMinDifference < 0: 
        raise Exception("The difference (charge_max - charge_min) must be positive")

    urlSuffix = (urlSuffix+"mass_max="+str(mass_max)+"&") if mass_max is not None else urlSuffix
    urlSuffix = (urlSuffix+"mass_min="+str(mass_min)+"&") if mass_min is not None else urlSuffix

    urlSuffix = (urlSuffix+"charge_max="+str(charge_max)+"&") if charge_max is not None else urlSuffix
    urlSuffix = (urlSuffix+"charge_min="+str(charge_min)+"&") if charge_min is not None else urlSuffix

    fullUrl = urlSpeciesEndpoint + urlSuffix

    species_df , node_df = _getChemicalInfoFromEnpoint(fullUrl)
    
    return species_df, node_df

    
  


def getAllSpeciesInExcelFile(filePath):
    AllSpeciesDF, df_nodes = getAllSpecies()
    
    now = datetime.now()
    date_time = now.strftime("%Y%m%d_%H_%M")
    file_name = filePath+"/SpeciesDatabase"+date_time+".xlsx"

    writer = pd.ExcelWriter(file_name, engine='openpyxl')

    # convert the DataFrame to an Excel object
    AllSpeciesDF.to_excel(writer, index=False, sheet_name='species')

    # get the workbook and worksheet objects
    workbook = writer.book
    worksheet = writer.sheets['species']

    # iterate over the columns and set the width of each column based on the maximum length of the data in that column
    for i, column in enumerate(AllSpeciesDF.columns):
        column_width = max(AllSpeciesDF[column].astype(str).map(len).max(), len(column))
        worksheet.column_dimensions[worksheet.cell(row=1, column=i+1).column_letter].width = column_width

    #convert the Node DataFrame to an Excel object
    df_nodes.to_excel(writer, index=False, sheet_name='nodes')
    workbook = writer.book
    worksheet = writer.sheets['nodes']

    # iterate over the columns and set the width of each column based on the maximum length of the data in that column
    for i, column in enumerate(df_nodes.columns):
        column_width = max(df_nodes[column].astype(str).map(len).max(), len(column))
        worksheet.column_dimensions[worksheet.cell(row=1, column=i+1).column_letter].width = column_width

    # save the Excel file
    #import warnings
    #warnings.filterwarnings("ignore")
    writer.close()
    print("The excel file "+ file_name+" has been successfully created.")
    return file_name





def _getChemicalInfoFromEnpoint(specificSpeciesEndpoint):
    response = urllib.request.urlopen(specificSpeciesEndpoint)
    data = json.loads(response.read())

    # wrapping the results into a unique Pandas Dataframe (called AllSpeciesDF)
    dataFrames = []
    AllSpeciesDF = None
    for key, value in data.items():
        datum = json.dumps(value)
        df = pd.read_json(StringIO(datum), orient='records')
        df.insert(0,"ivoIdentifier", str(key))
        dataFrames.append(df)

    AllSpeciesDF = pd.concat(dataFrames)

    # getting a second dataframe with the information of Data-Nodes having chemical data
    df_nodes = getNodeHavingSpecies()

    # merging the raw chemical information with the ones from the Data-Nodes
    AllSpeciesDF = AllSpeciesDF.merge(df_nodes[['ivoIdentifier', 'shortName', 'tapEndpoint']], on='ivoIdentifier', how='left')

    col = AllSpeciesDF.pop('shortName')  # Remove the column and store it in col
    AllSpeciesDF.insert(0, 'shortName', col)  # Insert the column at the beginning

    # Split the column 'lastSeenDateTime' and expand into two separate columns
    AllSpeciesDF['lastIngestionScriptDate']= AllSpeciesDF["lastSeenDateTime"].apply(lambda x: x.split('||')[0])
    AllSpeciesDF['speciesLastSeenOn']= AllSpeciesDF["lastSeenDateTime"].apply(lambda x: x.split('||')[1])

    # Drop the original 'lastSeenDateTime' column
    AllSpeciesDF = AllSpeciesDF.drop('lastSeenDateTime', axis=1)

    return AllSpeciesDF, df_nodes


def main():

    getAllSpeciesInExcelFile("/home/zwolf")
    df_nodes = getNodeHavingSpecies()
    

 #   row_indices = [0, 2, 4]
    ## Filtering the DataFrame
#    filtered_df = df_nodes.iloc[row_indices]

#    print(filtered_df["ivoIdentifier"].to_list)


if __name__=='__main__':
    main()
