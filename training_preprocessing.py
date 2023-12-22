# %% imports
import pandas as pd
import os
import json
import string

# %% definitions

# paths
strPathJSON = 'c:/repositories/zzz_data_for_lstm/data/outputs/'

# file name
strExtension = '.json'

# functions
def dtfJSONtoDataFrame(pobjJSONData: dict) -> pd.DataFrame:
    """Process ingested JSON data to appropriate form for pandas data frame.

    Inputs:
        - pobjJSONData - data read in from a JSON file

    Outputs:
        - dtfOut - processed data frame that contains text column with the
        source data, annotations column with all tokens, start column with
        integer value of the starting position of the token, and end column
        with integer value of the ending position of the token
    """

    assert type(pobjJSONData) == dict, 'Must be a dictionary from JSON input'

    # process annotations first, normalize the text labels
    dtfAnnotations = pd.json_normalize(
        pobjJSONData,
        'annotations',
        meta='text',
        sep='_'
    )

    # process the text data
    dtfText = pd.DataFrame(pobjJSONData)

    # drop annotations from the full table
    dtfText.drop('annotations', axis=1, inplace=True)

    # merge the two datasets to the final one
    dtfOut = pd.merge(dtfText, dtfAnnotations, on='text')

    return dtfOut

def strCleanUpText(pstrText: str) -> str:
    """Clean up the text from punctuation.
    
    Inputs:
        - pstrText - original text to clean up

    Outputs:
        - strOut - text cleaned of all punctuation
    """

    assert type(pstrText) == str, 'String input required'

    # define replacement dictionary
    dctReplace = str.maketrans('', '', string.punctuation)

    # remove punctuation from the input string
    strOut = pstrText.translate(dctReplace)

    return strOut

# %% data processing

# initialize an empty data frame for storing the data
dtfData = pd.DataFrame()

# import available JSON files and clean them up
for strFile in os.listdir(strPathJSON):
    # process only JSON files
    if strFile.endswith('.json') and os.path.isfile(strPathJSON + strFile):
        # open the file and ingest the data
        with open(strPathJSON + strFile) as objFile:
            dctData = json.load(objFile)
        
        # consolidate the ingested data
        dtfProcessing = dtfJSONtoDataFrame(dctData)

        # append the processed dataset to the main data frame
        dtfData = pd.concat(
            [dtfData, dtfProcessing],
            axis=0,
            ignore_index=True
        )