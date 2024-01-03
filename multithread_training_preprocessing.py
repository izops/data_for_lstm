# %% imports
import pandas as pd
import os
import json
import string
import concurrent.futures
import datetime

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

def dtfProcessJSON(pstrPath: str):
    """Import and process a JSON file from the path to a required form.
    
    Inputs:
        - pstrPath - full path to a JSON file

    Outputs:
        - dtfProcessing - pandas data frame containing the processed JSON file,
        the start and end tokens indexes adjusted for stripping the punctuation
    """
    assert os.path.isfile(pstrPath), 'Input must be a path to a file.'

    # initialize an empty data frame
    dtfProcessing = pd.DataFrame()

    try:
        # process only JSON files
        if pstrPath.endswith('.json'):
            # open the file and ingest the data
            with open(pstrPath) as objFile:
                dctData = json.load(objFile)
            
            # consolidate the ingested data
            dtfProcessing = dtfJSONtoDataFrame(dctData)

    except Exception as e:
        print(f'Error in JSON processing: {e}')

    return dtfProcessing

def dtfMergeDataFrames(plstDataFrames: list) -> pd.DataFrame:
    """Join all data frames contained in a list to a single data frame.

    Inputs:
        - plstDataFrames - list of pandas data frames with the same column
        structure

    Outputs:
        - dtfOut - pandas data frame of concatenated data frames
    """
    assert type(plstDataFrames) == list, 'Input must be a list of data frames'

    # concatenate all data frames from the input list to a single data frame
    dtfOut = pd.concat(plstDataFrames, ignore_index=True)

    return dtfOut

def dtfThreading(pstrPath: str) -> pd.DataFrame:
    """Import JSON files in multithreaded process.
    
    Inputs:
        - pstrPath - path to the JSON files folder

    Outputs:
        - dtfOut - pandas data frame containing all imported JSON files
        concatenated under each other
    """
    # get all json files from the given directory
    lstJSONFiles = [
        os.path.join(
            pstrPath,
            strFile
        ) for strFile in os.listdir(pstrPath) if strFile.endswith('.json')
    ]
    
    # initialize the list of outputs
    lstOutputs = []

    # initialize file counter
    intCount = 0

    with concurrent.futures.ThreadPoolExecutor() as objExecutor:
        lstFutures = [
            objExecutor.submit(
                dtfProcessJSON,
                strPath
            ) for strPath in lstJSONFiles
        ]

    for objFuture in concurrent.futures.as_completed(lstFutures):
        try:
            # get result of data reading
            dtfResult = objFuture.result()

            # append the result to the final list
            lstOutputs.append(dtfResult)

        except Exception as e:
            # print error message
            print(f'Error processing file: {e}')

        # increment the counter
        intCount += 1

        if intCount % 1000 == 0:
             # get time
            strTime = str(datetime.datetime.now())

            # print message
            print(f'\t{strTime}: Files processed: {intCount}')

    # merge all data frames together
    dtfOut = dtfMergeDataFrames(lstOutputs)

    return dtfOut

# %% run the import process
if __name__ == '__main__':
    print(datetime.datetime.now())
    dtfImport = dtfThreading(strPathJSON)
    print(datetime.datetime.now())

# %% post process the imported data

strTime = str(datetime.datetime.now())
print(f'{strTime} Start')

# replace punctuation from the original input text
dtfImport['clean_text'] = dtfImport['text'].apply(
    strCleanUpText
)

strTime = str(datetime.datetime.now())
print(f' - {strTime} Text cleaned up')

# recalculate position of the labels after the cleanup
dtfImport['start_fix'] = dtfImport.apply(
    lambda row: row['start'] - sum(
        [
            1 for char in row['text'][:row['start']] \
            if char in string.punctuation
        ]
    ),
    axis=1
)

strTime = str(datetime.datetime.now())
print(f' - {strTime} Label start recalculated')

dtfImport['end_fix'] = dtfImport.apply(
    lambda row: row['end'] - sum(
        [
            1 for char in row['text'][:row['end']] \
            if char in string.punctuation
        ]
    ),
    axis=1
)

strTime = str(datetime.datetime.now())
print(f' - {strTime} Label end recalculated')

# drop redundant columns
dtfImport.drop(['text', 'start', 'end'], axis=1, inplace=True)

strTime = str(datetime.datetime.now())
print(f'{strTime} End')