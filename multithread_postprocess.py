# %% imports
import pandas as pd
import numpy as np
import concurrent.futures
import string
import datetime

# %% definitions

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

def dtfProcessData(pdtfData: pd.DataFrame, pobjCounter) -> pd.DataFrame:
    # replace punctuation from the original input text
    pdtfData['clean_text'] = pdtfData['text'].apply(
        strCleanUpText
    )

    # recalculate position of the labels after the cleanup
    pdtfData['start_fix'] = pdtfData.apply(
        lambda row: row['start'] - sum(
            [
                1 for char in row['text'][:row['start']] \
                if char in string.punctuation
            ]
        ),
        axis=1
    )

    pdtfData['end_fix'] = pdtfData.apply(
        lambda row: row['end'] - sum(
            [
                1 for char in row['text'][:row['end']] \
                if char in string.punctuation
            ]
        ),
        axis=1
    )

    # drop redundant columns
    pdtfData.drop(['text', 'start', 'end'], axis=1, inplace=True)

    # increment counter
    pobjCounter.value += 1

    return pdtfData

def dtfParallelization(pdtfData, pintChunkSize):
    # split the data into chunks
    lstChunks = np.array_split(
        pdtfData,
        range(pintChunkSize, len(pdtfData), pintChunkSize)
    )

    # paralellize with use of manager for shared counter
    with concurrent.futures.ProcessPoolExecutor() as objExecutor:
        lstProcessed = list(objExecutor.map(dtfProcessData, lstChunks))

    dtfOut = pd.concat(lstProcessed, ignore_index=True)

    return dtfOut
#%%
if __name__ == '__main__':
    chunk_size = 10000

    
    strTime = str(datetime.datetime.now())
    print(f'{strTime} Start')

    dtfResult = dtfParallelization(dtfImport, chunk_size)
    
    strTime = str(datetime.datetime.now())
    print(f'{strTime} End')