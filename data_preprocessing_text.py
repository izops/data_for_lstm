# %% library import
import logging
import keras
import numpy as np
import pandas as pd
from scipy.sparse import lil_matrix
from sklearn.preprocessing import LabelEncoder
import os
import datetime

# %% set up logging
logging.basicConfig(
    level = logging.INFO,
    format=' %(asctime)s -  %(levelname)s -  %(message)s'
)

# %% definitions
# path to processed data files
strDataPath = 'c:/repositories/zzz_data_for_lstm/data/outputs/backup/'

# processed text data file name
strDataName = 'text.parquet'

# output file name
strOutputName = 'sequences.parquet'

# %% data import
# import data
dtfText = pd.read_parquet(os.path.join(strDataPath, 'text.parquet'))
dtfAnnotations = pd.read_parquet(
    os.path.join(strDataPath, 'annotations.parquet')
)

# %% text data tokenization

# timestamp
logging.info('Process start')

# extract texts to a list
lstTexts = dtfText['text'].tolist()

# initialize the tokenizer for character tokenization with unknown token
objTokenizer = keras.preprocessing.text.Tokenizer(
    char_level=True,
    oov_token='UNK',
)

# fit the tokenizer to the data
objTokenizer.fit_on_texts(lstTexts)

# convert the text to sequences of integers
lstSequences = objTokenizer.texts_to_sequences(lstTexts)

# timestamp
logging.info('Tokenization finished')

# find maximum sequence length
intMax = max(len(lstSeq) for lstSeq in lstSequences)

# pad sequences to the maximum length using the entire dataset
arrPaddedSequences = keras.preprocessing.sequence.pad_sequences(
    lstSequences,
    maxlen=intMax,
    padding='post'
)

# convert padded sequences to the list
lstPadded = arrPaddedSequences.tolist()

# add the padded sequences to the original data frame
dtfText['sequence'] = lstPadded

# timestamp
logging.info('Padding finished')

# merge the padded sequences to the annotations data frame
dtfAnnotations = pd.merge(
    dtfAnnotations,
    dtfText[['hash', 'sequence']],
    how='left',
    on=['hash']
)

# timestamp
logging.info('Merging finished')

# %% encode labels
# extract labels
lstLabels = dtfAnnotations['label'].to_list()

# initialize the encoder
objEncoder = LabelEncoder()

# fit the encoder on the data
objEncoder.fit(lstLabels)

# put the encoded labels to the original data frame
dtfAnnotations['label_encoded'] = objEncoder.transform(lstLabels)

# timestamp
logging.info('Encoding finished')

# %% output sequences

# timestamp
logging.info('Process start')

# initialize a sparse matrix for output sequences
sparseOutSeq = lil_matrix((len(dtfAnnotations), intMax), dtype=int)

# extract start, end, and encoded labels to NumPy arrays
arrStart = np.array(dtfAnnotations['start'])
arrEnd = np.array(dtfAnnotations['end'])
arrLabels = np.array(dtfAnnotations['label_encoded'])

# timestamp
logging.info('Loading to numpy finished')

# prepare output sequences using vectorization
for intIndex in range(arrStart.shape[0]):
    sparseOutSeq[
        intIndex,
        arrStart[intIndex]:(arrEnd[intIndex] + 1)
    ] = arrLabels[intIndex]

    if intIndex % 100000 == 0:
        # get time
        strTime = str(datetime.datetime.now())

        # print message
        print(f'{strTime} Files processed: {intIndex}')

# timestamp
logging.info('Output sequences preparation finished')

# convert the sparse matrix to a numpy array
arrOutSeq = sparseOutSeq.toarray()