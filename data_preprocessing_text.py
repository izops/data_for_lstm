# %% library import
import keras
import pandas as pd
import os
import datetime

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

# print timestamp
strTime = str(datetime.datetime.now())
print(f'{strTime} Process start')

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

# print timestamp
strTime = str(datetime.datetime.now())
print(f'{strTime} Tokenization finished')

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

# print timestamp
strTime = str(datetime.datetime.now())
print(f'{strTime} Padding finished')

# merge the padded sequences to the annotations data frame
dtfAnnotations = pd.merge(
    dtfAnnotations,
    dtfText[['hash', 'sequence']],
    how='left',
    on=['hash']
)

# print timestamp
strTime = str(datetime.datetime.now())
print(f'{strTime} Merging finished')

# save the output as a backup
dtfAnnotations.to_parquet(
    os.path.join(strDataPath, strOutputName),
    compression='snappy'
)

# print timestamp
strTime = str(datetime.datetime.now())
print(f'{strTime} Saving finished')