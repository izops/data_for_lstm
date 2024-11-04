# %%
import logging
import numpy as np
import os
import tensorflow as tf

# %% set up logging
logging.basicConfig(
    level = logging.INFO,
    format=' %(asctime)s -  %(levelname)s -  %(message)s'
)

# %% definitions
# path to processed data files
strDataPath = 'c:/repositories/zzz_data_for_lstm/data/outputs/backup/'

# %% import files
logging.info('Process start')

arrX = np.load(os.path.join(strDataPath, 'X.npy'), allow_pickle=True)
arry = np.load(os.path.join(strDataPath, 'y.npy'), allow_pickle=True)

logging.info('Data loaded')

# %%
# calculate the vocabulary size
intVocabSize = len(np.unique(arrX))

# calculate input length
intInputLength = arry.shape[1]

# calculate number of entity types, subtract one for non-entity identificator
intNumTypes = len(np.unique(arry)) - 1

# %% design the model

model = tf.keras.models.Sequential([
    tf.keras.layers.Embedding(
        input_dim=intVocabSize,
        output_dim=64,
        input_length=intInputLength
    ),
    tf.keras.layers.LSTM(50, return_sequences=True),
    tf.keras.layers.Dense(intNumTypes, activation='softmax')
])