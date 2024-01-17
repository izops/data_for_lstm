# %%
dtfAnnotations = dtfAnnotations.head(100000)

# %%
# filter data by the first available hash
dtfFilter = dtfAnnotations[dtfAnnotations['hash'] == dtfAnnotations['hash'].iloc[0]]

lstStarts = dtfFilter['start'].to_list()
lstEnds = dtfFilter['end'].to_list()

strText = dtfAnnotations['text'].iloc[0]

# %%

def lstSplitPoints(plstStarts: list, plstEnds: list, pintTextLen: int) -> list:
    # initialize starting values
    intStart = -1
    intEnd = -1
    lstSplits = []

    # create a continuous list of splits and indexes of labels
    for intCounter, intIndex in enumerate(plstStarts):
        # label gap at the beginning
        if intEnd < plstStarts[intCounter] - 1:
            # calculate the indexes
            intStart = intEnd + 1
            intEnd = plstStarts[intCounter] - 1

            # assign indicator
            intFlag = 0

            # append to the main list
            lstSplits.append([intStart, intEnd, intFlag])

        # neighboring labels, current start index succeeds previous end
        if intEnd == plstStarts[intCounter] - 1:
            # calculate the indexes
            intStart = plstStarts[intCounter]
            intEnd = plstEnds[intCounter]

            # assign indicator
            intFlag = 1

            # append to the main list
            lstSplits.append([intStart, intEnd, intFlag])

        # end sequence
        if intCounter + 1 == len(plstStarts) and pintTextLen > len(plstStarts):
            # calculate the indexes
            intStart = intEnd + 1
            intEnd = pintTextLen

            # assign indicator
            intFlag = 0

            # append to the main list
            lstSplits.append([intStart, intEnd, intFlag])

    return lstSplits

# %%

# split string by the split points
def lstSplitString(pstrString: str, plstSplitPoints: list) -> list:
    # initialize the return list
    lstParts = []

    # split the string by the split points
    for lstSplits in plstSplitPoints:
        # get the part of the string
        strPart = pstrString[lstSplits[0]:(lstSplits[1] + 1)]

        # append the part to the list of parts
        lstParts.append(strPart)

    return lstParts

# %%

# tokenize the data, calculate lengths and return new positions
def tplNewPositions(pobjTokenizer, plstString, plstSplitPoints):
    # tokenize the data
    lstTokenized = pobjTokenizer.texts_to_sequences(plstString)

    # initialize the index values
    intStart = -1
    intEnd = -1

    # initialize lists to remember indexes
    lstStart = []
    lstEnd = []

    # measure lengths
    for intIndex, lstTokens in enumerate(lstTokenized):
        # recalculate start and end of the tokenized string
        intStart = intEnd + 1
        intEnd = intStart + len(lstTokens)

        # remember the start and end position if flagged
        print(intIndex)
        if plstSplitPoints[intIndex][2] == 1:
            lstStart.append(intStart)
            lstEnd.append(intEnd)

    return lstStart, lstEnd

# %%

lstTexts = dtfText['text'].head().unique().tolist()
lstHashes = dtfText['hash'].head().unique().tolist()

for strText, intHash in zip(lstTexts, lstHashes):
    # obtain the start and end positions
    lstStarts = dtfAnnotations['start'][dtfAnnotations['hash'] == intHash].to_list()
    lstEnds = dtfAnnotations['end'][dtfAnnotations['hash'] == intHash].to_list()

    # calculate split points
    lstSplits = lstSplitPoints(lstStarts, lstEnds, len(strText))

    # split string by the splitting points
    lstParts = lstSplitString(strText, lstSplits)

    # obtain new start and end indexes
    lstNewStart, lstNewEnd = tplNewPositions(objTokenizer, strText, lstSplits)

    # add the indexes to the data frame
    dtfAnnotations['new_start'][dtfAnnotations['hash'] == intHash] = lstNewStart
    dtfAnnotations['new_end'][dtfAnnotations['hash'] == intHash] = lstNewEnd
# %%
# do the character tokenization instead