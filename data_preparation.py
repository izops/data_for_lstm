# %% imports
import random
import numpy as np
import pandas as pd
import datetime
import json
import logging

# %% set up logging
logging.basicConfig(
    level = logging.INFO,
    format=' %(asctime)s -  %(levelname)s -  %(message)s'
)

# %% paths and definitions

# number of files to generate
intFiles = 50000

# number of available templates
intTemplates = 11

# tax rate
intTaxRate = 0.2

# paths
strPathData = 'c:/repositories/zzz_data_for_lstm/data/inputs/'
strPathTemplates = 'c:/repositories/zzz_data_for_lstm/templates/'
strPathOutputs = 'c:/repositories/zzz_data_for_lstm/data/outputs/'

# file names
strTemplateName = 'template'
strTemplateExt = '.txt'

# invoice tokens
strTokCompany = '[my-company]'
strTokStreet = '[my-address-street]'
strTokCity = '[my-address-city]'
strTokZip = '[my-address-zip]'
strTokPhone = '[my-contact-phone]'
strTokEmail = '[my-contact-email]'
strTokDateIn = '[date-invoice]'
strTokDateDue = '[date-due]'
strTokClient = '[client]'
strTokClientStreet = '[client-address-street]'
strTokClientCity = '[client-address-city]'
strTokClientZip = '[client-address-zip]'
strTokInvoiceNo = '[invoice-no]'
strTokItemsStart = '[list-item-start]'
strTokItemsEnd = '[list-item-end]'
strTokItem = '[item-name]'
strTokQ = '[qty]'
strTokR = '[rate]'
strTokA = '[amount]'
strTokS = '[subtotal]'
strTokTax = '[tax]'
strTokTotal = '[total]'

lstTokens = [
    strTokCompany,
    strTokStreet,
    strTokCity,
    strTokZip,
    strTokPhone,
    strTokEmail,
    strTokDateIn,
    strTokDateDue,
    strTokClient,
    strTokClientStreet,
    strTokClientCity,
    strTokClientZip,
    strTokInvoiceNo,
    strTokItemsStart,
    strTokItemsEnd,
    strTokItem,
    strTokQ,
    strTokR,
    strTokA,
    strTokS,
    strTokTax,
    strTokTotal
]

# functions
def strGetRandomTemplate(pintSampleSize: int) -> str:
    """Return text saved in one of the possible templates.
    
    Inputs:
        - pintSampleSize - number of available templates to choose from

    Outputs:
        - strSingleLine - one line string containing contents of a random
        template
    """
    
    assert type(pintSampleSize) == int, 'The sample size must be an integer'
    assert pintSampleSize > 0, 'There must be more than 1 template available'

    # generate a number to specify the template
    intTemplate = random.randint(1, pintSampleSize)

    # define the full name of the file to import
    strFile = strPathTemplates + strTemplateName + str(intTemplate)
    strFile += strTemplateExt

    # open and read in the file
    objTemplate = open(strFile, 'r')
    lstContents = objTemplate.readlines()

    # join the contents to a single line string
    strSingleLine = ''.join(lstContents)

    return strSingleLine

def tplFindEarliestToken(pstrText: str, plstTokens: list) -> tuple:
    """Return position of the earliest occurence of any of the tokens, together
    with the token itself.
    
    Inputs:
        - pstrText - text to search for a token
        - plstTokens - list of tokens to look for

    Outputs:
        - tplPosition - tuple of the earliest token and its position,
        if no token is found, (-1, None) is returned
    """

    assert type(pstrText) == str
    assert type(plstTokens) == list

    # initialize position and token values
    tplPosition = (-1, None)

    for strToken in plstTokens:
        # locate the token in the string
        intPosition = pstrText.find(strToken)

        # remember the token if found on an earlier position
        if intPosition >= 0:
            # change the return position and token if earlier or first found
            if intPosition < tplPosition[0]:
                tplPosition = (intPosition, strToken)
            elif tplPosition[0] == -1:
                tplPosition = (intPosition, strToken)

        # add a break optimization in case no token start found in
        # the remaining part of the string
        if not '[' in pstrText[:intPosition]:
            break

        # logging
        # strLog = 'tplFindEarliestToken : \n'
        # strLog += 'strToken: ' + strToken + '\n'
        # strLog += 'intPosition: ' + str(intPosition) + '\n'
        # strLog += '-'*32 + '\n'
        # logging.debug(strLog)

    return tplPosition

def strCreateJSONLabel(pstrValue: str) -> str:
    """Convert string to required JSON label.
    
    Inputs:
        - pstrValue - value to convert

    Outputs:
        - strOut - cleaned up value
    """

    assert type(pstrValue) == str

    # replace spaces and hyphens with underscores
    strOut = pstrValue.replace('-', '_')
    strOut = strOut.replace(' ', '_')

    # remove other characters
    strRemove = '!@#$%^&*()+-=[]\\{}|;\':",./<>?'
    strOut = ''.join([x.lower() for x in strOut if x not in strRemove])

    return strOut

def strRandomDate() -> str:
    """Generate random date and return it in dd/mm/yyyy format as string.
    
    Inputs:
        - None

    Outputs:
        - strDate - a valid random date between 01/01/2020 and 31/12/2030
        in dd/mm/yyyy format
    """

    # generate random year
    intYear = random.randint(2020, 2030)

    # generate random month
    intMonth = random.randint(1, 12)

    # generate random day based on the year and month
    if intMonth in [1, 3, 5, 7, 8, 10, 12]:
        intDay = random.randint(1, 31)
    elif intMonth in [4, 6, 9, 11]:
        intDay = random.randint(1, 30)
    else:
        # generate february date for leap year
        if (intYear % 4 == 0 and intYear % 100 != 0) or (intYear % 400 == 0):
            intDay = random.randint(1, 29)
        else:
            intDay = random.randint(1, 28)
    
    # create the date
    dteRandomDate = datetime.date(intYear, intMonth, intDay)

    # convert the date to string
    strDate = dteRandomDate.strftime('%d/%m/%Y')

    return strDate

def strRandomizeTemplateItems(pstrTemplate: str) -> str:
    """Extend text of the template by random number of items.
    
    Inputs:
        - pstrTemplate - template string to modify, it must contain start and
        end tokens that define the part of the template that will be repeated

    Outputs:
        - template extended by a random number of items    
    """

    # verify the presence of the start and end tokens
    assert strTokItemsStart in pstrTemplate, 'Missing item list start token'
    assert strTokItemsEnd in pstrTemplate, 'Missing item list end token'

    # find the substring that represents the item list
    intStart = pstrTemplate.find(strTokItemsStart)
    intEnd = pstrTemplate.find(strTokItemsEnd)

    # exclude start token from the requested string
    intStart = intStart + len(strTokItemsStart)

    # extract template sections
    strPreList = pstrTemplate[:intStart]
    strList = pstrTemplate[intStart:intEnd] + '\n'
    strPostList = pstrTemplate[intEnd:]

    # get a random number of repetitions
    random.seed(11)
    intRepeat = random.randint(1, 20)

    # prepare the modified template
    strTemplateOut = strPreList + (strList * intRepeat) + strPostList

    return strTemplateOut

# %% import data

dtfCity = pd.read_csv(strPathData + 'cities.csv', encoding='latin-1')
dtfCompany = pd.read_csv(strPathData + 'company.csv', encoding='latin-1')
dtfItems = pd.read_csv(strPathData + 'items.csv', encoding='latin-1')
dtfName = pd.read_csv(strPathData + 'name.csv', encoding='latin-1')
dtfStreet = pd.read_csv(strPathData + 'street.csv', encoding='latin-1')
dtfPhone = pd.read_csv(
    strPathData + 'phone.csv',
    encoding='latin-1',
    dtype={'Phone': str}
)

# %% generate annotations
for intCount in range(intFiles):
    # get a random template to annotate
    strText = strGetRandomTemplate(intTemplates)

    # create random list of invoice items
    strText = strRandomizeTemplateItems(strText)

    # initialize annotation dictionary
    dctJSON = {
        'text': '',
        'annotations': []
    }

    # initialize position variables
    intEnd = -1
    intListStart = -1
    intListEnd = -1

    # initialize variables for templates that don't require this information
    intSubtotal = 0
    intTax = 0
    intQuantity = 1
    intRate = 1
    intAmount = 1

    # get first token
    intStart, strToken = tplFindEarliestToken(strText, lstTokens)
    logging.debug(
        'Initial position and token: \n' + str(intStart) + '\n' + strToken + \
        '\n' + '-' * 16 +'\n'
    )

    # do this for all tokens
    while intStart >= 0:
        # based on token generate the appropriate data to replace it
        if strToken in [strTokClient, strTokCompany]:
            # shuffle company data and get first observation
            dtfRandomCompany = dtfCompany.apply(np.random.permutation)
            strReplace = dtfRandomCompany.iloc[0][0]
            strReplace += ' ' + dtfRandomCompany.iloc[0][1]

        elif strToken in [strTokCity, strTokClientCity]:
            # shuffle city data and get first observation
            dtfRandom = dtfCity.apply(np.random.permutation)
            strReplace = dtfRandom.iloc[0][0]

        elif strToken in [strTokStreet, strTokClientStreet]:
            # shuffle street data and get first observation
            dtfRandom = dtfStreet.apply(np.random.permutation)
            strReplace = dtfRandom.iloc[0][0]

        elif strToken in [strTokZip, strTokClientZip]:
            # generate zip code
            intZip = random.randint(100000, 199999)
            strZip = str(intZip)
            strReplace = strZip[1:]

        elif strToken == strTokPhone:
            # shuffle phone data and get first observation
            dtfRandom = dtfPhone.apply(np.random.permutation)
            strReplace = dtfRandom.iloc[0][0]

        elif strToken in [strTokDateIn, strTokDateDue]:
            # generate random date
            strReplace = strRandomDate()

        elif strToken == strTokEmail:
            # shuffle name data and generate email from name and company info
            dtfRandom = dtfName.apply(np.random.permutation)
            strReplace = dtfRandom.iloc[0][0] + '.' + dtfRandom.iloc[0][1]
            strReplace += '@' + dtfRandomCompany.iloc[0][0] 
            strReplace += dtfRandomCompany.iloc[0][1] + '.com'

        elif strToken == strTokInvoiceNo:
            # generate a random invoice number
            intInvoiceNum = random.randint(10000, 99999999)
            strReplace = str(intInvoiceNum)

        elif strToken == strTokItemsStart:
            # remember the starting position of the item list
            intListStart = intStart

            # replace the token with empty string
            strReplace = ''

        elif strToken == strTokItemsEnd:
            # remember the ending position of the item list
            intListEnd = intStart

            # replace the token with empty string
            strReplace = ''

        elif strToken == strTokItem:
            # shuffle phone data and get first observation
            dtfRandom = dtfItems.apply(np.random.permutation)
            strReplace = dtfRandom.iloc[0][0]

        elif strToken == strTokQ:
            # generate random quantity and store it for amount calculation
            intQuantity = random.randint(1, 20)
            strReplace = str(intQuantity)

        elif strToken == strTokR:
            # generate random unit price and store it for amount calculation
            intRate = random.randint(100, 2000)
            strReplace = str(intRate)

        elif strToken == strTokA:
            # calculate the total amount
            intAmount = intQuantity * intRate

            # increment subtotal
            intSubtotal += intAmount

            # create a value for token replacement
            strReplace = str(intAmount)

        elif strToken == strTokS:
            # use calculated subtotal as a replacement
            strReplace = str(intSubtotal)

        elif strToken == strTokTax:
            # use pre-defined tax value for tax calculation
            intTax = round(intTaxRate * intSubtotal, 2)
            strReplace = str(intTax)

        elif strToken == strTokTotal:
            # sum the subtotal and tax
            strReplace = str(intSubtotal + intTax)

        # replace the first instance of the token in the text
        if not strToken is None:
            strText = strText.replace(strToken, strReplace, 1)

            logging.debug('strText replace - strToken: ' + strToken)
            logging.debug('strText replace - strReplace: ' + strReplace)

        # update the document and annotation unless it's a list start token
        if not strToken is None and strToken != strTokItemsStart:
            # create special annotation for item list and standard for the rest
            if strToken == strTokItemsEnd:
                # use list start and end indexes
                intStart = intListStart
                intEnd = intListEnd

                # use custom label
                strLabel = 'item_list'
            else:
                # calculate the end position of the token
                intEnd = intStart + len(strReplace) - 1

                # create label
                strLabel = strCreateJSONLabel(strToken)

            # create annotation
            dctAnnotation = {
                'label': strLabel,
                'start': intStart,
                'end': intEnd
            }

            # update the JSON dictionary
            dctJSON['text'] = strText
            dctJSON['annotations'].append(dctAnnotation)

        # get the next token
        intStart, strToken = tplFindEarliestToken(strText, lstTokens)

    # export the annotated file to json
    strJSON = json.dumps(dctJSON, indent=4)
    
    # create a name for the output file
    strOut = strPathOutputs + 'a' + str(100000 + intCount) + '.json'

    # save the invoice in a json file
    objNewJSON = open(strOut, 'w')
    objNewJSON.writelines(strJSON)
    objNewJSON.close()