# %% imports
import random
import datetime
import numpy as np
import pandas as pd

# %% paths and definitions

# number of files to generate
intFiles = 10

# number of available templates
intTemplates = 1

# paths
strPathData = 'c:/repositories/zzz_data_for_lstm/data/inputs/'
strPathTemplates = 'c:/repositories/zzz_data_for_lstm/templates/'

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
strTokT = '[tax]'
strTokTotal = '[total]'

lstTokens = [
    # strTokCompany,
    # strTokStreet,
    # strTokCity,
    # strTokZip,
    # strTokPhone,
    # strTokEmail,
    # strTokDateIn,
    # strTokDateDue,
    # strTokClient,
    # strTokClientStreet,
    # strTokClientCity,
    # strTokClientZip,
    # strTokInvoiceNo,
    # strTokItemsStart,
    strTokItemsEnd,
    strTokItem,
    strTokQ,
    strTokR,
    strTokA,
    strTokS,
    strTokT,
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

    # initialize return values
    tplPosition = (-1, None)

    for strToken in plstTokens:
        # locate the token in the string
        intPosition = pstrText.find(strToken)

        # remember the token if found on an earlier position
        if intPosition >= 0 and intPosition < tplPosition[0]:
            tplPosition = (intPosition, strToken)

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
    strOut = pstrValue.replace(' ', '_')

    # remove other characters
    strRemove = '!@#$%^&*()+-=[]\\{}|;\':",./<>?'
    strOut = ''.join([x.lower() for x in strOut if x not in strRemove])

    return strOut

def strRandomDate() -> str:
    """Generate random date and return it in dd/mm/yyyy format as string.
    
    Inputs:
        - None

    Outputs:
        - strDate - date in dd/mm/yyyy format, it can potentially generate also
        nonsense dates like 30/02/2021 or 31/04/2025
    """

    # generate random day
    intDay = random.randint(1, 31)

    # generate random month
    intMonth = random.randint(1, 12)

    # generate random year
    intYear = random.randint(2020, 2030)

    # create date
    dteRandom = datetime.date(intYear, intMonth, intDay)

    strDate = dteRandom.strftime('%d/%m/%y')

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
    strList = pstrTemplate[intStart:intEnd]
    strPostList = pstrTemplate[intEnd:]

    # get a random number of repetitions
    intRepeat = random.randint(1, 20)

    # prepare the modified template
    strTemplateOut = strPreList + (strList * intRepeat) + strPostList

    return strTemplateOut

# %% import data

dtfCity = pd.read_csv(strPathData + 'cities.csv', encoding='latin-1')
dtfCompany = pd.read_csv(strPathData + 'company.csv', encoding='latin-1')
dtfItems = pd.read_csv(strPathData + 'items.csv', encoding='latin-1')
dtfName = pd.read_csv(strPathData + 'name.csv', encoding='latin-1')
dtfPhone = pd.read_csv(strPathData + 'phone.csv', encoding='latin-1')
dtfStreet = pd.read_csv(strPathData + 'street.csv', encoding='latin-1')

# %% generate annotations
for intCount in range(intFiles):
    # get a random template to annotate
    strText = strGetRandomTemplate(intTemplates)

    # initialize annotation dictionary
    dctJSON = {
        'text': '',
        'annotations': []
    }

    # initialize position variables
    intStart = 0
    intEnd = 0
    intListStart = 0
    intListEnd = 0

    # set seed
    np.random.seed(0)

    # get token
    intStart, strToken = tplFindEarliestToken(strText, lstTokens)

    # do this for all tokens
    while intStart >= 0:
        # based on token generate the appropriate data to replace it
        if strToken in [strTokClient, strTokCompany]:
            # shuffle company data and get first observation
            dtfRandom = dtfCompany.apply(np.random.permutation)
            strReplace = dtfRandom.iloc[0][0] + ' ' + dtfRandom.iloc[0][1]

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
            strZip = strZip[1:]

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
            strReplace += '@' + dtfCompany.iloc[0][0] + dtfCompany.iloc[0][1]
            strReplace += '.com'

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

        # calculate the end position of the token
        intEnd = intStart + len(strReplace) - 1

        # replace the token in the text
        strText = strText.replace(strToken, strReplace)

        # update the document and annotation unless it's a list start token
        if strToken != strTokItemsStart:
            # create annotation
            dctAnnotation = {
                'label': strCreateJSONLabel(strToken),
                'start': intStart,
                'end': intEnd
            }

            # update the JSON dictionary
            dctJSON['text'] = strText
            dctJSON['annotations'].append(dctAnnotation)

        # get the next token
        intStart, strToken = tplFindEarliestToken(strText, lstTokens)