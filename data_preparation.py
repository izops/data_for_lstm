# %% imports
import random
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

# %% import data

dtfCity = pd.read_csv(strPathData + 'cities.csv', encoding='latin-1')
dtfCompany = pd.read_csv(strPathData + 'company.csv', encoding='latin-1')
dtfItems = pd.read_csv(strPathData + 'items.csv', encoding='latin-1')
dtfName = pd.read_csv(strPathData + 'name.csv', encoding='latin-1')
dtfPhone = pd.read_csv(strPathData + 'phone.csv', encoding='latin-1')
dtfStreet = pd.read_csv(strPathData + 'street.csv', encoding='latin-1')

