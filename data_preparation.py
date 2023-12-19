# %% imports
import pandas as pd

# %% paths and definitions

# paths
strPathData = 'c:/repositories/zzz_data_for_lstm/data/inputs/'
strPathTemplates = 'c:/repositories/zzz_data_for_lstm/templates/'

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
def tplFindEarliestToken(pstrText: str, plstTokens: list) -> tuple:
    """Return position of the earliest occurence of any of the tokens, together
    with the token itself
    
    Inputs:
        - pstrText - text to search for a token
        - plstTokens - list of tokens to look for

    Outputs:
        - tuple of the earliest token and its position, if no token is found,
        (-1, None) is returned
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