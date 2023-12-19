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