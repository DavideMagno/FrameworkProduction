require('RblDataLicense')

# These are dummy credentials. Replace with the credentials received from Bloomberg
RblConnect(user = 'f7f0b8f9989c6cce7676fbcd891ea2a9', 
           pw = '5760c3a3a4857986cb65c8be8a796a80dc2d7b72a99c3e319da9f75f5893f36d',
           host = "api.bloomberg.com/eap/notifications/sse",
           protocol = "https") 


data <- RblQuery(fields = c('PX_LAST', 'PX_OPEN', 'PX_HIGH', 'PX_LOW'), 
                 identifiers = c('SXXE Index', "SX5E Index"), 
                 from = '2005-01-01')
