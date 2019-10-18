#Create the class for Bookeo scrapper

class Bookeo_ETL():
  """An instance of this class sets up the connection to the Bookeo API and 
  retrieves the data in a json format to then transform it into a pandas dataframe
  and load it into a SQL database"""
  
  def __init__(self, location='casa_loma'):
    """Instantiate the class and create internal attributes"""
    
    # Credentials
    self.secretKey = 'insert secret key here'
    self.apikeys = {
      'casa_loma': 'insert api key here',
      'black_creek': 'insert api key here'}
    self.apiKey = self.apikeys[location]
    self.location = location
    
    #Let's find the last date
    #default start time
    self.lastUpdatedStartTime = datetime.datetime(2019, 10, 1)
    
    #Query for last date
    #Connect to database
    engine = db.create_engine('insert database here')
    connection = engine.connect()
    metadata = db.MetaData()
    try:
      booking_table = db.Table('booking', metadata, autoload=True, autoload_with=engine)
      query = db.select([booking_table.columns.creationTime]).order_by(db.desc(booking_table.columns.creationTime))
    except:
      print('Table not found')
    else:
      ResultProxy = connection.execute(query)
      ResultSet = ResultProxy.fetchone()
      time = ResultSet.values()[0]
      time = time.rsplit('-',1)[0]
      self.lastUpdatedStartTime = dt.strptime(time, '%Y-%m-%dT%H:%M:%S')
      
    print(f'Start time: {self.lastUpdatedStartTime}')
    
    self.current_bookings = self.extract_data(self.lastUpdatedStartTime)
    self.customer_df, self.booking_df = self.transform_data()
  
  
  
  def __str__(self):
    return 'This is a template for the Bookeo ETL class'
  
  
  
  def extract_data(self, lastUpdatedStartTime):
    """This method handles and instantiates the connection to the API in order to send 
    and receive requests. After the connection is created, it applies a predefined
    API request and extracts a json file with the data sent back from the API
    
    int -> json
    start from january, and keep calling `get_request()` until it is either december or current month

    Pseudocode:
        1. get all months for year
        2. call get one month booking
        3. return df
    """
      
    # Set endtime to now
    lastUpdatedEndTime = datetime.datetime.now()
    
    # variables
    
    #current_month = lastUpdatedStartTime.month
    #current_year = lastUpdatedStartTime.year
    #current_datetime = datetime.datetime(current_year, current_month, 1)
    
    current_datetime = lastUpdatedStartTime
    
    json_data = []

    # loop through all months
    while current_datetime <= lastUpdatedEndTime:
        
        _json_data = self.__get_one_month_bookings(current_datetime)
        json_data += _json_data
        current_datetime = current_datetime + relativedelta(months=1)
        
    return json_data

  
  
  def __get_one_month_bookings(self, lastUpdatedStartTime):
    """
        int, int -> json
        Get One month of booking information from Bookeo
        Note: Bookeo only allows fetches of 31 days
    """

    # get start and end time
    lastUpdatedEndTime = lastUpdatedStartTime + relativedelta(months=1, days=-1)
    
    #Format accordingly
    lastUpdatedStartTime = lastUpdatedStartTime.strftime("%Y-%m-%dT%H:%M:%S-04:00")
    lastUpdatedEndTime = lastUpdatedEndTime.strftime("%Y-%m-%dT%H:%M:%S-04:00")
    
    print(lastUpdatedStartTime)
    print(lastUpdatedEndTime)
    
    # call GET request
    url = f'https://api.bookeo.com/v2/bookings?secretKey={self.secretKey}&apiKey={self.apiKey}&lastUpdatedStartTime={lastUpdatedStartTime}&lastUpdatedEndTime={lastUpdatedEndTime}&itemsPerPage=100&expandParticipants=true&expandCustomer=true&includeCanceled=true'
    currentPage, totalPages, pageNavigationToken, json_data = self.__get_request(url)

    while currentPage < totalPages:
        next_page_url = f'https://api.bookeo.com/v2/bookings?secretKey={self.secretKey}&apiKey={self.apiKey}&pageNavigationToken={pageNavigationToken}&pageNumber={currentPage + 1}&itemsPerPage=100'
        currentPage, _, _, _json_data = self.__get_request(next_page_url)
        json_data += _json_data

    return json_data

  
  def transform_data(self):
    """Transforms the loaded json dump into a dataframe with all the information ready to
    be sent into the SQL database
    
    Outputs: customer_df, booking_df

    Pseudocode:
    1. Put current_bookings into dataframe (df) and flatten json entries
    2. Assign Internal id based on SCA customer id
    3. Other transformations
    4. Split df into customer_df and booking_df
    """

    # Flatten json object
    df = pd.DataFrame(self.current_bookings)
    df = flat_table.normalize(df)

    # Assign location 
    df['location'] = self.location

    # Grab all unique id 
    customer_id = (df['customerId'].unique()).tolist()
    
    # Internal id dictionary
    # TODO: We might need to export this?
    IID = {}
    k   = 1
    for identity in customer_id:
      IID[identity] = k
      k = k + 1
    
    # Map IID to each booking data
    df['IID'] = df['customerId'].apply(lambda x: IID[x])


    # OTHER TRANSFORMATIONS
    # Set customer columns
    customer_col = [
                'IID',
                'customer.streetAddress.countryCode',
                'customer.startTimeOfPreviousBooking',
                'customer.startTimeOfNextBooking',
                'customer.numNoShows',
                'customer.numCancelations',
                'customer.numBookings',
                'customer.member',
                'customer.lastName',
                'customer.id',
                'customer.gender',
                'customer.firstName',
                'customer.facebookId',
                'customer.emailAddress',
                'customer.credit.currency',
                'customer.credit.amount',
                'customer.creationTime',
                'customer.acceptSmsReminders',
                'customer.phoneNumbers.type',
                'customer.phoneNumbers.number',
                'customer.phoneNumbers.ext'
                ]
    # Set bookings column 
    # Note: One set of taxes column were drop since it was redundant
    booking_col = [
                'IID',
                'accepted',
                'customer.emailAddress',
                'bookingNumber',
                'cancelationAgent',
                'cancelationTime',
                'canceled',
                'creationAgent',
                'creationTime',
                'customerId',
                'endTime',
                'eventId',
                'lastChangeAgent',
                'lastChangeTime',
                'privateEvent',
                'productId',
                'productName',
                'promotionName',
                'participants.numbers.number',
                'sourceIp',
                'location',
                'startTime',
                'title',
                'options.value',
                'options.name',
                'options.id',
                'price.totalTaxes.currency',
                'price.totalTaxes.amount',
                'price.totalPaid.currency',
                'price.totalPaid.amount',
                'price.totalNet.currency',
                'price.totalNet.amount',
                'price.totalGross.currency',
                'price.totalGross.amount',
                'corporateBooking'
                ]
    #if a column is not in the df, assign a null column to it
    for col in booking_col:
      if(col not in df.columns):
        df[col]= np.nan
    for col in customer_col:
      if(col not in df.columns):
        df[col]= np.nan   
    
    # Cleaning
    #print('Starting cleaning')
    #print(df.info())
    #seperate extensions and move them to their own column
    df['customer.phoneNumbers.ext'] = df['customer.phoneNumbers.number']     
    df['customer.phoneNumbers.number'] = df['customer.phoneNumbers.number'].apply(self.number)
    df['customer.phoneNumbers.ext'] = df['customer.phoneNumbers.ext'].apply(self.ext)
    
    # remove non numerical characters
    df['customer.phoneNumbers.number'] = df['customer.phoneNumbers.number'].apply(self.remove_char)
    df['customer.phoneNumbers.ext'] = df['customer.phoneNumbers.ext'].apply(self.remove_char)
    
    # Remove the trailing whitespace from game names
    df['productName'] = df['productName'].str.rstrip()
    
    #change privateEvent column to true for private games
    private_bookings = df[df['productName'].str.contains('Private')]
    df.loc[private_bookings.index, 'privateEvent']= True
    
    #add the corporate booking column
    corporate_list = []
    for index, row in df.iterrows():
      corporate_list.append(self.is_corp(row))                                         
    df['corporateBooking'] = corporate_list
    

    
    
    # Split data into customer and bookings
    customer_df = df[customer_col]
    bookings_df = df[booking_col]

    # Eliminate duplicates in customer_df based on IID
    customer_df = (customer_df.drop_duplicates(subset='IID')).reset_index(drop = True)
    customer_df = (customer_df.drop_duplicates(subset='customer.emailAddress')).reset_index(drop = True)
    bookings_df = (bookings_df.drop_duplicates(subset='bookingNumber')).reset_index(drop = True)

    #Format column names
    customer_df.columns = customer_df.columns.str.replace('.', '_')
    bookings_df.columns = bookings_df.columns.str.replace('.', '_')

    return customer_df, bookings_df
    #return bookings_df
  
  
  
  def load_data(self):
    """This method first sets up the connection to the database and harnesses 
    the methods of a dataframe to send an upsert command with a data dump into
    the SQL database"""
    
    #Connect to database
    engine = db.create_engine('insert database here')

    
    self.customer_df.to_sql('customer', engine, index_label='customer_emailAddress',
                         if_exists='append', index=False)
    self.booking_df.to_sql('booking', engine, index_label='bookingNumber',
                         if_exists='append', index=False)

    
    
  def __get_request(self, url):
    """
    Private method to be used within the extract_data method in order to get 
    send the GET request.
      
    str -> (int, int, str, json)
    Given an url, call the http GET request and return a tuple (currentPage, totalPages, pageNavigationToken, df)

    Note: you only get a pageNavigationToken if the total count of item is greater than 50 (default pagination)
    """

    # call GET
    response = requests.get(url)
    json_response = response.json()

    # SUCCESS
    if response.status_code == 200:
        # parse GET
        json_response_data = json_response['data']
        json_response_info  = json_response['info']
        pageNavigationToken = json_response_info['pageNavigationToken'] if 'pageNavigationToken' in json_response_info else ""
        currentPage = json_response_info['currentPage']
        totalPages = json_response_info['totalPages']

        # build DF and return
        return (currentPage, totalPages, pageNavigationToken, json_response_data)

    # Failed Retry
    elif response.status_code == 429:
        print("Retrying....")
        json_response_retryAfter = json_response['retryAfter']
        time.sleep(json_response_retryAfter)
        return self.__get_request(url)

    # FAIL
    else:
        print("ERROR!")
        print(f'Status Code: {response.status_code}')
        print(f'Content: {response.content}')
        return(9999, -1, "", None)


  
  # HELPER FUNCTIONS 
  #remove non numerical values
  def remove_char(self, num):
    if(pd.isna(num)):
      return np.nan
    #removes non numerical values from the phone number
    num = re.sub("[^0-9]", "", num)
    if(len(num)==0):
      return np.nan
    return num
  
  #get the phone number portion from the column
  def number(self, num):
    if(pd.isna(num)):
      return np.nan
    numbers = re.split('x',num, flags  = re.IGNORECASE)
    if(len(numbers) == 1):
      numbers.append(np.nan)
    return numbers[0]
  
  #get the extension portion from the column
  def ext(self, num):
    if(pd.isna(num)):
      return  np.nan
    numbers = re.split('x',num, flags  = re.IGNORECASE)
    if(len(numbers) == 1):
      numbers.append(np.nan)
    return numbers[1]  
  
  #check if a email is corporate 
  def is_corp_email(self, email):
    '''
    Check if an email is corporate by checking if it is in a given list of providers
    '''
    #list of public emails
    emailList = [
      'gmail.com', 'yahoo.com', 'hotmail.com', 'aol.com', 'hotmail.co.uk',
      'hotmail.fr', 'msn.com', 'yahoo.fr', 'wanadoo.fr', 'orange.fr',
      'comcast.net', 'yahoo.co.uk', 'yahoo.com.br', 'yahoo.co.in', 'live.com',
      'rediffmail.com', 'free.fr', 'gmx.de', 'web.de', 'yandex.ru', 'ymail.com',
      'libero.it', 'outlook.com', 'uol.com.br', 'bol.com.br', 'mail.ru',
      'cox.net', 'hotmail.it', 'sbcglobal.net', 'sfr.fr', 'live.fr',
      'verizon.net', 'live.co.uk', 'googlemail.com', 'yahoo.es', 'ig.com.br',
      'live.nl', 'bigpond.com', 'terra.com.br', 'yahoo.it', 'neuf.fr',
      'yahoo.de', 'alice.it', 'rocketmail.com', 'att.net', 'laposte.net',
      'facebook.com', 'bellsouth.net', 'yahoo.in', 'hotmail.es', 'charter.net',
      'yahoo.ca', 'yahoo.com.au', 'rambler.ru', 'hotmail.de', 'tiscali.it',
      'shaw.ca', 'yahoo.co.jp', 'sky.com', 'earthlink.net', 'optonline.net',
      'freenet.de', 't-online.de', 'aliceadsl.fr', 'virgilio.it', 'home.nl',
      'qq.com', 'telenet.be', 'me.com', 'yahoo.com.ar', 'tiscali.co.uk',
      'yahoo.com.mx', 'voila.fr', 'gmx.net', 'mail.com', 'planet.nl',
      'tin.it', 'live.it', 'ntlworld.com', 'arcor.de', 'yahoo.co.id',
      'frontiernet.net', 'hetnet.nl', 'live.com.au', 'yahoo.com.sg', 'zonnet.nl',
      'club-internet.fr', 'juno.com', 'optusnet.com.au', 'blueyonder.co.uk', 'bluewin.ch',
      'skynet.be', 'sympatico.ca', 'windstream.net', 'mac.com', 'centurytel.net',
      'chello.nl', 'live.ca', 'aim.com', 'bigpond.net', 'cogeco.net',
      'cogeco.ca', 'bell.ca', 'bell.com', 'bell.net', 'bell.ca', 'rogers.com'
    ]                                     
    if(pd.isna(email)==True):
      return False
    provider = email.split('@')[1]
    if(provider in (emailList)):
      return False
    return True
                
    
    
  #check if a booking is corporate
  def is_corp(self, booking):
    '''See if a booking is corporate'''
    score = 0
    #1) possess corporate email 
    if(self.is_corp_email(booking['customer.emailAddress'])):
      score+=1
    #2) possess corporate phone number (ext)
    if(pd.isna(booking['customer.phoneNumbers.ext']) == False):
      score+=1
    #3) full booking
    #max capacity for each game
    capacities = {
        'King of the Bootleggers':16, 'Bootleggers Private':16,
        'Escape from the Tower':12, 'Tower Private Game':12, 
        'Station M':12, 'Station M Private Game':12,
        'Where Dark Things Dwell':60,
        'Murdoch Mysteries Escape Game':12, 'Murdoch Mysteries Private Game':12,
        'Escape from the Time Travel Lab':11, 'TTL (Private Booking)':11, 
        'The Trial of the Mad Fox Society':11, 'MadFox (Private Booking)':11,
        'Black Creek (Private 30 people)':30, 'Black Creek (Private 60 People)':60,
        'Big Top Battle':120, 'Big Top Battle Private Game':120, 
        'The Secret of Station House No. 4':12, 'The Secret of Station House No 4(Private Booking)':12,
        "The Dragon's Song":12, "The Dragon's Song Private":12, "The Dragon's Song Family Game":12   
    }
    if(booking['participants.numbers.number'] in capacities):
      if(booking['participants.numbers.number'] >= capacities[booking['productName']]):
        score+=1
    #4) private event
    if(booking['privateEvent']==True):
      score+=1
    return score>=3
  
