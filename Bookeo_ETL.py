#Imports for Bookeo_ETL
import requests
import json
import datetime
from datetime import datetime as dt
import time
import pandas as pd
from pandas.io.json import json_normalize
from dateutil.relativedelta import relativedelta
import re
import numpy as np

import sqlalchemy as db

!pip install flat_table
import flat_table

#Create the class for Bookeo scrapper

class Bookeo_ETL():
  """An instance of this class sets up the connection to the Bookeo API and 
  retrieves the data in a json format to then transform it into a pandas dataframe
  and load it into a SQL database"""
  
  def __init__(self, location):
    """Instantiate the class and create internal attributes"""
    
    # Credentials
    #MAKE SURE TO REMOVE THESE WHEN UPLOADING TO A PUBLIC SITE
    self.secretKey = 'nRhykUJ5mIQQLSyMOfswBYMvfPetLL3v'
    self.apikeys = {'casa_loma': 'AAWNLWW6WEMLRTNTUCJ3T41551RCE94P14F1F8479C2', 'black_creek': 'ARJKX9MATEMLRTNTUCJ3T3152XELJPJ1456D150190'}
    self.apiKey = self.apikeys[location]
    self.location = location
    
    self.db_url = 'postgresql://postgres:psql1234@sca-db.cvz6xur1mv50.us-east-2.rds.amazonaws.com:5432/sca_data'
    self.engine = db.create_engine(self.db_url)
    

  def __str__(self):
    return f'This is a template for the Bookeo ETL class for {self.location}'

  
  
#__________________________________________________EXTRACTING__________________________________________________  
  def extract_data(self, startTime = None, endTime = datetime.datetime.now(), fromStartOfMonth=True):
    """This method handles and instantiates the connection to the API in order to send 
    and receive requests. After the connection is created, it applies a predefined
    API request and extracts a json file with the data sent back from the API
    
    `startTime` is the date to start at. If not specified, it queries the database
    for the last updated time. If the start time cannot be obtained from the db,
    it defaults to Jan 01 2017
    
    `endTime` is the date end the extraction. By default it is the current date.
    
    `fromStartOfMonth` determines whether extraction starts on the exact date of 
    `startTime` the 1st of the month of `startTime` 

    Pseudocode:
        1. Determine the date to start extraction
        2. Extract data one month at a time
        3. Add data to the list
    """
    
    #If the start time is not specified, query the database for the last updated time
    if(startTime == None):
      connection = self.engine.connect()
      metadata = db.MetaData()
      try:
        booking_table = db.Table('booking', metadata, autoload=True, autoload_with=self.engine)
        query = db.select([booking_table.columns.creationTime]).order_by(db.desc(booking_table.columns.creationTime))
        
      #If a last updated time cannot be found, likely due to the table not existing, extract all data starting Jan 01 2017
      except:
        startTime = datetime.datetime(2017, 1, 1)
        print(f'Table not found, extracting all data starting: {startTime}')
      else:
        ResultProxy = connection.execute(query)
        ResultSet = ResultProxy.fetchone()
        time = ResultSet.values()[0]
        print(f'Last updated: {time}')
        time = time.rsplit('-',1)[0]
        startTime = dt.strptime(time, '%Y-%m-%dT%H:%M:%S')
             
    #Set the starting time to the start of the month
    if(fromStartOfMonth==True):
      # variables
      current_month = startTime.month
      current_year = startTime.year
      startTime = datetime.datetime(current_year, current_month, 1)

    print(f'Preparing to extract data from {startTime} to {endTime}')

    print('Extracting')    
    json_data = []
    # loop through all months
    currentTime = startTime
    while currentTime <= endTime:
        
        _json_data = self.__get_one_month_bookings(currentTime, endTime)
        json_data += _json_data
        currentTime = currentTime + relativedelta(months=1)
        
    
    print(f'Extracted {len(json_data)} rows')
    return  json_data
    
  
  def __get_one_month_bookings(self, startTime, endTime):
    """
        int, int -> json
        Get One month or until endTime of booking information from Bookeo
        Note: Bookeo only allows fetches of 31 days
    """

    # get start and end time
    endTimeMonth = startTime + relativedelta(months=1, days=-1)
    
    if(endTimeMonth> endTime):
      endTimeMonth = endTime
    
    #Format accordingly
    startTime = startTime.strftime("%Y-%m-%dT%H:%M:%S-04:00")
    endTimeMonth = endTimeMonth.strftime("%Y-%m-%dT%H:%M:%S-04:00")
    
    print(f'Extracting data from {startTime} to {endTimeMonth}')
    
    # call GET request
    url = f'https://api.bookeo.com/v2/bookings?secretKey={self.secretKey}&apiKey={self.apiKey}&lastUpdatedStartTime={startTime}&lastUpdatedEndTime={endTimeMonth}&itemsPerPage=100&expandParticipants=true&expandCustomer=true&includeCanceled=true'
    currentPage, totalPages, pageNavigationToken, json_data = self.__get_request(url)
    
    #grab new pages as long as there are pages left
    while currentPage < totalPages:
        next_page_url = f'https://api.bookeo.com/v2/bookings?secretKey={self.secretKey}&apiKey={self.apiKey}&pageNavigationToken={pageNavigationToken}&pageNumber={currentPage + 1}&itemsPerPage=100'
        currentPage, _, _, _json_data = self.__get_request(next_page_url)
        json_data += _json_data
    return json_data  


    
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
      
      
      

      
#_________________________TRANSFORMING__________________________________________________  
  def transform_data(self, current_bookings):
    """Transforms the loaded json dump into a dataframe with all the information ready to
    be sent into the SQL database

    Pseudocode:
    1. Put current_bookings into dataframe (df) and flatten json entries
    2. Assign Internal id based on SCA customer id (no longer in use)
    3. Cleaning transformations
    4. Split df into customer_df and booking_df
    """
    print('Transforming data')
    
    # Set customer columns
    customer_col = [
                #'IID',
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
                #'IID',
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
    
    
    # Flatten json object
    df = pd.DataFrame(current_bookings)
    
    #if the df is empty
    if(df.size == 0):
      bookings_df = pd.DataFrame(columns = booking_col)
      customer_df = pd.DataFrame(columns = booking_col)
      self.customer_df = customer_df
      self.bookings_df = bookings_df
      return
    
    df = flat_table.normalize(df)

    # Assign location 
    df['location'] = self.location

    # Grab all unique id, no longer in use 
    #customer_id = (df['customerId'].unique()).tolist()
    # Internal id dictionary
    # TODO: We might need to export this?
    #IID = {}
    #k   = 1
    #for identity in customer_id:
    #  IID[identity] = k
    #  k = k + 1    
    # Map IID to each booking data
    # df['IID'] = df['customerId'].apply(lambda x: IID[x])


    # Cleaning
    #if a column is not in the df, assign a null column to it
    for col in booking_col:
      if(col not in df.columns):
        df[col]= np.nan
    for col in customer_col:
      if(col not in df.columns):
        df[col]= np.nan   
    
    print('Cleaning')
    
    #change number formats to numbers
    num_col = [
        'customer.numNoShows','customer.numCancelations','customer.numBookings'     
    ]
    
    price_col = ['price.totalTaxes.amount','price.totalPaid.amount','price.totalNet.amount','price.totalGross.amount']
    
    date_col = [
       'customer.startTimeOfPreviousBooking','customer.startTimeOfNextBooking','customer.creationTime',       
        'cancelationTime','creationTime','endTime','lastChangeTime','startTime'
    ]
    df[date_col] = df[date_col].apply(self.__to_datetime)
    df[num_col] = df[num_col].astype(int)
    df[price_col] = df[price_col].astype(float)
    
    
    
    print('Seperating phone extensions')
    #seperate extensions and move them to their own column
    df['customer.phoneNumbers.ext'] = df['customer.phoneNumbers.number']     
    df['customer.phoneNumbers.number'] = df['customer.phoneNumbers.number'].apply(self.__number)
    df['customer.phoneNumbers.ext'] = df['customer.phoneNumbers.ext'].apply(self.__ext)
    
    # remove non numerical characters
    df['customer.phoneNumbers.number'] = df['customer.phoneNumbers.number'].apply(self.__remove_char)
    df['customer.phoneNumbers.ext'] = df['customer.phoneNumbers.ext'].apply(self.__remove_char)
    
    
    
    print('Cleaning product names')
    # Remove the trailing whitespace from game names
    df['productName'] = df['productName'].str.rstrip()
    
    #change privateEvent column to true for private games
    private_bookings = df[df['productName'].str.contains('Private')]
    df.loc[private_bookings.index, 'privateEvent']= True
    
    
    #add the corporate booking column
    corporate_list = []
    for index, row in df.iterrows():
      corporate_list.append(self.__is_corp(row))                                         
    df['corporateBooking'] = corporate_list
    
    df['productName'] = df['productName'].apply(self.__clean_game_name)  
    
    
    # Split data into customer and bookings
    customer_df = df[customer_col]
    bookings_df = df[booking_col]

    # Eliminate duplicates in customer_df based on IID
    customer_df = (customer_df.drop_duplicates(subset='customer.id')).reset_index(drop = True)
    customer_df = (customer_df.drop_duplicates(subset='customer.emailAddress')).reset_index(drop = True)
    bookings_df = (bookings_df.drop_duplicates(subset='bookingNumber')).reset_index(drop = True)
    
    customer_df = customer_df.sort_values('customer.creationTime')
    bookings_df = bookings_df.sort_values('creationTime')
    
    #Format column names
    customer_df.columns = customer_df.columns.str.replace('.', '_')
    bookings_df.columns = bookings_df.columns.str.replace('.', '_')
    
    print('Finished transforming')
    return {'customer':customer_df, 'booking':bookings_df}
    
    

  # HELPER FUNCTIONS 
  #convert to datetime
  def __to_datetime(self, date):
    date = pd.to_datetime(date, utc = True)
    date = date.dt.tz_convert('US/Eastern')
    date = date.dt.tz_localize(None)
    return date
  
  #remove non numerical values
  def __remove_char(self, num):
    if(pd.isna(num)):
      return np.nan
    #removes non numerical values from the phone number
    num = re.sub("[^0-9]", "", num)
    if(len(num)==0):
      return np.nan
    return num
  
  #get the phone number portion from the column
  def __number(self, num):
    if(pd.isna(num)):
      return None
    numbers = re.split('x',num, flags  = re.IGNORECASE)
    if(len(numbers) == 1):
      numbers.append(np.nan)
    return str(numbers[0])
  
  #get the extension portion from the column
  def __ext(self, num):
    if(pd.isna(num)):
      return  None
    numbers = re.split('x',num, flags  = re.IGNORECASE)
    if(len(numbers) == 1):
      numbers.append(np.nan)
    return str(numbers[1])  
  
  #check if a email is corporate 
  def __is_corp_email(self, email):
    '''
    Check if an email is corporate by checking if it is in a given list of providers
    '''   
    #list of public emails
    self.emailList = [
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
    if(provider in (self.emailList)):
      return False
    return True    

  #check if a booking is corporate
  def __is_corp(self, booking):
    #max capacities for each game
    self.capacities = {
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
    
    '''See if a booking is corporate'''
    score = 0
    #1) possess corporate email 
    if(self.__is_corp_email(booking['customer.emailAddress'])):
      score+=1
    #2) possess corporate phone number (ext)
    if(pd.isna(booking['customer.phoneNumbers.ext']) == False):
      score+=1
    #3) full booking
    #max capacity for each game
    if(booking['participants.numbers.number'] in self.capacities):
      if(booking['participants.numbers.number'] >= self.capacities[booking['productName']]):
        score+=1
    #4) private event
    if(booking['privateEvent']==True):
      score+=1
    return score>=3
  
  def __clean_game_name(self, productName):
    self.game_names = {  
      'King of the Bootleggers':'King of the Bootleggers', 
      'King Of The Bootleggers':'King of the Bootleggers', 
      'Bootleggers Private':'King of the Bootleggers',

      'Escape from the Tower':'Escape from the Tower', 
      'Tower Private Game':'Escape from the Tower', 

      'Station M':'Station M', 
      'Station M Private Game':'Station M',

      'Where Dark Things Dwell':'Where Dark Things Dwell',

      'Murdoch Mysteries Escape Game':'Murdoch Mysteries Escape Game', 
      'Murdoch Mysteries Private Game':'Murdoch Mysteries Escape Game',

      'Escape from the Time Travel Lab':'Escape from the Time Travel Lab', 
      'TTL (Private Booking)':'Escape from the Time Travel Lab', 

      'The Trial of the Mad Fox Society':'The Trial of the Mad Fox Society', 
      'MadFox (Private Booking)':'The Trial of the Mad Fox Society',


      'Black Creek (Private 30 people)':'Black Creek Private', 
      'Black Creek (Private 60 People)':'Black Creek Private',

      'Big Top Battle':'Big Top Battle', 
      'Big Top Battle Private Game':'Big Top Battle', 


      'The Secret of Station House No. 4':'The Secret of Station House No. 4', 
      'The Secret of Station House No 4(Private Booking)':'The Secret of Station House No. 4',

      "The Dragon's Song":"The Dragon's Song", 
      "The Dragon's Song Private":"The Dragon's Song", 
      "The Dragon's Song Family Game":"The Dragon's Song"  
    }
    if (productName in self.game_names):
        return self.game_names[productName]
    else: 
      #print(productName)
      return productName
  
  
    
#__________________________________________________LOADING__________________________________________________
  def load_data(self, data_dict):
    """
      Takes dictionary of table name and dataframe
    """
    for data in data_dict:
      print(f'Loading {data}')
      self.__load_df( data_dict[data],data)

                                          
    
  def __load_df(self, df, table_name):
    """
      Function for loading in data. It loads in data from the dataframe row by row.
      If an error is encountered, it simply ignores that row. It will print an update
      every 200 rows.
    """
    error_count = 0
    load_count = 0
    for i in range(len(df)):
      try:
        df.iloc[i:i+1].to_sql(table_name, self.engine, if_exists='append' ,index=False)
      except Exception as e: 
        #print(e)
        error_count+=1     
      else:
        load_count+=1
      if((load_count+error_count)%50 ==0):
        print(f'Loaded {load_count} / {df.shape[0]} entries')
        print(f'Encountered {error_count} errors')      
    print(f'Loaded {load_count} / {df.shape[0]} entries')
    print(f'Encountered {error_count} errors')
    
    
