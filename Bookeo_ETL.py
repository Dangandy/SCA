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
  
  def __init__(self):
    """Instantiate the class and create internal attributes"""
    
    # Credentials
    #MAKE SURE TO REMOVE THESE WHEN UPLOADING TO A PUBLIC SITE
    self.secretKey = secret key
    self.apikeys = {'casa_loma': casa loma api key, 
                    'black_creek': black creek api key}
    
    
    self.db_url = database url
    self.engine = db.create_engine(self.db_url)
    
    self.raw_data = {}
    self.raw_data_df = {}
    self.transformed_data = {}
    
  def __str__(self):
    return f'This is a template for the Bookeo ETL class'

  
  
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=EXTRACTING=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
  
    
  def extract_data(self, startTime = None, endTime = datetime.datetime.now(), fromStartOfMonth=True):
    """This method handles and instantiates the connection to the API in order to send 
    and receive requests. After the connection is created, it applies a predefined
    API request and extracts a json file with the data sent back from the API
    
    `startTime` is the date to start at. If not specified, it queries the database
    for the last updated time. If the start time cannot be obtained from the db,
    it defaults to Jan 01 2017
    
    `endTime` is the date end the extraction. By default it is the current date.
    
    `fromStartOfMonth` determines whether extraction starts on the exact date or the 1st of the month
    
    `startTime` the 1st of the month of `startTime` 

    Pseudocode:
        1. Determine the date to start extraction
        2. Extract data one month at a time
        3. Add data to the list
    """
    
    self.raw_data = {}
    #extract data for each location that has an api key
    for location in self.apikeys:
      self.raw_data[location] = self.__extract_data(location, startTime, endTime, fromStartOfMonth)
    return self.raw_data
  
  def __extract_data(self, location, startTime = None, endTime = datetime.datetime.now(), fromStartOfMonth=True):
    """
      Determines the starting and ending times based on inputs, and extracts data month by month
      
      Inputs are the same as extract_data
    """
    self.apiKey = self.apikeys[location]
    
    #If the start time is not specified, query the database for the last updated time
    if(startTime == None):
      connection = self.engine.connect()
      metadata = db.MetaData()
      try:
        #query the database for the last updated time
        booking_table = db.Table('booking', metadata, autoload=True, autoload_with=self.engine)
        query = db.select([booking_table.columns.creationtime]).order_by(db.desc(booking_table.columns.creationtime))
        
      #if a last updated time cannot be found, likely due to the data not existing, extract all data starting Jan 01 2017
      except Exception as e:
        print(e)
        startTime = datetime.datetime(2017, 1, 1)
        print(f'Table not found, extracting all data starting: {startTime}')
      else:
        #set the start time to the resulting time
        ResultProxy = connection.execute(query)
        ResultSet = ResultProxy.fetchone()
        time = ResultSet.values()[0]
        print(f'Last updated: {time}')
        startTime = time
             
    if(fromStartOfMonth==True):
      #set the starting time to the start of the month
      current_month = startTime.month
      current_year = startTime.year
      startTime = datetime.datetime(current_year, current_month, 1)

    print(f'Preparing to extract data from {startTime} to {endTime}')

    print('Extracting')    
    
    json_data = []
    # loop through all months
    currentTime = startTime
    while currentTime <= endTime:
        #pull one month of data and append it to the list
        _json_data = self.__get_one_month_bookings(currentTime, endTime)
        json_data += _json_data
        #increment by one month
        currentTime = currentTime + relativedelta(months=1)
            
    print(f'Extracted {len(json_data)} rows')
    return  json_data
    
  
  def __get_one_month_bookings(self, startTime, endTime):
    """
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
    # this grabs up to 100 rows at a time
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
      
      
      

      
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=TRANSFORMATION=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-= 
      
  def __to_dataframe(self, raw_data):
    '''
      Converts the json raw data into a dataframe
    '''
    # Flatten json object
    df = pd.DataFrame(raw_data)
    
    #if the df is empty
    if(df.size == 0):
      return pd.DataFrame()
    df = flat_table.normalize(df)
    return df
  
  def transform_data(self, raw_data):
    """Transforms the loaded json dump into a dataframe with all the information ready to
    be sent into the SQL database

    Pseudocode:
    1. Put raw_data into dataframe (df) and flatten json entries
    2. Cleaning transformations
      a. Change number and time formats
      b. Seperate extensions and remove non numerical values from phone numbers
      c. 
      d. 
      e. 
    3. Split df into customer_df and booking_df
    4. Retrieve utilization table from information
    """
    print('Transforming data')
    
    print('Merging data')

    #Assign the raw data to attributes
    casa_df = self.__to_dataframe(raw_data['casa_loma'])
    black_df = self.__to_dataframe(raw_data['black_creek'])

    #assign the location column
    self.raw_data_df['casa_loma'] = casa_df
    casa_df['location'] = 'Casa Loma'
    self.raw_data_df['black_creek'] = casa_df
    black_df['location'] = 'Black Creek'
    
    #merge the dataframes
    df = pd.DataFrame()
    df = df.append(casa_df)
    df = df.append(black_df)
    
    
    #remove duplicate bookings
    df = (df.drop_duplicates(subset='bookingNumber')).reset_index(drop = True)  
    df = (df.drop_duplicates(subset='customer.id')).reset_index(drop = True)   
    
    #these names are the names of columns in the json file. they will be renamed later
    # Set customer columns
    customer_col = [
                'customer.emailAddress',
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
                'customer.credit.currency',
                'customer.credit.amount',
                'customer.creationTime',
                'customer.acceptSmsReminders',
                'customer.phoneNumbers.type',
                'customer.phoneNumbers.number',
                'customer.phoneNumbers.ext',
                'repeat_purchases'
                ]
    # Set bookings column 
    # Note: One set of taxes column were drop since it was redundant
    booking_col = [
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
                'max_capacity',
                'sourceIp',
                'location',
                'startTime',
                'start_month',
                'start_year',
                'start_quarter',
                'start_weekend',
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
                'corporate_booking'
                ]      
                
    # Cleaning
    #if a column is not in the df, assign a null column to it
    for col in booking_col:
      if(col not in df.columns):
        df[col]= np.nan
    for col in customer_col:
      if(col not in df.columns):
        df[col]= np.nan   
    

    print('Cleaning')
    #rename column names
    #replace all . with _
    df.columns = df.columns.str.replace('.', '_')
    customer_col = [col.replace('.', '_') for col in customer_col]
    booking_col = [col.replace('.', '_') for col in booking_col]
    #change all names to lower case
    df.columns = df.columns.str.lower()
    customer_col = [col.lower() for col in customer_col]
    booking_col = [col.lower() for col in booking_col]

    #change number and date formats   
    num_col = [
        'customer_numnoshows','customer_numcancelations','customer_numbookings'     
    ]
    price_col = ['price_totaltaxes_amount','price_totalpaid_amount','price_totalnet_amount','price_totalgross_amount']
    
    date_col = [
       'customer_starttimeofpreviousbooking', 'customer_starttimeofnextbooking', 'customer_creationtime',       
        'cancelationtime', 'creationtime', 'endtime', 'lastchangetime', 'starttime'
    ]
    df[date_col] = df[date_col].apply(self.__to_datetime)
    df[num_col] = df[num_col].astype(float)
    df[price_col] = df[price_col].astype(float)    
    
    #seperate extensions and move them to their own column
    print('Seperating phone extensions')
    df['customer_phonenumbers_number'] = df['customer_phonenumbers_number'].apply(self.__number)
    df['customer_phonenumbers_ext'] = df['customer_phonenumbers_ext'].apply(self.__ext)
    
    # remove non numerical characters
    df['customer_phonenumbers_number'] = df['customer_phonenumbers_number'].apply(self.__remove_char)
    df['customer_phonenumbers_ext'] = df['customer_phonenumbers_ext'].apply(self.__remove_char)
    
    
    print('Cleaning product names')
    # Remove the trailing whitespace from game names
    df['productname'] = df['productname'].str.rstrip()
    #change privateEvent column to true for private games
    private_bookings = df[df['productname'].str.contains('Private', flags = re.IGNORECASE)]
    df.loc[private_bookings.index, 'privateevent']= True
    
    #get max capacity and clean up the product names
    df['max_capacity'] = df['productname'].apply(self.__get_max_cap)
    df['productname'] = df['productname'].apply(self.__clean_game_name) 
    df['max_capacity'] = df['productname'].apply(self.__get_max_cap)
    
    #add the corporate booking column
    print('Identifying corporate bookings')
    corporate_list = []
    for index, row in df.iterrows():
      corporate_list.append(self.__is_corp(row))                                         
    df['corporate_booking'] = corporate_list
     
    
    #add columns for the month, year, quarter, and whether it's a weekday for start time
    df['start_month'] = df['starttime'].dt.month
    df['start_year'] = df['starttime'].dt.year
    df['start_quarter'] = df['starttime'].dt.quarter
    df['start_weekend'] = df['starttime'].dt.weekday>4
    
    # Split data into customer and bookings
    customer_df = df[customer_col]
    bookings_df = df[booking_col]

    #add column for how many times a customer has made a purchase
    repeat_purchases = bookings_df['customer_emailaddress'].value_counts().reset_index()
    repeat_dict = repeat_purchases.set_index('index').to_dict()['customer_emailaddress']
    customer_df['repeat_purchases'] = customer_df['customer_emailaddress']
    customer_df.replace({'repeat_purchases':repeat_dict}, inplace=True)
        
    # Eliminate duplicates in customer_df based on email
    customer_df = (customer_df.drop_duplicates(subset='customer_emailaddress')).reset_index(drop = True)
    
    #drop customers with null emails
    customer_df.drop(customer_df[customer_df['customer_emailaddress'].isna()].index, inplace = True)

    #order by creation date
    customer_df = customer_df.sort_values('customer_creationtime')
    bookings_df = bookings_df.sort_values('creationtime')
    
    #generate utilization table
    monthly_capacity_df = pd.DataFrame(self.__get_utilization(bookings_df))
    
    transformed_data = {'customer':customer_df, 'booking':bookings_df, 'utilization':monthly_capacity_df}
    self.transformed_data = transformed_data
    
    print('Finished transforming')
    return transformed_data
    
    

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
  
  
  
  #get the max capacity of a game
  def __get_max_cap(self, game):
    #max capacities for each game
    self.capacities = {
        'King of the Bootleggers':16, 'King Of The Bootleggers':16, 'Bootleggers Private':16,
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
    if(game in self.capacities):
      return self.capacities[game]
    else:
      return np.nan
    
  
  #check if a email is corporate 
  def __is_corp_email(self, email):
    '''
    Check if an email is corporate by checking if it is in a given list of providers
    '''   
    #list of common public emails
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
    '''See if a booking is corporate'''
    score = 0
    #1) possess corporate email 
    if(self.__is_corp_email(booking['customer_emailaddress'])):
      score+=1
    #2) possess corporate phone number (ext)
    if(pd.isna(booking['customer_phonenumbers_ext']) == False):
      score+=1
    #3) full booking
    #max capacity for each game
    if(booking['participants_numbers_number'] >= booking['max_capacity']):
        score+=1
    #4) private event
    if(booking['privateevent']==True):
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
  
  # Function for obtaining utilization dataframe
  def __get_utilization(self, booking_data):

    #Cut the dataframe until last month
    booking_data = booking_data[booking_data.starttime < datetime.datetime(datetime.datetime.today().year, datetime.datetime.today().month+1, 1,0,0,0)] #, tzinfo=pytz.FixedOffset(0))]
    booking_data = booking_data[booking_data.starttime >= datetime.datetime(2017, 11, 1,0,0,0)] #, tzinfo=pytz.FixedOffset(0))]

    #Check if it's a weekend ==1
    booking_data['weekend'] = booking_data.starttime.dt.dayofweek.isin([4,5,6])
    bool_dict = {True:'weekend', False: 'weekday'}
    booking_data.replace({'weekend':bool_dict}, inplace=True)

    #Create column for year-week
    booking_data['yearweek'] = booking_data.starttime.dt.strftime('%Y-%V')
    #Create column for year-month
    booking_data['yearmonth'] = booking_data.starttime.dt.strftime('%Y-%m')
    #Create a column for show ID
    booking_data['showID'] = booking_data.productname + booking_data.starttime.dt.strftime('%Y%m%d%H%M')

    #display(booking_data)

    #Set up the dataframe by show
    capacity_df = booking_data.groupby(['yearmonth', 'showID', 'weekend', 'productname']).aggregate({'participants_numbers_number':'sum', 'max_capacity':'max'}).reset_index()
    #display(capacity_df)

    #Calculate the monthly capacity
    monthly_capacity = capacity_df.groupby(['yearmonth', 'weekend', 'productname']).aggregate({'participants_numbers_number': 'sum', 'max_capacity': 'sum'}).reset_index()


    monthly_capacity.max_capacity.replace(to_replace=0,value=monthly_capacity.participants_numbers_number.astype(int), inplace=True)
    monthly_capacity = monthly_capacity.groupby(['yearmonth', 'weekend']).aggregate({'max_capacity':'sum','participants_numbers_number': 'sum'}).reset_index()

    monthly_capacity['yearmonth'] = pd.to_datetime(monthly_capacity['yearmonth'],format  = '%Y-%m')

    #Calculate utilization and append to dataframe
    monthly_capacity['utilization'] = round(monthly_capacity.participants_numbers_number/monthly_capacity.max_capacity, 2)

    monthly_capacity = monthly_capacity[['yearmonth', 'weekend', 'utilization']]

    monthly_capacity = monthly_capacity.pivot_table(index = 'yearmonth', columns = 'weekend')
    monthly_capacity.columns = monthly_capacity.columns.droplevel(0)
    monthly_capacity.columns.name = None               
    monthly_capacity = monthly_capacity.reset_index() 


    monthly_capacity['year'] = monthly_capacity['yearmonth'].dt.year
    monthly_capacity['month'] = monthly_capacity['yearmonth'].dt.month

    #Return only useful data
    return monthly_capacity[['yearmonth', 'year', 'month', 'weekday', 'weekend']]

#__________________________________________________LOADING__________________________________________________
  def load_data(self, data_dict):
    """
      Takes dictionary of table name and dataframe
    """
    for data in data_dict:
      print(f'Loading {data}')
      self.__load_df(data_dict[data], data)

                                          
    
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
      if((load_count+error_count)%100 ==0):
        print(f'Loaded {load_count} / {df.shape[0]} entries')
        print(f'Encountered {error_count} errors')      
    print(f'Loaded {load_count} / {df.shape[0]} entries')
    print(f'Encountered {error_count} errors')
    
#Main   
sca_data = Bookeo_ETL()
raw_data = sca_data.extract_data()
sca_dfs = sca_data.transform_data(raw_data)
sca_data.load_data(sca_dfs)
