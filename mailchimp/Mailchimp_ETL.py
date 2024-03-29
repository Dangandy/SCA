#Imports for MailChimpETL
import json
import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
import flat_table
from mailchimp3 import MailChimp

#Create the class for MailChimp scrapper

class MailChimp_ETL():
  """An instance of this class sets up the connection to the Mailchimp API and 
  retrieves the data in a json format to then transform it into a pandas dataframe
  and load it into a SQL database"""
  
  def __init__(self, mc_api, mc_user, db_url):
    """Instantiate the class and create internal attributes"""
    
    # Credentials
    self.client = MailChimp(mc_api=mc_api, mc_user=mc_user)
    self.engine = create_engine(db_url)
  
    self.extracted_data = {}
    self.extracted_data_df = {}
    self.transformed_data = {}
    
    
  def __str__(self):
    return 'This is a template for the MailChimp ETL class' 
  

#====================================================Extraction functions====================================================       
  def extract_data(self, data_list = None): 
    '''
      Function for extracting data from Mail Chimp. Input a list of tables
      to be extracted. If no tables are specified, extract all tables.
      
      Outputs a dictionary with the table name as the key. Also saves it to 
      attribute self.extracted_data
      Example output: {'users': extracted_users_data}
      
      Tables are: campaigns, users, opens, clicks
      
      Campaigns will always be extracted, as it is needed to extract the other tables
    '''
    
    #dictionary of functions to extract each table
    self.extract_func = {
        'campaigns': self.__extract_campaigns,
        'users': self.__extract_users,
        'opens': self.__extract_opens,
        'clicks': self.__extract_clicks
    }    
    
    #if the tables to be extracted is not specified, extract all tables
    if(data_list==None):
      data_list = list(self.extract_func.keys())
      
    #campaigns needs to be extracted first to extract the rest
    data_list.append('campaigns')
    data_list = list(set(data_list))
    data_list.remove('campaigns')
    
    #create a list of data to extract
    extract_list = []
    for val in data_list:
      if(val in self.extract_func):
        extract_list.append(val)
      else:
        print(f'{val} is not an extractable object')
    
    
    print('Extracting...')
    extracted_data = {}
    
    
    #extract campaigns first
    print(f'Extracting campaigns...')
    self.campaigns_data = self.__extract_campaigns()
    extracted_data['campaigns'] = self.campaigns_data 
    print(f'Finished extracting campaigns')
    
    #extract each item in the extract list
    for data in extract_list:
      print(f'Extracting {data}...')
      #run the extraction function for each table
      extracted_data[data] = self.extract_func[data]()    
      print(f'Finished extracting {data}')
    print('Finished extracting...')
    
    self.extracted_data = extracted_data
    return extracted_data
       
#----------------------------------------------------Campaigns----------------------------------------------------

  def __extract_campaigns(self):
    """
    This method extracts all the campaings sent by the company into a list
    of dictionaries to be further processed
    
    returns -> list of dictionaries
    """
    campaign_details = []
    campaigns = self.client.campaigns.all(get_all=True)['campaigns']
    _campaigns = map(self.__campaign_mapper, campaigns)
    return list(_campaigns)

  def __campaign_mapper(self, campaign):
    """Helper to unwrap the campaign object from the API
    
    returns -> dict"""
    return {
          'id': campaign['id'],
          'open_rate': campaign.get('report_summary', {'open_rate': 0})['open_rate'],
          'click_rate': campaign.get('report_summary', {'click_rate': 0})['click_rate'],
          'send_time': campaign['send_time'],
          'status': campaign['status'],
          'title': campaign['settings']['title'],
          'subject_line': campaign['settings'].get('subject_line', "")
      }        

  
#----------------------------------------------------Users----------------------------------------------------
  def __extract_users(self):
    """
    This method extracts the lists along with all their members and 
    returns a list of dictionaries to be processed

    returns -> list of dictionaries
    """
    
    all_lists = self.client.lists.all(get_all=True)
    lists = all_lists['lists']
    row_lists = []

    for lst in lists:
        lst_id = lst['id']
        lst_name = lst['name']
        members = self.client.lists.members.all(lst_id, get_all=True)
        for member in members['members']:
            first_name = ''
            last_name = ''
            try:
                member_id = member['id']
                email = member['email_address']
                status = member['status']
                first_name = member['merge_fields']['FNAME']
                last_name = member['merge_fields']['LNAME']
            except:
                pass
            finally:
                row_lists.append({
                    'email_id': member_id,
                    'status': status,
                    'list_name': lst_name,
                    'list_id': lst_id,
                    'email': email,
                    'first_name': first_name,
                    'last_name': last_name
                })
    return row_lists   
  
  
#----------------------------------------------------Opens----------------------------------------------------  
  def __extract_opens(self):
    """This method uses the connection to query the opens or the registers of 
    the interaction of each user with the or campaigns sent
    
    returns -> list of dictionaries"""
    
    open_members = []
    for campaign in self.campaigns_data:
        campaign_id = campaign['id']
        open_report = self.client.reports.open_details.all(campaign_id, get_all=True)['members']
        open_member_list = map(self.__open_report_mapper, open_report)
        open_members += list(open_member_list)
        
    return open_members
  
  def __open_report_mapper(self,member):
    """Helper to unwrap the opens object from the API

    returns -> dict"""
    opens = member.get('opens', [])
    if opens:
        last_open = opens[-1]
    else:
      last_open = None
    return {
          'email_id' : member['email_id'],
          'campaign_id' : member['campaign_id'],
          'opens' : opens,
          'opens_count' : member.get('opens_count', 0),
          'last_open' : last_open
      } 
  
  
#----------------------------------------------------Clicks----------------------------------------------------  
  def __extract_clicks(self):
    """This method returns all user user interactions with the urls contained within the
    campaigns that were sent by the company
    
    returns -> list of dictionaries"""
        
    # get all click details for all campaigns  
    click_members = []
    total_clicks = 0
    
    for campaign in self.campaigns_data:
      campaign_id = campaign['id']
      click_report = self.client.reports.click_details.all(campaign_id, get_all=True)
      urls = click_report['urls_clicked']

      # get all url details for all click reports
      for url in urls:
        url_link = url['url']
        url_id = url['id']
        total_clicks = url['total_clicks']
        url_params = [url_id, url_link]

        # if there are clicks, grab its detail and append
        if total_clicks > 0:
          click_details_report = self.client.reports.click_details.members.all(campaign_id, url_id, get_all=True)['members']
          click_members += [self.__click_detail_mapper(click_detail, url_params) for click_detail in click_details_report]
  
    return click_members    

  
  def __click_detail_mapper(self, member, params):
    """Helper function to unwrap user click information received from the API

    returns -> dict"""

    return {
         'campaign_id' : member['campaign_id'],
         'clicks' : member['clicks'],
         'email_id' : member['email_id'],
         'list_id' : member['list_id'],
         'url_id': params[0],
         'url': params[1],
    }   
   
    
    
    
#====================================================Transformation functions====================================================       
  def transform_data(self, extracted_data, data_list = None):
    '''
      Function for transforming previously extracted data. Input a dict of
      extracted data with table names for keys (see extract_data above), and a list of tables
      to be extracted. If no tables are specified, transform all tables.

      Outputs dictionary of table names and transformed data, similar to extracted data.
      Also saves it to attribute self.transformed_data

      After converting the extracted data into a dataframe, that dataframe is
      saved in self.extracted_data_df in the same dictionary format. This data
      can be saved in a csv and loaded into this function inplace of the 
      extracted data. This is to save on having to extract every time.

      Tables are: campaigns, users, lists, opens, clicks

      Lists and users are both take from the users data. If not specified, 
      both will be transformed
    '''
    #dictionary of functions to transform each table
    self.transform_func = {
    'campaigns': self.__transform_campaigns,
    'lists': self.__transform_lists,
    'users': self.__transform_users,
    'opens': self.__transform_opens,
    'clicks': self.__transformt_clicks
    }
    
    
    #if the tables to be transformed is not specified, transform all tables
    if(data_list == None):
      data_list = list(extracted_data.keys())
      #if users is in the extracted data, add list to the extraction list as well
      if('users' in extracted_data.keys()):
          data_list.append('lists')
    
    #remove unique values
    data_list = list(set(data_list))
    #remove all invalid values
    transform_list = []
    for val in data_list:
      if(val in self.transform_func):
        transform_list.append(val)
      else:
        print(f'{val} is not an transformable object')
    
    transformed_data = {}
        
    #transform the data
    print('Transforming..')
    for data in transform_list:
      print(f'Transforming {data}...')
      #if lists is being transformed, set the extracted data to lists
      if(data == 'lists'):
        _extracted_data = extracted_data['users']
      else:
        _extracted_data = extracted_data[data]
      transformed_data[data] = self.transform_func[data](_extracted_data)    
      print(f'Finished transforming {data}')
    print('Finished transforming')
      
    self.transformed_data = transformed_data
    return transformed_data 
  
  
#====================================================Transformation functions======================================================        
  def __transform_campaigns(self, extracted_data):
    # 1. Transform campaigns_dict -> df
    df = pd.DataFrame(extracted_data)
    self.extracted_data_df['campaigns'] = df
    
    ## Strip leading/treading whitespaces
    df['subject_line'] = df['subject_line'].str.strip()
    df['title'] = df['title'].str.strip()
    
    col = ['click_rate', 'id', 'open_rate', 'send_time','status', 'subject_line', 'title']
    df = df[col]
    
    df['click_rate'] = df['click_rate'].astype(float)
    df['open_rate'] = df['open_rate'].astype(float)
    df['send_time'] = pd.to_datetime(df['send_time'])
    
    #columns:
    #click_rate (float)
    #id
    #open_rate  (float)
    #send_time	(time)
    #status	
    #subject_line	
    #title
    
    #id is the primary key. get rid of duplicates
    df = (df.drop_duplicates(subset='id')).reset_index(drop = True)
    return df
    

  
  #list and user data are both extracted from users
  def __transform_lists(self,extracted_data):
    #columns:
    #list_id	
    #list_name	
    #email_id   
    return self.__transform_user_lists(extracted_data)[0]
  
  def __transform_users(self,extracted_data):
    #columns:
    #email	
    #email_id	
    #first_name	
    #last_name	
    #status
    
    #email_id is the primary key. get rid of duplicates
    df = self.__transform_user_lists(extracted_data)[1]
    df = (df.drop_duplicates(subset='email_id')).reset_index(drop = True)
    return df

  #user and lists are stored in the same extracted data
  def __transform_user_lists(self,extracted_data):
    # 2. Transform users_dict -> df
    df = pd.DataFrame(extracted_data)
    self.extracted_data_df['users'] = df
    
    df['list_name'] = df['list_name'].str.strip()
    df['first_name'] = df['first_name'].str.strip()
    df['last_name'] = df['last_name'].str.strip()
      
    ## (a) Split off users_df by the list attributes
    col_list   = ['list_id', 'list_name', 'email_id']
    list_df  = df[col_list]
    
    
    ## (b) Drop the list attributes from users_df
    col_users   = ['email', 'email_id', 'first_name', 'last_name', 'status']
    users_df = df[col_users]
    
    
    return [list_df, users_df] 

  
  def __transform_opens(self, extracted_data):
    # 3. Transform opens_dict -> df
    df = pd.DataFrame(extracted_data)
    self.extracted_data_df['opens'] = df
    
#    df['last_open'] = df['last_open'].apply(lambda x: x['timestamp'])
    
    
    col = ['campaign_id', 'email_id', 'last_open', 'opens', 'opens_count']
    df = df[col]
    
    df['last_open'] = pd.to_datetime(df['last_open'])
    df['opens'] = df['opens'].astype(str)
    df['opens_count'] = df['opens_count'].astype(int)

    #columns:
    #campaign_id	
    #email_id	
    #last_open	(date)
    #opens	(list of dates)
    #opens_count (int)
    return df  
  
  
  def __transformt_clicks(self, extracted_data):
    # 4. Transform clicks_dict into dataframe
    df = pd.DataFrame(extracted_data)
    self.extracted_data_df['clicks'] = df
    
    ## Strip leading/treading whitespaces
    
    col = ['campaign_id', 'clicks', 'email_id', 'list_id', 'url',	'url_id']
    df = df[col]
    
    df['url'] = df['url'].str.strip()  
    df['clicks'] = df['clicks'].astype(int)

    
    #columns:
    #campaign_id	
    #clicks	 (int)
    #email_id	
    #list_id	
    #url	
    #url_id
    return df

  

#====================================================Loading functions======================================================  
  def load_data(self, transformed_data, data_list= None):
    '''
      Function for loading transformed data. Input a dict of transformed data 
      with table names for keys (see extract_data above), and a list of tables
      to be loaded. If no tables are specified, load all tables.
        
      Data is loaded row by row. This is so that loading doesn't all fail should
      an error occur. A progress report is given every 50 rows.
      
      Make sure to load campaigns and users first, as there are foreign key dependencies.
      I didn't have time to make this an automatic feature so you have to do it yourself.

      Tables are: campaigns, users, lists, opens, clicks
    '''
    
    #if the tables to be loaded is not specified, load all data in the input
    if(data_list == None):
      data_list = transformed_data.keys()
    
    #load the data by table
    for data in data_list:
      print(f'Loading: {data}...')
      #variables to keep track of rows loaded and errors encountered
      error_count = 0
      load_count = 0
      _transformed_data = transformed_data[data]
      #data is loaded row by row
      for i in range(len(_transformed_data)):
        try:
          _transformed_data.iloc[i:i+1].to_sql(data, self.engine, if_exists='append', index = False)
        except Exception as e: 
          print(e) 
          error_count+=1
          pass 
        else:
          load_count+=1
        #give a progress update every 50 rows. feel free to change this number
        if((load_count+error_count)%50 ==0):
          print(f'Loaded {load_count} / {_transformed_data.shape[0]} entries')
          print(f'Encountered {error_count} errors')   
      print(f'Loaded {load_count} / {_transformed_data.shape[0]} entries')
      print(f'Encountered {error_count} errors')
      
      
      #Main Function
      scrapper = MailChimp_ETL()
      extracted_data = scrapper.extract_data()
      transformed_data = scrapper.transform_data(extracted_data)
      scrapper.load_data(transformed_data, ['campaigns','users'])
      scrapper.load_data(transformed_data, ['lists','opens','clicks'])

