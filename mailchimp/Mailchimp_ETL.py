#Imports for MailChimpETL
import json
import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
!pip install mailchimp3
!pip install flat_table
import flat_table
from mailchimp3 import MailChimp

#Create the class for Bookeo scrapper

class MailChimp_ETL():
  """An instance of this class sets up the connection to the Mailchimp API and 
  retrieves the data in a json format to then transform it into a pandas dataframe
  and load it into a SQL database"""
  
  def __init__(self):
    """Instantiate the class and create internal attributes"""
    
    # Credentials
    mc_user= username
    mc_api= api
    self.client = MailChimp(mc_api=mc_api, mc_user=mc_user)
    
    
    self.db_url = databaseurl
    self.engine = create_engine(self.db_url)
  
    self.extracted_data = {}
    self.extracted_data_df = {}
    self.transformed_data = {}
       
  def __str__(self):
    return 'This is a template for the MailChimp ETL class' 
  

#====================================================Extraction functions====================================================       
  def extract_data(self, data_list = None): 
    #dictionary of functions for extaction
    self.extract_func = {
        'campaigns': self.__extract_campaigns,
        'users': self.__extract_users,
        'opens': self.__extract_opens,
        'clicks': self.__extract_clicks
    }    
    
    
    if(data_list==None):
      data_list = list(self.extract_func.keys())
      
    #campaigns will always be extracted
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
    
    extracted_data = {}
    
    #extract campaigns first
    print(f'Extracting campaigns...')
    self.campaigns_data = self.__extract_campaigns()
    extracted_data['campaigns'] = self.campaigns_data 
    print(f'Finished extracting campaigns')
    
    #extract each item in the extract list
    for data in extract_list:
      print(f'Extracting {data}...')
      extracted_data[data] = self.extract_func[data]()    
      print(f'Finished extracting {data}')
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
    self.transform_func = {
        'campaigns': self.__transform_campaigns,
        'lists': self.__transform_lists,
        'users': self.__transform_users,
        'opens': self.__transform_opens,
        'clicks': self.__transformt_clicks
    }
    if(data_list == None):
      data_list = list(extracted_data.keys())
      if('users' in extracted_data.keys()):
        data_list.append('lists')
    
    data_list = list(set(data_list))
    
    transform_list = []
    for val in data_list:
      if(val in self.transform_func):
        transform_list.append(val)
      else:
        print(f'{val} is not an transformable object')
    
    transformed_data = {}
        
    #transform the data
    for data in transform_list:
      print(f'Transforming {data}...')
      #lists has to to be extracted from the users table
      if(data == 'lists'):
        _extracted_data = extracted_data['users']
      else:
        _extracted_data = extracted_data[data]
      transformed_data[data] = self.transform_func[data](_extracted_data)    
      print(f'Finished transforming {data}')

    print('Transforming..')

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
    df = self.__transform_user_lists(extracted_data)[1]
    df = (df.drop_duplicates(subset='email_id')).reset_index(drop = True)
    return df

  
  def __transform_user_lists(self,extracted_data):
    # 2. Transform users_dict -> df
    df = pd.DataFrame(extracted_data)
    self.extracted_data_df['users'] = df
    
    df['list_name'] = df['list_name'].str.strip()
    df['first_name'] = df['first_name'].str.strip()
    df['last_name'] = df['last_name'].str.strip()
      
    ## (a) Split off users_df by the list attributes
    col_list_df   = ['list_id', 'list_name', 'email_id']
    list_df  = df[col_list_df]
    
    ## (b) Drop the list attributes from users_df
    users_df = df.drop(columns = ['list_id', 'list_name'])
    
    
    return [list_df, users_df] 

  
  def __transform_opens(self, extracted_data):
    # 3. Transform opens_dict -> df
    df = pd.DataFrame(extracted_data)
    self.extracted_data_df['opens'] = df
    
    df['last_open'] = df['last_open'].apply(lambda x: x['timestamp'])
    
    df['last_open'] = pd.to_datetime(df['last_open'])
    #df['opens'] = pd.to_datetime(df['opens'])
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
    if(data_list == None):
      data_list = transformed_data.keys()
    
    for data in data_list:
      print(f'Loading: {data}...')
      error_count = 0
      load_count = 0
      _transformed_data = transformed_data[data]
      for i in range(len(_transformed_data)):
        try:
          _transformed_data.iloc[i:i+1].to_sql(data, self.engine, if_exists='append')
        except Exception as e: 
          print(e)
          error_count+=1
          pass 
        else:
          load_count+=1
        if((load_count+error_count)%50 ==0):
          print(f'Loaded {load_count} / {_transformed_data.shape[0]} entries')
          print(f'Encountered {error_count} errors')   
      print(f'Loaded {load_count} / {_transformed_data.shape[0]} entries')
      print(f'Encountered {error_count} errors')
 
 
 
#code to extract, transform and load all data
scrapper = MailChimp_ETL()
#extract
extracted_data = scrapper.extract_data()
#transform
transformed_data = scrapper.transform_data(extracted_data)
#load
scrapper.load_data(transformed_data)


