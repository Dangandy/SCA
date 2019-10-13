# 3rd party imports
import requests
import json
import datetime
import time
import pandas as pd

from google.colab import drive
drive.mount('/content/drive')


# functions
def get_request(url):
  """
    str -> (int, int, str, dataframe)
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
    df = pd.read_json(json.dumps(json_response_data))
    return (currentPage, totalPages, pageNavigationToken, df)

  # Failed.. Retry
  elif response.status_code == 429:
    print("ERROR!")
    print(f'Status Code: {response.status_code}')
    print(f'Content: {response.content}')
    json_response_retryAfter = json_response['retryAfter']
    time.sleep(json_response_retryAfter)
    print("Retrying....")
    return get_request(url)

  # FAIL
  else:
    print("ERROR!")
    print(f'Status Code: {response.status_code}')
    print(f'Content: {response.content}')
    return(9999, -1, "", None)


def get_one_month_bookings(year, month):
  """
    int, int -> df
    Get One month of booking information from Bookeo
    Note: Bookeo only allows fetches of 31 days
  """

  # get start and end time formatted
  startTime = datetime.datetime(year, month, 1).strftime("%Y-%m-%dT%H:%M:%S-04:00")
  endTime = datetime.datetime(int(year + (month / 12)), (month % 12) + 1, 1).strftime("%Y-%m-%dT%H:%M:%S-04:00")

  # call GET
  url = f'https://api.bookeo.com/v2/bookings?secretKey={secretKey}&apiKey={apiKey}&startTime={startTime}&endTime={endTime}&itemsPerPage=100'
  currentPage, totalPages, pageNavigationToken, df = get_request(url)

  while currentPage < totalPages:
    next_page_url = f'https://api.bookeo.com/v2/bookings?secretKey={secretKey}&apiKey={apiKey}&pageNavigationToken={pageNavigationToken}&pageNumber={currentPage + 1}&itemsPerPage=100'
    currentPage, _, _, _df = get_request(next_page_url)
    df = df.append(_df, ignore_index=True, sort=False)

  # ERROR
  if totalPages == -1:
    print(f"Progress: {month}-{year}")
  
  return df
  
  
def get_one_year_bookings(year):
  """
  
  int -> dataframe
  start from january, and keep calling `get_request()` until it is either december or current month

  Pseudocode:
    1. get all months for year
    2. call get one month booking
    3. return df
  """

  # variables
  last_month = 12
  current_month = 1
  df = pd.DataFrame()

  # current year
  if datetime.datetime(year, 12, 31) > datetime.datetime.now():
    last_month = datetime.datetime.now().month

  # loop through all months
  while current_month <= last_month:
    _df = get_one_month_bookings(year, current_month)
    df = df.append(_df, ignore_index=True, sort=False)
    current_month = current_month + 1

  return df
  
  
# creds
secretKey = ''
apiKey = ''

# get all year data
year_2019 = get_one_year_bookings(2019)
year_2018 = get_one_year_bookings(2018)
year_2017 = get_one_year_bookings(2017)

black_creek = year_2019.append(year_2019, ignore_index=True, sort=False)
black_creek = black_creek.append(year_2018, ignore_index=True, sort=False)
black_creek = black_creek.append(year_2017, ignore_index=True, sort=False)

# save to csv
path = '/content/drive/My Drive/'
black_creek.to_csv(path+"black creek.csv", index=False)
