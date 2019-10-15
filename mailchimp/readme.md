# Mailchimp

Please look in our group chat for the credentials

---

## Exploring Data

1. go to mailchimp playground:
  https://us1.api.mailchimp.com/playground/

2. enter the api key to and you can now visualize what you can grab from the API

---

## Exploring APIs (using google colab)

1. `!pip install mailchimp3`
2. import and enter api key
```
from mailchimp3 import MailChimp
client = MailChimp(mc_api='APIKEY', mc_user='USERNAME')
```
3. start exploring

---

## Analysis

- 17 lists
- 136 campaigns
- All queries result in a dict with the following structure `{query_item, total_item, links}`

---

## Schema

Below are the columns for the CSV:
  - Email: Email
  - Lists: [string]
  - Campaigns: [string]
  - Campaign Details: {
    opens: int,
    clicks: int,
    last open: date,
    last click: date
  }
  
---

## Additional Resources:
[Python Mailchimp Library (v3)](https://github.com/VingtCinq/python-mailchimp)
