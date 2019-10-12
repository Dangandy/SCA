# Secret City Adventures

---

## Goal: 
- use lambda scheduler to periodically (One Month) feed info (Bookeo) into Amazon RDS

---

## Tools:
- Amazon
  - RDS
  - Lambda
- postgres
- postman
- pgAdmin

---

## Tutorial

### Steps 1: Create Database
  1. Create Amazon RDS postgres instance
  2. Connect RDS with pgAdmin
  3. Create a table with pgAdmin
  4. Add a record into table using pgAdmin
  
### Step 2: Explore Bookeo Api
  4. Grab one month of booking info from Bookeo Api using Postman
    - use pagerequesttoken identifier if there are more than 100 records per request
  5. Grab all customers
  6. grab all payment info

### Step 3: Import data into database
  7. Build a schema for each table (3)
    - should we separate the tables by location? Casa Loma & Black Creek?
  8. Import one month of booking data into database
  
### Step 4: Python AWS Lambda
  9. Create a Lambda function using canary as the blueprint
  10. insert record into your database using handler
  
---

## Additional Resources:
