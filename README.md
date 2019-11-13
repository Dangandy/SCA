# Secret City Adventures

---

## Goal: 
- To create an Extract data from Bookeo and Mailchimp, transform it into a usable DataFrame, and Load it into an AWS RDS instance

---

## Tools:
- [Amazon](https://aws.amazon.com/)
  - RDS
  - EC2 with Airflows
- postgres
- postman
- [pgAdmin](https://www.pgadmin.org/download/)

---

## Tutorial Bookeo:

### Step 1: Create Database
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

## FAQ:

### I cannot connect to the RDS instance
  - Check if your postgres is turned on, for macOS this command is: `brew services start postgresql`


### I got an internal server error when I try to connect my RDS instance with pgAdmin
  - You may need to change your security inbound rules 
  - Allow all traffic for `0.0.0.0/0` and `::/0`
  - [Instruction for changing inbound rules](https://docs.aws.amazon.com/vpc/latest/userguide/VPC_SecurityGroups.html#SG_Changing_Group_Membership)
---

## Additional Resources:

### Creating and connecting to Amazon RDS with pgAdmin
  - https://www.youtube.com/watch?v=0GpQJM7w6M8
  - https://www.youtube.com/watch?v=vK8dbx0xhGE
