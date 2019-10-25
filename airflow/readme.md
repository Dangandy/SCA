# How to set up Airflow

1. Set up Amazon EC2 instance
2. Install [airflow](https://airflow.apache.org/start.html) in EC2
  - get in airflow admin and check around ( codes )
  
  Excercise Create and Test the following DAGS:
  
    1. print context
    2. print OS variables
    3. combine the 2 dags together
    
  Assignment:
  
    - import Bookeo class
    - Perform Extract
      - push data into XCOM
    - Transform Extract
      - pull data from XCOM
      
3. Install the following libraries for postgres
```bash
sudo apt-get install postgresql
sudo apt-get install python3-psycopg2
sudo apt-get install libpq-dev
```
---

FAQ:

Permissions 0644 for '1000mlproduction.pem' are too open.
> Set the permissions with `chmod 400 filename.pem`

Connection closed by port 22
> make sure you have the username of the server in the IP name `ssh -i "filename" ubuntu@...`

Can't find `pip3`
> you need to update your package manager (apt for ubuntu):
```bash 
sudo apt-get update
sudo apt install python3-pip
```

`airflow: Command not found` after pip3 install
> you need to source the profile `source ~/.profile`
