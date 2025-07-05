#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install faker 


# In[2]:


import csv
from faker import Faker
import random

# Initialize Faker
fake = Faker()

# Function to generate customer data
def generate_customer_data(num_records):
    data = []
    for i in range(1, num_records + 1):
        customer_id = i
        customer_name = fake.name()
        email = fake.email()
        join_date = fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d')
        active = random.choice([True, False])
        data.append([customer_id, customer_name, email, join_date, active])
    return data

# Define output file
output_file = 'sample_customer_data.csv'

# Generate data
customer_data = generate_customer_data(100)

# Write to CSV
with open(output_file, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(["customer_id", "customer_name", "email", "join_date", "active"])
    writer.writerows(customer_data)

print(f"Sample customer data CSV file created: {output_file}")


# In[3]:


import csv
from faker import Faker
import random
import os

# Initialize Faker
fake = Faker()

# Function to generate customer data
def generate_customer_data(num_records):
    data = []
    for i in range(1, num_records + 1):
        customer_id = i
        customer_name = fake.name()
        email = fake.email()
        join_date = fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d')
        active = random.choice([True, False])
        data.append([customer_id, customer_name, email, join_date, active])
    return data

# Define Desktop Path for Output
desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
output_file = os.path.join(desktop_path, 'sample_customer_data.csv')

# Generate data
customer_data = generate_customer_data(100)

# Write to CSV
with open(output_file, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(["customer_id", "customer_name", "email", "join_date", "active"])
    writer.writerows(customer_data)

print(f"Sample customer data CSV file created on Desktop: {output_file}")


# In[ ]:




