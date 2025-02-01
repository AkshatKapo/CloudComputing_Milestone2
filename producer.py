        
from google.cloud import pubsub_v1  # pip install google-cloud-pubsub
import glob  # for searching for JSON file
import json
import os
import csv

#Search the current directory for the JSON file (including the service account key)
#to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.

files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

#Set the project_id with your project ID
project_id = "dynamic-nomad-448416-h3"
topic_name = "CSVRecords"

# create a publisher and get the topic path for the publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Publishing messages to {topic_path}.")

#Stores path to the Labels.csv file
file_path = "Labels.csv"

#Open and read the CSV file
with open(file_path, mode='r') as csv_file:
    reader = csv.DictReader(csv_file)
    for row in reader:

        message = json.dumps(row).encode('utf-8') # serialize the message


        #send the value
        print("Publishing record:", message)
        future = publisher.publish(topic_path, message)

       #ensure that the publishing has been completed successfullyclar
        future.result()

print("All the records are published.")