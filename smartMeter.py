from google.cloud import pubsub_v1  # pip install google-cloud-pubsub
import glob  # for searching for JSON file
import json
import os
import csv

#Search the current directory for the JSON file (including the service account key)
#to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]


# Set the project_id with your project ID
project_id = "dynamic-nomad-448416-h3"
topic_name = "CSVRecords"

# Create a publisher and get the topic path for the publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Publishing messages to {topic_path}.")

# Path to the CSV file
file_path = "Labels.csv"

# Function to Convert Data Types from CSV
# I uses this function because the the format data we getting is not same as mysql use 
def change_value(x):
    """Convert CSV values to int, float, or keep as string."""
    try:
        if "." in x:  # If the number has a decimal, convert to float
            return float(x)
        return int(x)  # if not, convert to int
    except ValueError:
        return x  # If the conversion fails, return as string

# Open and Read the CSV File
with open(file_path, mode='r') as csv_file:
    reader = csv.DictReader(csv_file)

    for row in reader:
        # Convert each value so that it matches SQl table format
        row = {key: change_value(value) for key, value in row.items()}

        # Serialize the message correctly
        message = json.dumps(row).encode('utf-8')

        #send the value
        print("Publishing record:", message)
        # Publish the message
        future = publisher.publish(topic_path, message)

        #ensure that the publishing has been completed successfully
        future.result()

print("All records are published.")
