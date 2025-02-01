from google.cloud import pubsub_v1  # Install with: pip install google-cloud-pubsub
import glob
import json
import os
import base64

# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files = glob.glob("*.json")  
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]  

# Set the project_id with your project ID
project_id="dynamic-nomad-448416-h3";
topic_name = "ImagesRecords";   # change it for your topic name if needed

# Enable ordering for Pub/Sub messages
publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
publisher = pubsub_v1.PublisherClient(publisher_options=publisher_options)
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Published messages with ordering keys to {topic_path}.")

# path to the images folder
images_path = "Dataset_Occluded_Pedestrian/"  

# Find all image files
files = glob.glob(os.path.join(images_path, "*.*"))  # Finds all files in the folder

print(f"Publishing images to {topic_name}...")
for image_path in files:
    with open(image_path, "rb") as image_file:
       
        value = base64.b64encode(image_file.read()) # read the image and serizalize it to base64

    key = os.path.basename(image_path)  # 🔹 Extract the image name as the key

    try:
       
        future = publisher.publish(topic_path, value, ordering_key=key)
         #ensure that the publishing has been completed successfully
        future.result()  
        print(f"Published the key: {key}")
    except Exception as e:
        print(f"Failed to publish {key}")

print("All images are published.")