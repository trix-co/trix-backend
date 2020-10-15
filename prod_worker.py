import sys
sys.path.append('/home/ubuntu/fawkes/fawkes')

from datetime import datetime
import random
import string
import json
import os
import boto3
import time
import glob
import shutil
import multiprocessing
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore import Increment
from base64 import b64decode
from protection_compute_frontloaded import Fawkes
from utils import FaceNotFoundError
from format_demo_output import format_demo_output

global NUM_MESSAGES
NUM_MESSAGES = 3

def get_random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))


def initialize():
    session = boto3.session.Session(profile_name="do")
    global client
    client = session.client('s3', endpoint_url=S3_ENDPOINT_URL)

# the work function of each process which will fetch something from s3
def download(job):
    bucket, key, filename = job
    i = 0
    while i < 10:
        try:
            client.download_file(bucket, key, filename)
            break
        except Exception as e:
            print(e)
            i += 1
            time.sleep(5)

def consume_messages():
    print("consuming messages!")
    sqs = boto3.client('sqs', aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET, region_name="us-west-2")
    queue_url = SQS_QUEUE_URL
    #print("queues: ", sqs.list_queues())

    message_bodies = []
    id_ctr = 1000
    receipt_entries = []
    while True:
        # Receive message from SQS queue
        response = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=[
                'SentTimestamp'
            ],
            MaxNumberOfMessages=NUM_MESSAGES,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=30,
            WaitTimeSeconds=0
        )

        try:
            messages = response['Messages']
        except Exception as e:
            #no messages!
            break

        for mess in messages:
            message_bodies.append(json.loads(mess["Body"]))
            receipt_entries.append({'Id': str(id_ctr),'ReceiptHandle': mess['ReceiptHandle']})
            id_ctr += 1

        # max messages recieved!
        if (len(message_bodies) == NUM_MESSAGES):
            break

        #print(api_req)

    if len(message_bodies) > 0:
        del_response = sqs.delete_message_batch( QueueUrl=queue_url, Entries=receipt_entries)
        print(del_response)
    return {"messages":message_bodies}


if __name__ == "__main__":
    #
    random.seed(datetime.now())
    protector = Fawkes("high_extract", "0", 1)
    #firebase stuff
    cred = credentials.Certificate("trix-ai-app-firebase-adminsdk-rxzfw-aabec76c1d.json")
    firebase_admin.initialize_app(cred)
    firestore_db = firestore.client()
    while True:
        content = consume_messages()
        print(content)
        if len(content['messages']) == 0:
            print("no messages in queue, waiting 10 seconds!")
            time.sleep(10)
            continue

        jobs = []
        dirname = get_random_string(12)
        os.mkdir(dirname)
        #dirname = "qyzijmbmshsu"
        for mess in content["messages"]:
            messy = json.loads(mess)
            url_parts = messy["imageUrl"].split('/')
            s3_path = url_parts[-2] + '/'+ url_parts[-1]
            dl_path = dirname + '/'+ url_parts[-1]
            jobs.append(('trix', s3_path, dl_path))

        #download images
        pool = multiprocessing.Pool(5, initialize)
        pool.map(download, jobs)
        pool.close()
        pool.join()

        image_paths = glob.glob(os.path.join(dirname, "*"))
        file_ra = [path for path in image_paths if "_cloaked" not in path.split("/")[-1]]
        print(file_ra)


        try:
            protector.run_protection(file_ra, mode="low", th=.01, sd=1e9, lr=2, max_step=1000, batch_size=1, format="png", separate_target=True, debug=False)
        except Exception as inst:
            if isinstance(inst, FaceNotFoundError):
                print("no faces found!")
                pass
            else:
                print("something went wrong!", inst)


        batch = firestore_db.batch()
        #digital ocean stuff
        session = boto3.session.Session(profile_name="do")
        client = session.client('s3', endpoint_url=S3_ENDPOINT_URL)
        for mess in content["messages"]:
            messy = json.loads(mess)
            new_mess = dict(messy)
            url_parts = messy["imageUrl"].split('/')
            ul_path = dirname + '/' + url_parts[-1][:-4] + '_low_cloaked.jpg'
            ul_s3_path = 'processed'+'/'+ url_parts[-1]
            #decrement = firestore.FieldValue.increment(-1)
            user_ref = firestore_db.collection(u'users').document(messy['uid'])

            try:
                user_ref.update({u'unprocessedCount': Increment(-1)})
                client.upload_file(ul_path, 'trix', ul_s3_path)
                new_mess["unalteredImageUrl"] = new_mess.pop("imageUrl")
                new_mess["imageUrl"] = S3_CDN_URL + ul_s3_path
            except FileNotFoundError as e:
                #image not processed with model
                print("No file to upload found, uploading original")
                time.sleep(3)
                new_mess["unalteredImageUrl"] = new_mess["imageUrl"]

            new_ref = firestore_db.collection(u'trixpix').document()
            batch.set(new_ref, new_mess)

        batch.commit()
        shutil.rmtree(dirname)







