import requests
import json
import boto3
from datetime import datetime, timezone
import os

# Setting up local paramters
s3_client = boto3.client("s3")
LOCAL_FILE_SYS = "/tmp"
S3_BUCKET = "batch-processing-reddit-api"
FILE_NAME = "reddit_api_top_posts.json"


def _get_timestamp():
    dt_now = datetime.now(tz=timezone.utc)
    KEY = (
        dt_now.strftime("%Y-%m-%d")
        + "_"
        + dt_now.strftime("%H")
        + "_"
        + dt_now.strftime("%M")
        + "_"
    )
    return KEY

def create_temp_auth():

    # Setting up config parameters
    client_id = os.environ['reddit_client_id']
    secret_token = os.environ['reddit_secret_token']
    username = os.environ['reddit_username']
    password = os.environ['reddit_password']

    auth = requests.auth.HTTPBasicAuth(client_id,secret_token)

    # Setting up access method
    data = {'grant_type': 'password'
            ,'username': username
            ,'password': password}

    # setup our header info, which gives reddit a brief description of our app
    headers = {'User-Agent': 'DataEngSubscraper/0.0.1'}

    # Request OAuth token 
    res = requests.post('https://www.reddit.com/api/v1/access_token',
                    auth=auth, data=data, headers=headers)

    # convert response to JSON and pull access_token value
    TOKEN = res.json()['access_token']

    # add authorization to our headers dictionary
    headers = {**headers, **{'Authorization': f"bearer {TOKEN}"}}

    # while the token is valid (~2 hours) we just add headers=headers to our requests
    requests.get('https://oauth.reddit.com/api/v1/me', headers=headers)
    return headers

def get_today_top_posts_json(headers):
    """This function returns the top 100 posts in the last 24 hours."""
    posts = requests.get("https://oauth.reddit.com/r/dataengineering/top",
                   headers=headers, params={'t':'day','limit': '100'})

    posts_json = posts.json()

    return posts_json


#Attempt to write a function to parse this json result and save to csv. Too hard to treat the text the posts.
def parse_posts_json(input_json):
    """A function for parsing the result posts of reddit api calls. Optional, api result data can be stored as raw json instead."""
    posts=[]
    for post in input_json['data']['children']:
        
        row=dict()
        row["post_title"] = post['data']['title']
        row["post_id"] = post['data']['id']
        row["subreddit_id"] = post['data']['subreddit_id']
        row["post_author"] = post['data']['author']
        row["link_flair_text"] = post['data']['link_flair_text']         
        row["created_at_utc"] = post['data']['created_utc']
        row["post_text"] = post['data']['selftext']
        row["post_score"] = post['data']['score']
        
        posts.append(row.copy())
    output_json = json.dumps(posts, indent = 4)

    return output_json

def write_to_local(data,name,loc=LOCAL_FILE_SYS):
    local_filepath=loc+'/'+str(name)
    with open(local_filepath,'w') as file:
        file.write(data)


#lambda_handler is the entry point for the start of the lambda execution
def lambda_handler (event, context):
    #Create auth
    api_headers = create_temp_auth()

    #Get and parse the result from the api call
    posts_json = get_today_top_posts_json(api_headers)
    processed_posts_json = parse_posts_json(posts_json)

    #Store the result in local drive first
    key = _get_timestamp()
    write_to_local(processed_posts_json,key+FILE_NAME)

    #push to s3
    s3_client.upload_file(LOCAL_FILE_SYS+'/'+key+FILE_NAME, S3_BUCKET, "raw/posts/"+key+FILE_NAME)
    return("API result is now uploaded into s3:// "+S3_BUCKET+"raw/posts/"+key+FILE_NAME)

