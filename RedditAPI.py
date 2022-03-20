import project_config
import requests
import json,csv


def create_temp_auth():

    # Setting up config parameters
    client_id = project_config.redditAPI_personal_use_script
    secret_token = project_config.redditAPI_secret_token
    username = project_config.redditAPI_username
    password = project_config.redditAPI_password

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

def main():
    api_headers = create_temp_auth()
    posts_json = get_today_top_posts_json(api_headers)
    processed_posts_json=parse_posts_json(posts_json)

    with open('json_data.json', 'w') as outfile:
        outfile.write(processed_posts_json)
        
if __name__ == "__main__":
    main()