from tokenize import String
import project_config
import requests
import json,csv
#Function to request an OAuth token, this toke will expire after ~2 hours, and a new one will need to be requested.
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

#Attempt to write a function to parse this json result and save to csv. Too hard to treat the text the posts.
def reddit_json_to_csv(input_json_text:String,result_csv_path:String):
    """
       A function used to parse the result of reddit api.
        Input
        ----------
        input_json_text : str
            a json string from the reddit api.
        result_csv_path : str
            file path for the result
    """
    with open(result_csv_path, 'w', newline='') as csvfile:

        writer = csv.writer(csvfile, delimiter='|')
        writer.writerow(['post_title','author','post_category','post_time_utc','last_commet_time_utc','post_text'])
        for post in input_json_text['data']['children']:
            title = post['data']['title']
            author = post['data']['author']
            link_flair_text = post['data']['link_flair_text']
            #created_at_utc = post['data']['collections'][0]['created_at_utc'] #Not all post have these...
            #last_update_utc = post['data']['collections'][0]['last_update_utc'] #Not all post have these...
            selftext = post['data']['selftext']
            writer.writerow([title,author,link_flair_text,selftext])
    


api_headers = create_temp_auth()
#api_headers = {'User-Agent': 'DataEngSubscraper/0.0.1', 'Authorization': 'bearer 20340382-PD7s2j2eidebvVFNJfCMJGyQEP1WrQ'}

res = requests.get("https://oauth.reddit.com/r/dataengineering/hot",
                   headers=api_headers)

res_json = res.json()

reddit_json_to_csv(res_json,'./reddit_api_result.csv')
    



