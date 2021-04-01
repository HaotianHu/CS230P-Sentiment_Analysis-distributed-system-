
import json
from getpass import getpass
import requests


CLIENT_ID = "FXL8TUARfAJYLw"
CLIENT_SECRET = "ke6ZuLtXm8VL5CpdmZwGrzetVyc"
USER_AGENT = "python:hikerpi_developer_api_01 (by /u/hikerpi_developer01)"
USERNAME = "hikerpi_developer01"
PASSWORD = "hikerpi_developer01"


def login(username, password):
    if password is None:
        password = getpass.getpass("Enter reddit password"
                                   " for user {}: ".format(username))
    headers = {"User-Agent": USER_AGENT}
    client_auth = requests.auth.HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET)
    post_data = {"grant_type": "password", "username": username,
                 "password": password}
    try:
        response = requests.post("https://www.reddit.com/api/v1/access_token",
                                 auth=client_auth, data=post_data,
                                 headers=headers)
    except Exception as e:
        print(e)
        exit()
    return response.json()



# subreddit = "worldnews"
# url = "https://oauth.reddit.com/r/{}".format(subreddit)
# headers = {"Authorization": "bearer {}".format(token['access_token']), "User-Agent": USER_AGENT}
# response = requests.get(url, headers=headers)
# result = response.json()
# for story in result['data']['children']:
#     print(story['data']['title'])

from time import sleep


def get_subreddits(topic, token, n_pages=1):
    global global_subreddits
    after = None
    for page_number in range(n_pages):
        headers = {"Authorization": "bearer {}".format(token['access_token']), "User-Agent": USER_AGENT}
        url = "https://oauth.reddit.com/subreddits/search?q={}&limit=1".format(topic)
        if after:
            url += "&after={}".format(after)
        try:
            response = requests.get(url, headers=headers)
        except Exception as e:
            print(e)
        results = response.json()
        after = results['data']['after']
        sleep(0.5)
        global_subreddits.extend((result['data']['title'], result['data']['url'],
                                  result['data']['public_description'], result['data']['subscribers'], topic)
                                 for result in results['data']['children'])


def get_links(subreddit, token, n_pages=10):
    global global_after
    stories = []
    after = global_after
    this_topic = subreddit[4]
    # after = None
    for page_number in range(n_pages):
        headers = {"Authorization": "bearer {}".format(token['access_token']), "User-Agent": USER_AGENT}
        url = "https://oauth.reddit.com{}?limit=100".format(subreddit)
        if after:
            url += "&after={}".format(after)
        try:
            response = requests.get(url, headers=headers)
        except Exception as e:
            print(e)
            continue
        result = response.json()
        after = result['data']['after']
        global_after = after
        sleep(0.5)
        stories.extend([(story['data']['title'], story['data']['url'],
                         story['data']['score'], story['data']['id'], story['data']['subreddit'],
                         story['data']['selftext'], story['data']['created_utc'], this_topic)
                        for story in result['data']['children']])
    return stories


def get_comments_and_replies(article_subreddit, article_id, token):
    comments = []
    headers = {"Authorization": "bearer {}".format(token['access_token']), "User-Agent": USER_AGENT}
    url = "https://oauth.reddit.com/r/{0}/comments/{1}?depth=3&limit=9999".format(article_subreddit, article_id)
    try:
        response = requests.get(url, headers=headers)
    except Exception as e:
        print(e)
        return comments
    results = response.json()
    sleep(0.5)
    data = results[1]['data']
    comments.extend((result['data']['body'], result['data']['score'],
                     result['data']['replies'], result['data']['author'],
                     result['data']['id'], result['data']['created_utc'])
                    for result in results[1]['data']['children']
                    if 'body' in result['data'])
    return comments


def collect_comments(comments, dataframe):
    for body, score, replies, author, id, createdTime in comments:
        sub_comments = []
        localtime = time.localtime(createdTime)
        dt = time.strftime("%Y-%m-%d %H:%M:%S", localtime)
        # temp = {
        #     "body": "{}".format(body),
        #     "score": "{}".format(score),
        #     "author": "{}".format(author),
        #     "id": "{}".format(id),
        #     "createdTime": "{}".format(dt)
        # }
        new = pd.DataFrame({"body": body, "score": score, "author": author,
                            "id": id, "createdTime": dt}, index=[1])
        dataframe = dataframe.append(new, ignore_index=True)
        # comment_filepath = comments_folder + "/{}.txt".format(id)
        # with open(comment_filepath, 'w', encoding='utf-8') as outf:
        #     outf.write(json.dumps(temp, indent=4))
        if replies != '':
            sub_comments.extend((reply['data']['body'], reply['data']['score'],
                                 reply['data']['replies'], reply['data']['author'],
                                 reply['data']['id'], reply['data']['created_utc'])
                                for reply in replies['data']['children']
                                if 'body' in reply['data'])
            if len(sub_comments) != 0:
                dataframe = collect_comments(sub_comments, dataframe)
    return dataframe


import os
import hashlib


def get_html_from_url(url):
    article_data = ""
    number_errors = 0
    output_filename = hashlib.md5(url.encode()).hexdigest()
    article_folder = data_folder + "/{}".format(output_filename)
    if not os.path.exists(article_folder):
        os.mkdir(article_folder)

    #    fullpath = article_folder + "/{}_raw.txt".format(output_filename)
    while True:
        try:
            response = requests.get(url)
            article_data = response.text
            break
            # with open(fullpath, 'w', encoding='utf-8') as outf:
            #     outf.write(article_data)
        except Exception as e:
            number_errors += 1
            print(e)

    return article_data, output_filename, article_folder


import lxml
from lxml import etree



def get_text_from_html(data):
    skip_node_types = ["script", "head", "style", etree.Comment]

    def get_text_from_file(data):
        parser = etree.HTMLParser(encoding="utf-8")
        html_tree = lxml.etree.fromstring(data, parser=parser)
        return get_text_from_node(html_tree)

    def get_text_from_node(node):
        if len(node) == 0:
            # No children, just return text from this item
            if node.text and len(node.text) > 100:
                return node.text
            else:
                return ""
        results = (get_text_from_node(child) for child in node if child.tag not in skip_node_types)
        return "\n".join(r for r in results if len(r) > 1)

    text = get_text_from_file(data)
    return text


import time
import pandas as pd


def get_text_from_reddit(token, topic, needlinktext=True):
    global global_subreddits
    global global_after
    df_final = pd.DataFrame()
    for subreddit in global_subreddits:
        stories = []
        stories = get_links(subreddit[1], token)
        for title, url, score, id, subreddit, selftext, createdTime, this_topic in stories:
            localtime = time.localtime(createdTime)
            dt = time.strftime("%Y-%m-%d %H:%M:%S", localtime)
            output_filename = hashlib.md5(url.encode()).hexdigest()
            # html, output_filename, article_folder = get_html_from_url(url)
            # linktext = ""
            # if needlinktext:
            #     try:
            #         linktext = get_text_from_html(html)
            #     except Exception as e:
            #         print(e)

            # temp = {
            #     "title": "{}".format(title),
            #     "url": "{}".format(url),
            #     "score": "{}".format(score),
            #     "id": "{}".format(id),
            #     "subreddit": "{}".format(subreddit),
            #     "selftext": "{}".format(selftext),
            #     "linktext": "{}".format(linktext),
            #     "createdTime": "{}".format(dt)
            # }
            # df = pd.DataFrame(columns=["title", "url", "score", "id", "subreddit",
            #                            "selftext", "linktext", "createdTime"], dtype=str)

            # article_folder = data_folder + "/{}".format(output_filename)
            # fullpath = article_folder + "/{}.txt".format(output_filename)
            # with open(fullpath, "w", encoding='utf-8') as fp:
            #     fp.write(json.dumps(temp, indent=4))
            comments = get_comments_and_replies(subreddit, id, token)

            df_comments = pd.DataFrame(columns=["body", "score", "author", "id",
                                                "createdTime", "topic", "date"], dtype=str)



            if len(comments) != 0:
                df_comments = collect_comments(comments, df_comments)
                df_comments["topic"] = topic
                df_comments['date'] = '0'
                for i in range(len(df_comments)):
                    df_comments['date'][i] = df_comments['createdTime'][i][0:4] + \
                                             df_comments['createdTime'][i][5:7] + \
                                             df_comments['createdTime'][i][8:10]
                #insert_to_mango(df_comments)
            df_final = df_final.append(df_comments, ignore_index=True)

        global_subreddits = global_subreddits[1:]
        global_after = None

    output_filename = topic
    csv_path = data_folder + "/{}.csv".format(output_filename)
    file = open(csv_path, "w", encoding='utf-8')
    df_final.to_csv(file, encoding='utf-8')
    file.close()


from pymongo import MongoClient
from pymongo import InsertOne


def insert_to_mango(df):
    conn = MongoClient("localhost:27017", maxPoolSize=None)
    my_db = conn['reddit_text']
    my_collection = my_db['Reddit']
    try:
        my_collection.insert_many(json.loads(df.T.to_json()).values())
    except Exception as e:
        print(e)
    conn.close()



topics = ["uber","covid-19","google","microsoft","facebook","oracle","vmware","nvidia","amd","intel","tencent","siemenz","bmw","IBM"]
global global_after
global global_subreddits
global_subreddits = []
global_after = None
#token = login(USERNAME, PASSWORD)
requests.adapters.DEFAULT_RETRIES = 5
s = requests.session()
s.keep_alive = False

data_folder = "D:\pythonWorkSpace\dataProject\hikerpi/redditData/230before"

for topic in topics:
    try:
        token = login(USERNAME, PASSWORD)
        get_subreddits(topic, token)
        get_text_from_reddit(token, topic)
        global_subreddits = []
    except Exception as e:
        print(e)


