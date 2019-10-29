#!/usr/bin/env python


from collections import defaultdict
import sqlite3
import time
import datetime
from pprint import pprint
import matplotlib.pyplot as plt
from wordcloud import WordCloud

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


conn = sqlite3.connect("logs.db")
conn.row_factory = dict_factory

cur = conn.cursor()

cmd = 'SELECT * FROM users'
values = cur.execute(cmd).fetchall()

user_map = {}
for user in values:
    user_map[user['user_id']] = {}
    user_map[user['user_id']]['name'] = user['name']
    user_map[user['user_id']]['stats'] = defaultdict(lambda: 0)
    user_map[user['user_id']]['activity_time'] = defaultdict(lambda: 0)
    user_map[user['user_id']]['messages'] = []
#print("User Map")
#pprint(user_map)

cmd = 'SELECT * FROM channels'
values = cur.execute(cmd).fetchall()

channel_map = {}
for channel in values:
    channel_map[channel['channel_id']] = channel['name']
#print("Channel Map")
#print(channel_map)

cmd = 'SELECT author_id, channel_id, clean_content, created_at FROM messages'
messages = cur.execute(cmd).fetchall()
# {'channel_id': u'333833549213990924', 'author_id': u'205161476204396544', 'clean_content': u'becuase i am an idiot', 'created_at': 1499684205.367}
#print(len(messages))

global_total_messages = 0

for idx,message in enumerate(messages):
    if message['author_id'] not in user_map:
        continue
    global_total_messages += 1
    hour = datetime.datetime.fromtimestamp(message['created_at']).hour
    user_map[message['author_id']]['activity_time'][hour] += 1
    user_map[message['author_id']]['stats']['total_messages'] += 1
    user_map[message['author_id']]['messages'].append(message['clean_content'])

for author_id in user_map:
    print()
    print(user_map[author_id]['name'])

    total_messages = user_map[author_id]['stats']['total_messages']
    print(f"Server Total Messages:{total_messages} ({total_messages/global_total_messages:%})")

    print("Hours  Total   %")
    for hour in sorted(user_map[author_id]['activity_time']):
        hour_total = user_map[author_id]['activity_time'][hour]
        print(f"{(hour-5)%24:5d}: {hour_total:5,d} ({hour_total/total_messages:5.1%})")


for author_id in user_map:
    text = " ".join(user_map[author_id]['messages'])

    wordcloud = WordCloud().generate(text)

    # Display the generated image:
    # the matplotlib way:
    plt.figure()
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
plt.show()

