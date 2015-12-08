__author__ = 'spgenot'
import networkx as nx
from pymongo import MongoClient
import matplotlib.pyplot as plt
from math import isnan


test_user_id = 'ailzdefy6o'
client = MongoClient()
airbnb_db = client.airbnb
sessions = airbnb_db.sessions
train_users = airbnb_db.train_users
G = nx.DiGraph()
previous_node = 'start'
i = 0
countries = ['US', 'FR', 'CA', 'GB', 'ES', 'IT', 'PT', 'NL','DE', 'AU', 'NDF', 'other']


#First we need to add all the nodes.
#How do we select the order?
#First step: add them randomly.
#Second step: Add them by selecting the most 1st step + most 2nd etc...
#Check how this can be done smartly.


"""
Adding the nodes in order they are fetched
"""

pipeline_action = [
{'$group':
                    {'_id': '$action_detail',
                     'count': {'$sum': 1}
                    }
                }]

G.add_node('start', {'action': 'start', 'Order': i})
for user in sessions.aggregate(pipeline_action):
    i += 1
    action = user['_id']
    action_type = sessions.find({'action_detail': action}).limit(1).next()['action_type']
    if type(action) == float:
        if isnan(action):
            action = 'nan'
    G.add_node(action, {'action_type':action_type,'action_detail': action, 'Order': i})

"""
 Adding the countries as end nodes
"""
for x in countries:
    G.add_node(x, {'action_type': 'final_destination', 'Order': 150})

"""
Adding the edges
Now add all users
"""

pipeline = [
{'$group':
                    {'_id': '$user_id',
                     'count': {'$sum': 1}
                    }
                }]

for user in sessions.aggregate(pipeline):
    test_user_id = user['_id']
    previous_node = 'start'
    for s in sessions.find({'user_id': test_user_id}):
        next_node = s['action_detail']
        if type(next_node) == float:
            if isnan(next_node):
                next_node = 'nan'
        if G.has_edge(previous_node, next_node):
            G[previous_node][next_node]['weight'] += 1
        else:
            G.add_edge(previous_node, next_node, weight=1)
        previous_node = next_node
    #Here add the destination node.
    destination = train_users.find({'id':test_user_id}).limit(1)
    if destination.count() > 0:
        destination = destination.next()['country_destination']
        if G.has_edge(previous_node, destination):
                G[previous_node][destination]['weight'] += 1
        else:
            G.add_edge(previous_node, destination, weight=1)






print len(G)

nx.write_gexf(G, 'test_graph.gexf')


