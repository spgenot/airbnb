__author__ = 'spgenot'


from pymongo import MongoClient
from collections import defaultdict
import matplotlib.pyplot as plt
from math import isnan
import datetime
import operator
client = MongoClient()
airbnb_db = client.airbnb
train_users = airbnb_db.train_users
sessions = airbnb_db.sessions

#Analyse the class repartition
#Analyse the proportion of all the different fields

train_users_keys = ['signup_flow', 'signup_method', 'country_destination', 'first_affiliate_tracked',
                    'language', 'affiliate_channel',
                    'first_browser', 'affiliate_provider', 'gender', 'first_device_type',
                    'signup_app', 'age']
train_users_date_keys = ['date_account_created', 'date_first_booking']


"""
#Aggregate the database to return the count of each value accross the DB
#Need to take special care of the dates as they display very badly. Maybe bucket them by month or week?
for key in train_users_keys:
    count_dict = defaultdict(int)
    pipeline = [{'$group':
                    {'_id': '$' + key,
                     'count': {'$sum': 1}
                    }
                }]
    cursor = train_users.aggregate(pipeline)
    for x in cursor:
        count_dict[x['_id']] = x['count']
    xticks = range(0,len(count_dict.keys()))
    plt.bar(xticks, count_dict.values(), align='center')
    plt.title('Aggregation of ' + key)
    plt.xticks(xticks, count_dict.keys(), rotation='vertical')
    plt.show()


#Now we try to find the cross references (for each country how much of what)
countries = ['US', 'FR', 'CA', 'GB', 'ES', 'IT', 'PT', 'NL','DE', 'AU', 'NDF', 'other']
train_users_keys.remove('country_destination')
for k in train_users_keys:
    for c in countries:
        count_dict = defaultdict(int)
        pipeline = [{'$match':{'country_destination':c}},
                    {'$group':
                        {'_id': '$' + k,
                         'count': {'$sum': 1}
                        }
                    }]
        cursor = train_users.aggregate(pipeline)
        for x in cursor:
            count_dict[x['_id']] = x['count']
        xticks = range(0,len(count_dict.keys()))
        plt.bar(xticks, count_dict.values(), align='center')
        plt.title('Aggregation of ' + k + ' for country ' + c)
        plt.xticks(xticks, count_dict.keys(), rotation='vertical')
        plt.show()


#Other questions to answer: dealing with dates
#I think I can make the same kind of plots, but:
#   - deal with the xticks: display only the month name
#   - Try to aggregate this in terms of weekdays as well


for key in train_users_date_keys:
    count_dict = defaultdict(int)
    pipeline = [{'$group':
                    {'_id': '$' + key,
                     'count': {'$sum': 1}
                    }
                }]
    cursor = train_users.aggregate(pipeline)
    start_date = datetime.datetime.strptime('2010-01-01', '%Y-%m-%d')
    stop_date = datetime.datetime.strptime('2015-01-01', '%Y-%m-%d')
    nan_date = datetime.datetime.strptime('2016-02-01', '%Y-%m-%d')

    for x in cursor:
        try:
            d = datetime.datetime.strptime(x['_id'], '%Y-%m-%d')
            if d >= start_date:
                count_dict[d] = x['count']
        except TypeError:
            count_dict[nan_date] = x['count']

    sorted_dict = sorted(count_dict.items(), key=operator.itemgetter(1), reverse=True)
    print sorted_dict
    plt.bar(count_dict.keys(), count_dict.values(), align='center', color='red')
    plt.ylim(0, 1000)
    plt.title('Aggregation of ' + key)
    plt.show()

#Here can we aggregate on certain elements of the dates?
#Directly on the date, on the month, day, etc...
"""

#country_destination by day of the week:
countries = ['US', 'FR', 'CA', 'GB', 'ES', 'IT', 'PT', 'NL','DE', 'AU', 'NDF', 'other']
train_users_keys.remove('country_destination')
for k in train_users_date_keys:
    for c in countries:
        count_dict = defaultdict(int)
        pipeline = [{'$match':{'country_destination':c}},
                    {'$group':
                        {'_id': '$' + k,
                         'count': {'$sum': 1}
                        }
                    }]
        cursor = train_users.aggregate(pipeline)
        for x in cursor:
            try:
                d = datetime.datetime.strptime(x['_id'], '%Y-%m-%d')
                count_dict[d.weekday()]  += x['count']
            except TypeError:
                count_dict[-1] += x['count']
        xticks = range(0,len(count_dict.keys()))
        plt.bar(xticks, count_dict.values(), align='center')
        plt.title('Aggregation of ' + k + ' for country ' + c)
        plt.xticks(xticks, count_dict.keys(), rotation='vertical')
        plt.show()
"""

"""
#Country destination by the month of the year
countries = ['US', 'FR', 'CA', 'GB', 'ES', 'IT', 'PT', 'NL','DE', 'AU', 'NDF', 'other']
#train_users_keys.remove('country_destination')
for k in train_users_date_keys:
    for c in countries:
        count_dict = defaultdict(int)
        pipeline = [{'$match':{'country_destination':c}},
                    {'$group':
                        {'_id': '$' + k,
                         'count': {'$sum': 1}
                        }
                    }]
        cursor = train_users.aggregate(pipeline)
        for x in cursor:
            try:
                d = datetime.datetime.strptime(x['_id'], '%Y-%m-%d')
                count_dict[d.month] += x['count']
            except TypeError:
                count_dict[-1] += x['count']
        #xticks = range(0,len(count_dict.keys()))
        plt.bar(count_dict.keys(), count_dict.values(), align='center')
        plt.title('Aggregation of ' + k + ' for country ' + c)
        #plt.xticks(xticks, count_dict.keys(), rotation='vertical')
        plt.show()
"""

#Now try to cross reference the train_users and sessions dataset
#First, what time is spent in sessions for each country booking?

"""
countries = ['US', 'FR', 'CA', 'GB', 'ES', 'IT', 'PT', 'NL','DE', 'AU', 'NDF', 'other']
average_time_spent_by_country = defaultdict(float)
average_number_of_sessions_by_country = defaultdict(float)
for c in countries:
    average_time_spent = 0
    average_number_of_sessions = 0
    user_cursor = train_users.find({'country_destination': c, 'date_account_created': {'$gt': '2014-01-01'}})
    total_users = float(user_cursor.count())
    for user in user_cursor:
        sessions_cursor = sessions.find({'user_id': user['id']})
        average_number_of_sessions += sessions_cursor.count()/total_users
        for s in sessions_cursor:
            secs_elapsed = s['secs_elapsed']
            if not isnan(secs_elapsed):
                average_time_spent += secs_elapsed/float(total_users)
    average_time_spent_by_country[c] = average_time_spent
    average_number_of_sessions_by_country[c] = average_number_of_sessions

print average_time_spent_by_country
plt.bar(range(0, len(countries)), average_time_spent_by_country.values(), align='center')
plt.title('Average time spent on sessions')
plt.xlabel('Country')
plt.ylabel('Average time on sessions per country')
plt.xticks(range(0, len(countries)), average_time_spent_by_country.keys(), rotation='vertical')
plt.show()

print average_time_spent_by_country
plt.bar(range(0, len(countries)), average_number_of_sessions_by_country.values(), align='center')
plt.title('Average number of sessions per country')
plt.xlabel('Country')
plt.ylabel('Average number of sessions')
plt.xticks(range(0, len(countries)), average_number_of_sessions_by_country.keys(), rotation='vertical')
plt.show()


##We now dwelve deeper into the sessions data.
##We should find how the actions are spread out.

#TODO: To much data on the y axis, how do we transform this to fit all the data?

sessions_keys = ['action', 'action_type', 'action_detail']
for key in sessions_keys:
    count_dict = defaultdict(int)
    pipeline = [{'$group':
                    {'_id': '$' + key,
                     'count': {'$sum': 1}
                    }
                }]
    cursor = sessions.aggregate(pipeline)
    for x in cursor:
        count_dict[x['_id']] = x['count']
    xticks = range(0, len(count_dict.keys()))
    plt.bar(xticks, count_dict.values(), align='center', width=1)
    plt.title('Aggregation of ' + key)
    plt.xticks(xticks, count_dict.keys(), rotation='vertical')
    plt.show()



