__author__ = 'spgenot'
from pymongo import MongoClient
import pandas as pd

client = MongoClient()
db = client.airbnb
train_users = db.train_users
test_users = db.test_users
sessions = db.sessions
age_gender_bkts = db.age_gender_bkts
countries = db.countries

train_users_input = 'train_users.csv'
test_users_input = 'test_users.csv'
sessions_input = 'sessions.csv'
age_gender_bkts_input = 'age_gender_bkts.csv'
countries_input = 'countries.csv'




class DataInserter:

    def __init__(self):
        self.load_train()
        self.load_test()
        self.load_countries()
        self.load_age_gender_bkts()
        self.load_sessions()


    def load_train(self):
        train_df = pd.read_csv(train_users_input)
        print train_df.head()
        for index, rows in train_df.iterrows():
            doc = {'id': rows['id'],
                   'date_account_created': rows['date_account_created'],
                   'timestamp_first_active': rows['timestamp_first_active'],
                   'date_first_booking': rows['date_first_booking'],
                   'gender': rows['gender'],
                   'age': rows['age'],
                   'signup_method': rows['signup_method'],
                   'signup_flow': rows['signup_flow'],
                   'language': rows['language'],
                   'affiliate_channel': rows['affiliate_channel'],
                   'affiliate_provider': rows['affiliate_provider'],
                   'first_affiliate_tracked': rows['first_affiliate_tracked'],
                   'signup_app': rows['signup_app'],
                   'first_device_type': rows['first_device_type'],
                   'first_browser': rows['first_browser'],
                   'country_destination': rows['country_destination']
            }
            train_users.update({'id':doc['id']}, doc, upsert=True)


    def load_test(self):
        test_df = pd.read_csv(test_users_input)
        print test_df.head()
        for index, rows in test_df.iterrows():
            doc = {'id': rows['id'],
                   'date_account_created': rows['date_account_created'],
                   'timestamp_first_active': rows['timestamp_first_active'],
                   'date_first_booking': rows['date_first_booking'],
                   'gender': rows['gender'],
                   'age': rows['age'],
                   'signup_method': rows['signup_method'],
                   'signup_flow': rows['signup_flow'],
                   'language': rows['language'],
                   'affiliate_channel': rows['affiliate_channel'],
                   'affiliate_provider': rows['affiliate_provider'],
                   'first_affiliate_tracked': rows['first_affiliate_tracked'],
                   'signup_app': rows['signup_app'],
                   'first_device_type': rows['first_device_type'],
                   'first_browser': rows['first_browser']
            }
            test_users.update({'id':doc['id']}, doc, upsert=True)

    def load_countries(self):
        countries_df = pd.read_csv(countries_input)
        print countries_df.head()
        print list(countries_df.columns.values)
        for index, rows in countries_df.iterrows():
            doc = {'country_destination': rows['country_destination'],
                    'lat_destination': rows['lat_destination'],
                    'lng_destination': rows['lng_destination'],
                    'distance_km': rows['distance_km'],
                    'destination_km2': rows['destination_km2'],
                    'destination_language': rows['destination_language '],
                    'language_levenshtein_distance': rows['language_levenshtein_distance']
                    }
            countries.update({'country_destination': doc['country_destination']}, doc, upsert=True)

    def load_age_gender_bkts(self):
        age_gender_bkts_df = pd.read_csv(age_gender_bkts_input)
        print age_gender_bkts_df.head()
        for index, rows in age_gender_bkts_df.iterrows():
            doc = {'index': index,
                    'age_bucket': rows['age_bucket'],
                    'country_destination': rows['country_destination'],
                    'gender': rows['gender'],
                    'population_in_thousands': rows['population_in_thousands'],
                    'year': rows['year']
                   }
            age_gender_bkts.update({'index': index}, doc, upsert=True)

    def load_sessions(self):
        sessions_df = pd.read_csv(sessions_input)
        print sessions_df.head()
        for index, rows in sessions_df.iterrows():
            doc = {'index': index,
                   'user_id': rows['user_id'],
                   'action': rows['action'],
                   'action_type': rows['action_type'],
                   'action_detail': rows['action_detail'],
                   'device_type': rows['device_type'],
                   'secs_elapsed': rows['secs_elapsed']
                }
            sessions.update({'index':index}, doc, upsert=True)




di = DataInserter()

