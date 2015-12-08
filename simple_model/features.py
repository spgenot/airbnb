__author__ = 'spgenot'
from pymongo import MongoClient



client = MongoClient()
db = client.airbnb
train_users = db.train_users


def load_data():
    """
    Fetches the data from the database and returns it in the form of an array of dict
    :return: array of dict of the data
    """
    res = []
    for user in train_users.find():
        res.append(user)
    return res


def split_data(all_data, ratio):
    """
    Performs a simple ratio split of the data
    :param all_data: full dataset
    :return: [train_set, test_set]
    """
    threshold_int = int(ratio*len(all()))
    train_set = all_data[0:threshold_int]
    test_set = all_data[threshold_int:]
    return [train_set, test_set]



def pre_process_data(dataset):
    """
    Performs simple preprocessing of the data
    :param dataset: either train or test set
    :return: [Matrix of the preprocessed features, target]
    """
    res_set = []
    res_target = []
    for x in dataset:
        #TODO: We need to assign a number to all the instances. Can we do this in place rather than do this offline?
        break


