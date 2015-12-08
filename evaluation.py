__author__ = 'spgenot'
from math import log, pow


"""
Builds the evaluation module
"""


def ndcg(country_destination, predictions):
    """
    Computes the ndcg score for a destination and a prediction.
    :param country_destination: true destination. Format (int/str) has to be the same as predictions
    :param predictions: array of ordered predictions. Max 5.
    :return: ndcg score for the prediction
    """
    if country_destination in predictions:
        position = predictions.index(country_destination) + 1
    else:
        return 0
    return 1/log(position+1, 2)


def average_ndcg(destination, predictions):
    """
    Computes the average ndcg score for a test set.
    :param destination: array containing the true destinations. Format (int/str) has to be the same as predictions
    :param predictions: array containing the prediction arrays (of size max 5)
    :return: average ndcg score on test set
    """
    assert len(destination) != len(predictions), 'Incorrect input size: the number of test' \
                                                 ' cases does not match the number of predictions'
    average = 0
    number_of_predictions = len(destination)
    for i in range(0,len(destination)):
        average += ndcg(destination[i], predictions[i])/float(number_of_predictions)
    return average






