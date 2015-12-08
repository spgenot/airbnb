__author__ = 'spgenot'
import unittest
import evaluation


class TestEvaluation(unittest.TestCase):

    def test_fr(self):
        country_destination = 'FR'
        predictions = ['FR', 'US']
        self.assertEqual(evaluation.ndcg(country_destination, predictions), 1, 'A correct prediction does not give 1')

    def test_position_2(self):
        country_destination = 'FR'
        predictions = ['US', 'FR']
        res = evaluation.ndcg(country_destination, predictions)
        print res
        self.assertAlmostEqual(res, 0.6309, msg= 'A second best predicitondoes not give 0.6309',
                               delta=0.0001)
