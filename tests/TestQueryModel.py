import unittest
from nflapidb.Entity import Entity, Column
from nflapidb.QueryModel import QueryModel, Operator

class TestQueryModel(unittest.TestCase):
    def test_select_no_constraint(self):
        qmodel = QueryModel()
        self.assertEqual(qmodel.select(), {"_id": False})

    def test_select_no_constraint_with_id(self):
        qmodel = QueryModel()
        self.assertEqual(qmodel.select(withId=True), {})

    def test_select_one_include(self):
        qmodel = QueryModel()
        self.assertEqual(qmodel.select(column1=True), {"_id": False, "column1": True})

    def test_select_one_exclude(self):
        qmodel = QueryModel()
        self.assertEqual(qmodel.select(column1=False), {"_id": False, "column1": False})

    def test_constraint_one_constraint_eq(self):
        qmodel = QueryModel()
        qmodel.cstart("column1", "hello")
        self.assertEqual(qmodel.constraint, {"column1": {"$eq": "hello"}})

    def test_constraint_one_constraint_in(self):
        qmodel = QueryModel()
        qmodel.cstart("column1", ["hello", "world"], Operator.IN)
        self.assertEqual(qmodel.constraint, {"column1": {"$in": ["hello", "world"]}})

    def test_constraint_one_constraint_negate_eq(self):
        qmodel = QueryModel()
        qmodel.cstart("column1", "hello", Operator.negate(Operator.EQ))
        self.assertEqual(qmodel.constraint, {"column1": {"$not": {"$eq": "hello"}}})

    def test_constraint_one_constraint_negate_in(self):
        qmodel = QueryModel()
        qmodel.cstart("column1", ["hello", "world"], Operator.negate(Operator.IN))
        self.assertEqual(qmodel.constraint, {"column1": {"$not": {"$in": ["hello", "world"]}}})

    def test_constraint_two_constraint_and(self):
        qmodel = QueryModel()
        qmodel.cstart("column1", "hello").cand("column2", 1)
        self.assertEqual(qmodel.constraint,
                         {"$and": [{"column1": {"$eq": "hello"}},
                                   {"column2": {"$eq": 1}} ] })

    def test_constraint_two_constraint_or(self):
        qmodel = QueryModel()
        qmodel.cstart("column1", "hello").cor("column2", 1)
        self.assertEqual(qmodel.constraint,
                         {"$or": [{"column1": {"$eq": "hello"}},
                                  {"column2": {"$eq": 1}} ] })

    def test_constraint_two_constraint_nor(self):
        qmodel = QueryModel()
        qmodel.cstart("column1", "hello").cnor("column2", 1)
        self.assertEqual(qmodel.constraint,
                         {"$nor": [{"column1": {"$eq": "hello"}},
                                   {"column2": {"$eq": 1}} ] })

    def test_constraint_three_constraint_and(self):
        qmodel = QueryModel()
        qmodel.cstart("column1", "hello").cand("column2", 1).cand("column3", 1.0)
        self.assertEqual(qmodel.constraint,
                         {"$and": [{"column1": {"$eq": "hello"}},
                                   {"column2": {"$eq": 1}},
                                   {"column3": {"$eq": 1.0}} ] })

    def test_constraint_three_constraint_and_or(self):
        qmodel = QueryModel()
        qmodel.cstart("column1", "hello").cand("column2", 1).cor("column3", 1.0)
        self.assertEqual(qmodel.constraint,
                         {"$or": [{"$and": [{"column1": {"$eq": "hello"}},
                                            {"column2": {"$eq": 1}} ] },
                                  {"column3": {"$eq": 1.0}} ] })

    def test_constraint_three_constraint_or_and(self):
        qmodel = QueryModel()
        qmodel.cstart("column1", "hello").cor("column2", 1).cand("column3", 1.0)
        self.assertEqual(qmodel.constraint,
                         {"$and": [{"$or": [{"column1": {"$eq": "hello"}},
                                            {"column2": {"$eq": 1}} ] },
                                   {"column3": {"$eq": 1.0}} ] })
