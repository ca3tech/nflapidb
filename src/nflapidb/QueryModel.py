from typing import Any
from nflapidb.Entity import Entity
from nflapidb.EntityManager import EntityManager

class Operator:
    @staticmethod
    def EQ(value : Any) -> dict:
        """Equal"""
        return dict([("$eq", value)])

    @staticmethod
    def NE(value : Any) -> dict:
        """Not Equal"""
        return dict([("$ne", value)])

    @staticmethod
    def IN(value : Any) -> dict:
        """In"""
        return dict([("$in", value)])

    @staticmethod
    def NIN(value : Any) -> dict:
        """Not In"""
        return dict([("$nin", value)])

    @staticmethod
    def GT(value : Any) -> dict:
        """Greater Than"""
        return dict([("$gt", value)])

    @staticmethod
    def GTE(value : Any) -> dict:
        """Greater Than or Equal"""
        return dict([("$gte", value)])

    @staticmethod
    def LT(value : Any) -> dict:
        """Less Than"""
        return dict([("$lt", value)])

    @staticmethod
    def LTE(value : Any) -> dict:
        """Less Than or Equal"""
        return dict([("$lte", value)])

    @staticmethod
    def negate(operator : callable) -> callable:
        return lambda v: dict([("$not", operator(v))])

class QueryModel:

    def __init__(self):
        self._constraint = None

    def select(self, withId : bool = False, **kwargs) -> dict:
        sd = {}
        if not withId:
            sd["_id"] = False
        if len(kwargs):
            sd.update(kwargs)
        return sd

    @property
    def constraint(self) -> dict:
        c = {}
        if self._constraint is not None:
            c = self._constraint
        return c

    @constraint.setter
    def constraint(self, value : dict):
        self._constraint = value

    def cstart(self, name : str, value : Any, operator : callable = Operator.EQ):
        cnst = dict([(name, operator(value))])
        self.constraint = cnst
        return self

    def cand(self, name : str, value : Any, operator : callable = Operator.EQ):
        self._cappend(name, value, operator, "$and")
        return self

    def cor(self, name : str, value : Any, operator : callable = Operator.EQ):
        self._cappend(name, value, operator, "$or")
        return self

    def cnor(self, name : str, value : Any, operator : callable = Operator.EQ):
        self._cappend(name, value, operator, "$nor")
        return self

    def _cappend(self, name : str, value : Any, operator : callable, loperator : str):
        if self._constraint is None:
            self.cstart(name, value, operator)
        else:
            cnst = dict([(name, operator(value))])
            if loperator in self._constraint:
                self._constraint[loperator].append(cnst)
            else:
                self._constraint = dict([(loperator, [self._constraint, cnst])])
