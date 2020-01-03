from typing import Any, List
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
    def REGEX(value : Any, options : str = None) -> dict:
        """Regular Expression Match"""
        dt = [("$regex", value)]
        if options is not None:
            dt.append(("$options", options))
        return dict(dt)

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
        self._select : dict = {"_id": False}
        self._constraint : dict = None

    def select(self, withId : bool = False, **kwargs) -> dict:
        sd = self._select.copy()
        if "_id" in sd:
            if not withId:
                sd["_id"] = False
            else:
                del sd["_id"]
        if len(kwargs):
            sd.update(kwargs)
        return sd

    def sincludeID(self):
        if "_id" in self._select and not self._select["_id"]:
            del self._select["_id"]

    def sinclude(self, columnNames : List[str]):
        self._select.update(dict(zip(columnNames, [True for _ in columnNames])))

    def sexclude(self, columnNames : List[str]):
        self._select.update(dict(zip(columnNames, [False for _ in columnNames])))

    @property
    def constraint(self) -> dict:
        c = {}
        if self._constraint is not None:
            c = self._constraint
        return c

    @constraint.setter
    def constraint(self, value : dict):
        self._constraint = value

    def cstart(self, name : str, value : Any, operator : callable = Operator.EQ, operator_options : Any = None):
        if operator_options is None:
            cnst = dict([(name, operator(value))])
        else:
            cnst = dict([(name, operator(value, operator_options))])
        self.constraint = cnst
        return self

    def cand(self, name : str = None, value : Any = None, operator : callable = Operator.EQ, operator_options : Any = None, query_model : Any = None):
        # type: (QueryModel, str, Any, callable, Any, QueryModel)
        self._cappend(name, value, operator, operator_options, "$and", query_model)
        return self

    def cor(self, name : str = None, value : Any = None, operator : callable = Operator.EQ, operator_options : Any = None, query_model : Any = None):
        # type: (QueryModel, str, Any, callable, Any, QueryModel)
        self._cappend(name, value, operator, operator_options, "$or", query_model)
        return self

    def cnor(self, name : str = None, value : Any = None, operator : callable = Operator.EQ, operator_options : Any = None, query_model : Any = None):
        # type: (QueryModel, str, Any, callable, Any, QueryModel)
        self._cappend(name, value, operator, operator_options, "$nor", query_model)
        return self

    def _cappend(self, name : str, value : Any, operator : callable, operator_options : Any, loperator : str, query_model : Any):
        # type: (QueryModel, str, Any, callable, Any, str, QueryModel)
        cnst = None
        if query_model is not None:
            if self._constraint is None:
                self._constraint = query_model.constraint
            else:
                cnst = query_model.constraint
        else:
            if operator_options is None:
                cnst = dict([(name, operator(value))])
            else:
                cnst = dict([(name, operator(value, operator_options))])
        if self._constraint is None:
            self.cstart(name, value, operator, operator_options)
        elif cnst is not None:
            if loperator in self._constraint:
                self._constraint[loperator].append(cnst)
            else:
                self._constraint = dict([(loperator, [self._constraint, cnst])])
