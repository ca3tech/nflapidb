from typing import List, Set
import inspect
from functools import wraps

class Entity:
    def __init__(self):
        clazz = self.__class__
        self._clazz = clazz
        self._clazz_attrs = getattr(clazz, "__dict__")
        self._setPrimaryKey()
        self._setColumns()

    @property
    def primaryKey(self) -> Set[str]:
        return set([f.__name__ for f in self._primary_key])

    @property
    def columnNames(self) -> Set[str]:
        return set(self._col_map.keys())

    def columnType(self, cname : str) -> str:
        t = None
        if cname in self._col_map.keys():
            t = self._col_map[cname]
        return t
    
    def _setPrimaryKey(self):
        self._primary_key = set()
        self._initDecoratorSet(self._primary_key, self._isPrimaryKey)
    
    def _setColumns(self):
        self._col_map = {}
        for col in self._initDecoratorSet(set(), self._isColumn):
            self._col_map[col.__name__] = col(self)
        
    def _initDecoratorSet(self, s : set, t : callable) -> set:
        for k in self._clazz_attrs:
            v = self._clazz_attrs[k]
            if inspect.isfunction(v) and t(v):
                s.add(v)
        return s

    def _isPrimaryKey(self, f : callable) -> bool:
        return self._isDecorated(f, "PrimaryKey")

    def _isColumn(self, f : callable) -> bool:
        return self._isDecorated(f, "Column") or self._isDecorated(f, "PrimaryKey")

    def _decorators(self, f : callable) -> Set[str]:
        d = set()
        if hasattr(f, "decorators"):
            d = set(getattr(f, "decorators").split(","))
        return d

    def _isDecorated(self, f : callable, name : str) -> bool:
        return name in self._decorators(f)

def PrimaryKey(attr : callable):
    return __decorate__(attr, "PrimaryKey")

def Column(attr : callable):
    return __decorate__(attr, "Column")

def __addDecorator__(f : callable, dstr : str):
    if hasattr(f, "decorators"):
        dstr = ",".join([getattr(f, "decorators"), dstr])
    setattr(f, "decorators", dstr)
    return f

def __decorate__(attr : callable, name : str) -> callable:
    # If I don't decorate with wraps the __name__ of the
    # returned function will be call rather than the
    # original name
    @wraps(attr)
    def call(*args, **kwargs):
        return attr(*args, **kwargs)
    return __addDecorator__(call, name)