from typing import List, Tuple, Coroutine
import re
import asyncio
import datetime

def ddquery(q : List[dict], data : List[dict]) -> Tuple[List[dict]]:
    """Find items in data matching items in q
    
    Parameters
    ----------
    q : list of dict
        A list of dictionaries to look for in data
    data : list of dict
        A list of dictionaries
    
    Returns
    -------
    tuple of list of dict
        A 2 item tuple with the first item being the items in data
        that match items in q and the second item being the items
        in data that do not match any item in q
    """
    m = []
    nm = []
    if len(data) > 0 and len(q) > 0:
        for item in data:
            if not isinstance(item, dict):
                raise Exception("ParameterTypeException: data is not list of dict")
            dk = set(item.keys())
            mi = False
            for qitem in q:
                if not isinstance(qitem, dict):
                    raise Exception("ParameterTypeException: q is not list of dict")
                qk = set(qitem.keys())
                if qk.issubset(dk):
                    mi = all([item[k] == qitem[k] for k in qk])
                    if mi:
                        m.append(item)
                        break
            if not mi:
                nm.append(item)
    return (m, nm,)

def str2bool(v : str) -> bool:
    return not (v is None or v == "" or re.search(r"^(f(alse)*|no*)$", v, flags=re.IGNORECASE) is not None)

def runCoroutine(coro : Coroutine) -> any:
    return asyncio.get_event_loop().run_until_complete(coro)

def getSeason(dt : datetime.datetime = datetime.datetime.today()) -> int:
    season = dt.year
    if dt.month < 8:
        season -= 1
    return season

def todict(recs : List[dict], keys : List[str]) -> dict:
    """Create an organized dict from a list of dicts
    
    This will create a dict where the keys at each
    level corresponds to the values of the recs
    for the key at that level. For instance if recs is:
      [{"key1": "value1.1", "key2", "value2.1", "key3": 1},
       {"key1": "value1.1", "key2", "value2.1", "key3": 2},
       {"key1": "value1.1", "key2", "value2.2", "key3": 1},
       {"key1": "value1.1", "key2", "value2.2", "key3": 2}]
    and keys is ["key1", "key2"] then the result is:
      {
        "value1.1": {
          "value2.1": [
            {"key1": "value1.1", "key2", "value2.1", "key3": 1},
            {"key1": "value1.1", "key2", "value2.1", "key3": 2}
          ],
          "value2.2": [
            {"key1": "value1.1", "key2", "value2.2", "key3": 1},
            {"key1": "value1.1", "key2", "value2.2", "key3": 2}
          ]
        }
      }

    Parameters
    ----------
    recs : list of dict
        The records to organize based on keys
    keys : list of str
        The keys to organize records by

    Returns
    -------
    dict
    """
    key = keys[0]
    d = {}
    for r in recs:
        v = None
        if key in r:
            v = r[key]
        if v not in d:
            d[v] = []
        d[v].append(r)
    if len(keys) > 1:
        ckeys = keys[1:len(keys)]
        for k in d:
            cd = todict(d[k], ckeys)
            d[k] = cd
    return d

def getleafs(d : dict, path : [str] = []) -> dict:
    """Get leaf nodes from a dict returned by todict

    This will flatten out the dict returned by the todict
    function. The dict that is returned will have keys
    that are the / separated keys and the values being
    the leaf lists. For instance if d is:
      {
        "value1.1": {
          "value2.1": [
            {"key1": "value1.1", "key2", "value2.1", "key3": 1},
            {"key1": "value1.1", "key2", "value2.1", "key3": 2}
          ],
          "value2.2": [
            {"key1": "value1.1", "key2", "value2.2", "key3": 1},
            {"key1": "value1.1", "key2", "value2.2", "key3": 2}
          ]
        }
      }
    then the returned dict is:
      {
          "value1.1/value2.1": [
            {"key1": "value1.1", "key2", "value2.1", "key3": 1},
            {"key1": "value1.1", "key2", "value2.1", "key3": 2}
          ],
          "value1.1/value2.2": [
            {"key1": "value1.1", "key2", "value2.2", "key3": 1},
            {"key1": "value1.1", "key2", "value2.2", "key3": 2}
          ]
      }

    Parameters
    ----------
    d : dict
        The dictionary to flatten
    path : list of str
        The predecessor path of d

    Returns
    -------
    dict
    """
    keys = list(d)
    if isinstance(d[keys[0]], list):
        # this is a leaf node so create/return
        # a dict with paths as keys and the
        # corresponding list of dicts as values
        rd = {}
        for k in keys:
            p = path + [str(k)]
            rd["/".join(p)] = d[k]
    else:
        # this is not a leaf node so create/return
        # a dict built from calling myself for each
        # child dict
        rd = {}
        for k in keys:
            p = path + [str(k)]
            rd.update(getleafs(d[k], p))
    return rd

