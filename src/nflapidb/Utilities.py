from typing import List, Tuple
import re

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
    