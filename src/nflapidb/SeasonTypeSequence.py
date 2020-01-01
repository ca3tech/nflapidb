from typing import List

class SeasonTypeSequence:
    def __init__(self):
        self._season_types = ["preseason", "regular_season", "postseason"]
        self._cur_st_idx = 0

    @property
    def all(self) -> List[str]:
        return self._season_types.copy()

    @property
    def min(self) -> str:
        self._season_types[0]

    @property
    def current(self) -> str:
        self._season_types[self._cur_st_idx]

    @property
    def next(self) -> str:
        n = None
        i = self._cur_st_idx + 1
        if i < len(self._season_types):
            n = self._season_types[i]
            self._cur_st_idx = i
        return n

    def reset(self):
        self._cur_st_idx = 0