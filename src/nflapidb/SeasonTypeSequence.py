
class SeasonTypeSequence:
    def __init__(self):
        self._season_types = ["preseason", "regular_season", "postseason"]
        self._cur_st_idx = 0

    @property
    def min(self) -> str:
        self._season_types[0]

    @property
    def current(self) -> str:
        self._season_types[self._cur_st_idx]

    @current.setter
    def current(self, seasonType : str):
        li = [i for i in range(0, len(self._season_types)) if self._season_types[i] == seasonType]
        if len(li) == 0:
            raise Exception(f"{seasonType} is not a valid season type")
        self._cur_st_idx = li[0]

    @property
    def next(self) -> str:
        i = self._cur_st_idx + 1
        if i == len(self._season_types):
            i = 0
        self._cur_st_idx = i
        return self._cur_st_idx