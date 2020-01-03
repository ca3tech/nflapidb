from typing import List, abstractmethod
import nflapi.Client
from nflapidb.EntityManager import EntityManager
from nflapidb.DataManagerFacade import DataManagerFacade
from nflapidb.ScheduleManagerFacade import ScheduleManagerFacade
from nflapidb.QueryModel import QueryModel, Operator

class ScheduleDependantManagerFacade(DataManagerFacade):

    def __init__(self, entityName : str, entityManager : EntityManager,
                 apiClient : nflapi.Client.Client = None,
                 scheduleManager : ScheduleManagerFacade = None):
        super(ScheduleDependantManagerFacade, self).__init__(entityName, entityManager, apiClient)
        self._schmgr = scheduleManager

    async def sync(self) -> List[dict]:
        gsidqm = QueryModel()
        gsidqm.sinclude(["gsis_id"])
        cgsidd = await self.find(qm=gsidqm)
        cgsids = list(set([r["gsis_id"] for r in cgsidd]))
        schmgr = self._scheduleManager
        schqm = None
        if len(cgsids) > 0:
            schqm = QueryModel()
            schqm.cstart("gsis_id", cgsids, Operator.NIN)
            schqm.cand("finished", True)
        sch = await schmgr.find(qm=schqm)
        return await self.save(self._queryAPI(sch))

    @abstractmethod
    def _queryAPI(self, schedules : List[dict]) -> List[dict]:
        pass

    @property
    def _scheduleManager(self):
        if self._schmgr is None:
            self._schmgr = ScheduleManagerFacade(self._entityManager, self._apiClient)
        return self._schmgr
