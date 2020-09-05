from typing import List, abstractmethod
import logging
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
        logging.info("Syncing {} data...".format(self._entity_name))
        cur = await self.find()
        if len(cur) > 0:
            gsidqm = QueryModel()
            gsidqm.sinclude(["gsis_id"])
            cgsidd = await self.find(qm=gsidqm)
            cgsids = list(set([r["gsis_id"] for r in cgsidd]))
            schqm = QueryModel()
            schqm.cstart("finished", True)
            if len(cgsids) > 0:
                schqm.cand("gsis_id", cgsids, Operator.NIN)
            sch = await self._scheduleManager.find(qm=schqm)
        else:
            sch = await self._scheduleManager.find()
        return await self.save(self._queryAPI(sch))

    @abstractmethod
    def _queryAPI(self, schedules : List[dict]) -> List[dict]:
        pass

    @property
    def _scheduleManager(self):
        if self._schmgr is None:
            self._schmgr = ScheduleManagerFacade(self._entityManager, self._apiClient)
        return self._schmgr

    def _getQueryModel(self, cmap : dict) -> QueryModel:
        qm = QueryModel()
        for name in cmap:
            if cmap[name] is not None:
                if isinstance(cmap[name], list):
                    qm.cand(name, cmap[name], Operator.IN)
                else:
                    qm.cand(name, cmap[name], Operator.EQ)
        return qm
