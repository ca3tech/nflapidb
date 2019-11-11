from nflapidb.Entity import Entity, PrimaryKey

class ut_table1(Entity):
    @PrimaryKey
    def column1(self):
        return "str"
    @PrimaryKey
    def column2(self):
        return "str"
