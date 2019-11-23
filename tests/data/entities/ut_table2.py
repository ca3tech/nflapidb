from nflapidb.Entity import Entity, Column

class ut_table2(Entity):
    @Column
    def column1(self):
        return "str"
    @Column
    def column2(self):
        return "datetime"