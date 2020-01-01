from nflapidb.Entity import Entity, PrimaryKey, Column, Index

class schedule(Entity):
    @PrimaryKey
    def gsis_id(self):
        return "str"

    @Index
    def season(self):
        return "int"

    @Index
    def season_type(self):
        return "str"

    @Index
    def week(self):
        return "int"

    @Index
    def finished(self):
        return "bool"

    @Column
    def home_team_score(self):
        return "int"

    @Column
    def away_team_score(self):
        return "int"

    @Column
    def date(self):
        return "datetime"