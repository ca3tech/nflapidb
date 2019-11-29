from nflapidb.Entity import Entity, PrimaryKey, Column, Index

class roster(Entity):
    @PrimaryKey
    def profile_id(self):
        return "int"

    @Index
    def last_name(self):
        return "str"

    @Index
    def first_name(self):
        return "str"

    @Index
    def team(self):
        return "str"

    @Index
    def position(self):
        return "str"

    @Column
    def number(self):
        return "int"

    @Column
    def weight(self):
        return "int"

    @Column
    def birthdate(self):
        return "datetime"

    @Column
    def exp(self):
        return "int"

    @Index
    def previous_teams(self):
        return "list"