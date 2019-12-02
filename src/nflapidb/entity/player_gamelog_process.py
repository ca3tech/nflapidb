from nflapidb.Entity import Entity, Column

class player_gamelog_process(Entity):
    @Column
    def process_date(self):
        return "datetime"
