class Achievements:
    """Calculate the longest kill in the game."""

    def __init__(self, stats):
        self.potential_achievements = {}
        
        self.get_killpeak_lines(stats)
        self.get_combat_medic_lines(stats)


    def get_killpeak_lines(self, stats):
        """Return result as {"Award name": {"guid":"value"}}."""
        """Simply filter down killpeaks to 6"""
        temp_achievements = {}
        for guid, player_stat in stats.items():
            if int(player_stat.get("categories", {}).get("killpeak",0)) > 2:
                temp_achievements[guid] = int(player_stat.get("categories", {}).get("killpeak",0))            
        self.potential_achievements["Killpeak"] = temp_achievements
        
    def get_combat_medic_lines(self, stats):
        """Return result as {"Award name": {"guid":"value"}}."""
        """Determine combat medic achievement.
        
        This achievement requires a player to get a certain number of kills, gibs, revives.
        21/7/7 for gold (level 7 and above)
        18/6/6 for silver (level 6)
        15/5/5 for bronze (level 5)
        12/4/4 for rusted iron (level 4)
        """
        temp_achievements = {}
        for guid, player_stat in stats.items():
            kills = int(player_stat.get("categories", {}).get("kills",0))
            gibs = int(player_stat.get("categories", {}).get("gibs",0))
            revives = int(player_stat.get("categories", {}).get("revives",0))
            if  kills >= 12 and gibs >=4 and revives>=4: # to skip some cycles
                if  kills >= 30 and gibs >=10 and revives>=10:
                    temp_achievements[guid] = 10
                    continue
                if  kills >= 27 and gibs >=9 and revives>=9:
                    temp_achievements[guid] = 9
                    continue
                if  kills >= 24 and gibs >=8 and revives>=8:
                    temp_achievements[guid] = 8
                    continue
                if  kills >= 21 and gibs >=7 and revives>=7:
                    temp_achievements[guid] = 7
                    continue
                if  kills >= 18 and gibs >=6 and revives>=6:
                    temp_achievements[guid] = 6
                    continue
                if  kills >= 15 and gibs >=5 and revives>=5:
                    temp_achievements[guid] = 5
                    continue
                if  kills >= 12 and gibs >=4 and revives>=4:
                    temp_achievements[guid] = 4
                    continue
                #pretty much i dont feel like doing math in a loop today
        self.potential_achievements["Combat Medic"] = temp_achievements
    
# debug
# self = Achievements(stats)
# self.potential_achievements
       
    



