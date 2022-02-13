from gamelog_process.award_class import AwardClass

"""
relevant json element
{
    match_id: "1630953060",
    round_id: "1",
    unixtime: "1630953652",
    group: "player",
    label: "kill",
    agent: "68deaefc0a07be79fcb2cc5104a71b1e",
    other: "5379320f3c64f43cdaf3350fc13011ce",
    weapon: "MP-40",
    other_health: 125,
    agent_pos: "589.451965,-70.798233,69.000000",
    agent_angle: "79.161987",
    other_pos: "2791.199219,4.931318,188.575974",
    other_angle: "-88.071899",
    allies_alive: "5",
    axis_alive: "1"
}
"""


class ViewAngles(AwardClass):
    """Calculate the biggest kill streak ove y kills  in x seconds."""

    award_name = "ViewAngles"

    def __init__(self):
        super().__init__(self.award_name)
        self.kill_angle_values = {"kills":{},"deaths":{}}
        self.debug = True


    def get_custom_results(self):
        """Summarize everyone's average view angles and determine:
            1. Person who kills others while they arent looking
            2. Person who gets killed while not looking."""
            
        result = {}
        means = {}
        for category, players in self.kill_angle_values.items():  # kills or deaths
            means[category] = {}
            for player, angles in players.items():
                if "8e6a51baf1c7e338a118d9e32472954e" in player or "58e419de5a8b2655f6d48eab68275db5" in player:
                    # print("sharedguid")
                    continue
                lst = self.kill_angle_values[category][player]
                if len(lst) > 9:  # make this statistically significant
                    means[category][player] = int(abs(sum(lst) / len(lst) - 180))
       
        self.players_values = means["kills"]
        backstabbers = self.get_all_top_results()[self.award_name]
        self.players_values = means["deaths"]
        chickens = self.get_all_top_results()[self.award_name]
        
        result = {"Backstabber":  backstabbers, "Chicken": chickens}
             
        return result


    def process_event(self, rtcw_event):
        """Take incoming kill with SMG or Pistol and see if it's a longest one for the killer."""
        try:
            if rtcw_event.get("label", None) == "kill" and rtcw_event.get("weapon", None) in ["MP-40", "Thompson", "Sten", "Colt", "Luger"]:
                killer = rtcw_event.get("agent", "no guid")
                victim = rtcw_event.get("other", "no guid")
                
                attacker_angle, victim_angle = self.get_angles(rtcw_event)
                angle_diff = abs(attacker_angle - victim_angle)
                
                if killer in self.kill_angle_values["kills"]:
                    self.kill_angle_values["kills"][killer].append(angle_diff)
                else:
                    self.kill_angle_values["kills"][killer] = [angle_diff]
                    
                if victim in self.kill_angle_values["deaths"]:
                    self.kill_angle_values["deaths"][victim].append(angle_diff)
                else:
                    self.kill_angle_values["deaths"][victim] = [angle_diff]
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            error_msg = template.format(type(ex).__name__, ex.args)
            if self.debug:
                print("error at " + self.__name__ + " process event\n")
                print(error_msg)
                
                
    def get_angles(self, rtcw_event):
        """Extract view angles from the json."""
        attacker_angle = 0
        victim_angle = 180
        try:
            attacker_angle = float(rtcw_event["agent_angle"])
            victim_angle = float(rtcw_event["other_angle"])

        except Exception as ex:
            if self.debug:
                print("error at " + rtcw_event.get("unixtime", "no_unixtime") + ": could not split coordinates." + ex.args)
            # exception handling and reporting is not desired here because of data volumes and who receives them
        
        return attacker_angle, victim_angle
