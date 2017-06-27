class Job:
    def __init__(self, id):
        self.id = id
        self.index = 1
        self.user_id = 1
        self.stages = list()
        self.completed_stage_ids = list() #for use of simulation
        self.not_completed_stage_ids= list() #for use of simulation
        self.submitted_stage_ids = list()

        self.stagesDict = dict()
        self.submit_time = 0
        self.completion_time = 0
        self.duration = 0
        self.priority = 0
        self.service_type = 2

        self.targetAlloc = 0
        self.allocated = 0
        self.weight = 1.0

    def search_stage_by_id(self,stage_id):
        for stage in self.stages:
            if stage.id == stage_id:
                return stage
        return False


