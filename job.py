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

        self.alloc = 0.0
        self.weight = 1.0
        self.targetAlloc = 0.0
        self.fairAlloc = 0.0
        self.minAlloc = 0.0
        self.demand = 0.0
        self.nDemand = 0.0
        self.curve = list()
        self.lSlope = 0.0
        self.rSlope = 0.0
        self.lSlopeArray = list()
        self.rSlopeArray = list()

        self.alpha = 0.8

    def search_stage_by_id(self,stage_id):
        for stage in self.stages:
            if stage.id == stage_id:
                return stage
        return False

    def set_curve(self, curveString):
        self.curve = [float(i) for i in curveString.split("-")]
        print self.curve
        print len(self.curve)
        self.demand = float(len(self.curve) - 1)
        # initialize the targetAlloc in case the comparison error
        self.targetAlloc = self.demand
        self.nDemand = self.demand / self.weight
        self.lSlopeArray.append(100.0)
        for i in range(1, len(self.curve)):
            self.lSlopeArray.append(self.curve[i]-self.curve[i-1])
        self.rSlopeArray.append(0.0)
        for i in range(0, len(self.curve) - 1):
            self.rSlopeArray.append(self.curve[i+1]-self.curve[i])

    def re_initialize(self):
        self.nDemand = self.demand / self.weight
        self.fairAlloc = 0.0

    def update_slope(self):
        self.lSlope = self.lSlopeArray[int(self.targetAlloc)]
        print self.targetAlloc
        print len(self.rSlopeArray)
        self.rSlope = self.rSlopeArray[int(self.targetAlloc)]

    def get_min_alloc(self):
        tmp_i = 1
        while tmp_i <= int(self.fairAlloc) and self.curve[int(self.fairAlloc) - tmp_i] >= self.curve[int(self.fairAlloc)] * self.alpha:
            tmp_i += 1
        self.minAlloc = self.fairAlloc - tmp_i + 1


