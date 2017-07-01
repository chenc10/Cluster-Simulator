from core import Core


class Machine:

    def __init__(self, id, core_number):
        self.id = id
        self.core_number = core_number
        self.cores = [Core() for i in range(1, core_number+1)]
        self.is_vacant = True
        self.is_reserved = -1
        self.reserve_job = None
        #self.cache_policy = policy

    def assign_task(self,task):
        for core in self.cores:
            if core.is_running == False:
                core.running_task = task
                core.is_running = True
                self.check_if_vacant()
                return

    def check_if_vacant(self):
        self.is_vacant = False
        for core in self.cores:
            if core.is_running == False:
                self.is_vacant = True
                return

    def reset(self):
        self.is_vacant = True
        for core in self.cores:
            core.running_task = -1
            core.is_running = False