from stage import Stage

class Task:
    def __init__(self, job_id, stage_id, id, index, runtime, timeout, priority=0):
        self.runtime = runtime #runtime when the input data is read from memory
        self.stage_id = stage_id
        self.job_id = job_id
        self.index = index
        self.id = id   # user_0_1  1: index in the entire application
        self.user_id = 1

        self.priority = priority
        self.machine_id = 0
        self.first_attempt_time = 0
        self.timeout = timeout
        self.stage = None

        self.start_time = 0
        self.finish_time = 0
