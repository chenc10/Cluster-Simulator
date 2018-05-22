from job import Job

class Stage:
    def __init__(self, job_id, id, parent_ids,tasks):
        self.job_id = job_id
        self.id = id
        self.user_id = 1
        self.taskset = tasks
        self.parent_ids = parent_ids
        self.completed_tasks = list()
        self.produced_rdds = list()
        self.not_completed_tasks = [task for task in self.taskset]
        self.not_submitted_tasks = [task for task in self.taskset]

        self.priority = 0
        self.submit_time = 0
        self.last_task_submit_time = 0
        self.finish_time = 0
        self.monopolize_time = 0
        # unfinished, for locality maintaining.
        self.job = None
        self.has_done_speculation = False

    def handle_stage_submission(self):
        return

    def reset(self):
        self.not_submitted_tasks = self.taskset
        self.not_completed_tasks = self.taskset
        self.completed_tasks = list()

