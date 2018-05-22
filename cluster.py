from job import Job
from task import Task
#from block_manager import BlockManager
from machine import Machine

DEBUG = False

class Cluster:

    def __init__(self, machines):
        self.machine_number = len(machines)
        self.machines = machines
        self.running_jobs = list()
        self.finished_jobs = list()
        self.is_vacant = True
        self.vacant_machine_list = set(range(self.machine_number))

        self.alpha = 0.8
        self.isDebug = False
        self.totalJobNumber = 0

    def make_offers(self):
        # return the available resources to the framework scheduler
        return self.vacant_machine_list

    def assign_task(self, machineId, task, time):
        task.stage.not_submitted_tasks.remove(task)
        task.machine_id = machineId
        self.machines[machineId].assign_task(task)
        self.vacant_machine_list.remove(machineId)
        task.stage.job.alloc += 1
        if task.stage.job.alloc == 1:
            task.stage.job.start_execution_time = time
#        print "job ", task.stage.job.id, "alloc increase to", task.stage.job.alloc
        self.check_if_vacant()

    def search_job_by_id(self,job_id): # job_id contains user id
        for job in self.running_jobs:
            if job.id == job_id:
                return job
        return False

    def search_machine_by_id(self, id):
        for machine in self.machines:
            if machine.id == id:
                return machine

    def check_if_vacant(self):
        return True

    def release_task(self, task):
        running_machine_id = task.machine_id
        running_machine = self.machines[running_machine_id]
        for core in running_machine.cores:
            if core.running_task == task:
                core.running_task = None
                core.is_running = False
        running_machine.is_vacant = True
        self.vacant_machine_list.add(running_machine_id)
        self.is_vacant = True

    def reset(self):
        self.running_job = -1  # jobs will be executed one by one
        self.is_vacant = True
#        self.task_map = dict()  # map: (task_id, machine_number)
        for machine in self.machines:
            machine.reset()

    def calculate_fairAlloc(self):
        print "enter calculate_fair"
        # to be completed: calculate the targetAlloc of all jobs in the running_jobs list
        jobList = [job for job in self.running_jobs]
        totalResources = float(self.machine_number)
        totalWeight = sum([j.weight for j in jobList])
        jobList.sort(key=lambda i: i.nDemand)
        for i in range(len(jobList)):
            jobList[i].fairAlloc = 0.0
        for i in range(len(jobList)):
            if totalResources <= 0:
                break
            if jobList[i].nDemand * totalWeight < totalResources:
                totalResources -= jobList[i].nDemand * totalWeight
                unitAlloc = jobList[i].nDemand
                for j in range(i, len(jobList)):
                    jobList[j].fairAlloc += unitAlloc * jobList[j].weight
                    jobList[j].nDemand -= unitAlloc
            else:
                unitAlloc = totalResources / totalWeight
                for j in range(i, len(jobList)):
                    jobList[j].fairAlloc += unitAlloc * jobList[j].weight
                    jobList[j].nDemand -= unitAlloc
                totalResources = 0.0
#            result = [[int(j.id.split("_")[-1]), j.fairAlloc] for j in jobList]
        for i in range(len(jobList)):
            jobList[i].nDemand = jobList[i].demand/ jobList[i].weight
            jobList[i].targetAlloc = jobList[i].fairAlloc
            print "result: ", i, jobList[i].id, jobList[i].targetAlloc
            jobList[i].update_slope()

    def calculate_targetAlloc(self):
        jobList = [job for job in self.running_jobs]
        if len(jobList) != self.totalJobNumber - 1:
            return
        self.calculate_fairAlloc()
        return

