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
#        self.task_map = dict() # map: (task_id, machine_number)
        self.Memory_Disk_Ratio = 3  # ratio of the speed in reading from memory and disk
        self.vacant_machine_list = set(range(self.machine_number))
        self.jobIdToReservedNumber = {}
        self.jobIdToReservedMachineId = {}
        self.foreground_type = 0
        self.straggler_mitigation_enabled = False
        self.pre_reserve_enabled = False
        self.currently_reserving = False
        self.currently_pre_reserved_number = 0
        self.pre_reserve_goal = 0
        self.pre_reserve_job_id = ""

        self.alpha = 0.8
        self.isDebug = False
        self.totalJobNumber = 0

    def make_offers(self):
        # return the available resources to the framework scheduler
        return self.vacant_machine_list

    def assign_task(self, machineId, task, time):
        task.stage.not_submitted_tasks.remove(task)
        task.machine_id = machineId
#        if self.machines[machineId].is_reserved > -1:
#            if task.job_id <> self.machines[machineId].is_reserved:
#                print "Error! error! task.jobid:", task.job_id, task.stage_id, "machine reserved for:", self.machines[machineId].is_reserved
        self.machines[machineId].assign_task(task)
        if self.machines[machineId].is_vacant == False:
            self.vacant_machine_list.remove(machineId)
        if self.machines[machineId].is_reserved == -1:
            task.stage.job.alloc += 1
            if task.stage.job.alloc == 1:
                task.stage.job.start_execution_time = time
#            print "job ", task.stage.job.id, "alloc increase to", task.stage.job.alloc
        else:
            self.jobIdToReservedNumber[task.job_id] -= 1
            task.runtime = task.runtime / task.stage.job.accelerate_factor
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
        self.is_vacant = False
        for machine in self.machines:
            if machine.is_vacant == True:
                self.is_vacant = True
                return

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
        jobList = [job for job in self.running_jobs if job.service_type == self.foreground_type]
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
        jobList = [job for job in self.running_jobs if job.service_type == self.foreground_type]
        if len(jobList) != self.totalJobNumber - 1:
            return
        self.calculate_fairAlloc()
        if len(jobList) == 0:
            return
        if len(jobList) == 1:
            jobList[0].targetAlloc = float(self.machine_number)
        for j in jobList:
            j.get_min_alloc(self.alpha)
        setG = list()
        setH = list()
        setQ = list()
        min_lSJob = min(jobList, key=lambda x: x.lSlope)
        setG.append(min_lSJob)
        jobList.remove(min_lSJob)
        if len(jobList) == 0:
            return
        max_rSJob = max(jobList, key=lambda x: x.rSlope)
        setH.append(max_rSJob)
        jobList.remove(max_rSJob)

        while len(jobList) != 0:
            lMax = min(jobList, key=lambda x: x.lSlope).lSlope
            rMin = max(jobList, key=lambda x: x.rSlope).rSlope
            s = True
            stopCase_G = 0
            stopCase_H = 0
            while s:
                shallDoAjustment = True
                tmpGiverJob = min(setG, key=lambda x: x.lSlope)
                if tmpGiverJob.lSlope > lMax:
                    s = False
                    stopCase_G = 1
                    shallDoAjustment = False
                if tmpGiverJob.targetAlloc <= tmpGiverJob.minAlloc:
                    setG.remove(tmpGiverJob)
                    setQ.append(tmpGiverJob)
                    shallDoAjustment = False
                    if len(setG) == 0:
                        s = False
                        stopCase_G = 1
                tmpGainJob = max(setH, key=lambda x: x.rSlope)
                if tmpGainJob.rSlope < rMin:
                    s = False
                    stopCase_H = 1
                    shallDoAjustment = False
#                print "======check: ", tmpGiverJob.id, tmpGainJob.id, tmpGainJob.targetAlloc, tmpGainJob.demand
                if tmpGainJob.targetAlloc >= tmpGainJob.demand:
                    setH.remove(tmpGainJob)
                    shallDoAjustment = False
                    if len(setH) == 0:
                        s = False
                        stopCase_H = 1
                if shallDoAjustment:
                    tmpGiverJob.targetAlloc -= 1
                    tmpGainJob.targetAlloc += 1
                    tmpGiverJob.update_slope()
                    tmpGainJob.update_slope()
            if stopCase_G == 1:
                min_lSJob = min(jobList, key=lambda x: x.lSlope)
                setG.append(min_lSJob)
                jobList.remove(min_lSJob)
            if stopCase_H == 1 and len(jobList) > 0:
                max_rSJob = max(jobList, key=lambda x: x.rSlope)
                setH.append(max_rSJob)
                jobList.remove(max_rSJob)
        s = True
#        print "---calculate: second phase"
        while s:
            tmpGiverJob = min(setG, key=lambda x: x.lSlope)
            tmpGainJob = max(setH, key=lambda x: x.rSlope)
            shallDoAjustment = True
            if tmpGiverJob.lSlope >= tmpGainJob.rSlope:
                s = False
                shallDoAjustment = False
            if tmpGiverJob.targetAlloc <= tmpGiverJob.minAlloc:
                setG.remove(tmpGiverJob)
                setQ.append(tmpGiverJob)
                shallDoAjustment = False
                if len(setG) == 0:
                    s = False
            if tmpGainJob.targetAlloc >= tmpGainJob.demand:
                setH.remove(tmpGainJob)
                shallDoAjustment = False
                if len(setH) == 0:
                    s = False
            if shallDoAjustment:
                tmpGiverJob.targetAlloc -= 1
                tmpGainJob.targetAlloc += 1
                tmpGiverJob.update_slope()
                tmpGainJob.update_slope()

        jobList = [job for job in self.running_jobs if job.service_type == self.foreground_type]
        print "get targetAlloc: ",
        for j in jobList:
            print "id-" + str(j.id) + "-alloc-" + str(j.targetAlloc),
        print
