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
        self.open_machine_number = len(machines)
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
        self.accelerate_factor = 1.0

        self.isDebug = False

    def make_offers(self):
        # return the available resources to the framework scheduler
        return self.vacant_machine_list
#        offer_list = []
#        for machine in self.machines:
#            if machine.is_vacant:
#                offer_list.append(machine)
#        return offer_list

    def clear_reservation(self, job):
        for machineid in self.jobIdToReservedMachineId[job.id]:
            self.open_machine_number += 1
#            print "machineid:",machineid
            self.machines[machineid].is_reserved = -1
            self.jobIdToReservedNumber[job.id] -= 1
            job.alloc -= 1

    def set_reservation(self, machineid, task, job_id='-1'):
#        print "set reservation", machineid, "task", task.id, task.stage_id,"-", task.is_initial,"offer size:", len(self.make_offers()),"open machine number", self.open_machine_number
#        print " - jobIdto", self.jobIdToReservedNumber[task.job_id]
        if job_id == '-1':
            self.jobIdToReservedNumber[task.job_id] += 1
            self.jobIdToReservedMachineId[task.job_id].add(machineid)
            self.machines[machineid].is_reserved = task.job_id
        else:
            self.jobIdToReservedNumber[job_id] += 1
            self.jobIdToReservedMachineId[job_id].add(machineid)
            self.machines[machineid].is_reserved = job_id

    def assign_task(self, machineId, task, time):
        if self.machines[machineId].is_reserved > -1:
            if task.job_id <> self.machines[machineId].is_reserved:
                print "Error! error! task.jobid:", task.job_id, task.stage_id, "machine reserved for:", self.machines[machineId].is_reserved
        self.machines[machineId].assign_task(task)
        if self.machines[machineId].is_vacant == False:
            self.vacant_machine_list.remove(machineId)
        if self.machines[machineId].is_reserved == -1:
            self.open_machine_number -= 1
            task.stage.job.alloc += 1
            if task.stage.job.alloc == 1:
                task.stage.job.start_execution_time = time
#            print "job ", task.stage.job.id, "alloc increase to", task.stage.job.alloc
        else:
            self.jobIdToReservedNumber[task.job_id] -= 1

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
        if task.stage.job.service_type == self.foreground_type:
            self.set_reservation(running_machine_id, task)
        else:
            if self.pre_reserve_enabled == True and self.currently_reserving == True:
                print "pre_reserve: ", task.id, running_machine_id, self.pre_reserve_job_id
                self.set_reservation(running_machine_id, task, self.pre_reserve_job_id)
                self.currently_pre_reserved_number += 1
                if self.currently_pre_reserved_number == self.pre_reserve_goal:
                    self.currently_reserving = False
                    self.currently_pre_reserved_number = 0
            else:
                self.open_machine_number += 1

    def reset(self):
        self.running_job = -1  # jobs will be executed one by one
        self.is_vacant = True
#        self.task_map = dict()  # map: (task_id, machine_number)
        for machine in self.machines:
            machine.reset()

    def calculate_fairAlloc(self):
#        print "enter calculate"
        # to be completed: calculate the targetAlloc of all jobs in the running_jobs list
        jobList = [job for job in self.running_jobs if job.service_type == self.foreground_type]
        totalResources = float(self.machine_number)
        totalWeight = sum([j.weight for j in jobList])
        jobList.sort(key=lambda i: i.nDemand)
        for i in range(0, len(jobList)):
            jobList[i].fairAlloc = 0.0
        for i in range(0, len(jobList)):
#            print "enter fair:", i, "jobList[i].nDemand:", jobList[i].nDemand, jobList[i].weight, totalResources
            if totalResources <= 0:
                break
            if jobList[i].nDemand * totalWeight < totalResources:
#                print "totalWeight:", totalWeight
                totalResources -= jobList[i].nDemand * totalWeight
                for j in range(i, len(jobList)):
                    jobList[j].fairAlloc += jobList[i].nDemand * jobList[j].weight
                    jobList[j].nDemand -= jobList[i].nDemand
            else:
                for j in range(i, len(jobList)):
                    jobList[j].fairAlloc += totalResources / totalWeight * jobList[j].weight
                    jobList[j].nDemand -= totalResources / totalWeight
                totalResources = 0.0
        for i in range(0, len(jobList)):
            jobList[i].nDemand = jobList[i].demand/ jobList[i].weight
            jobList[i].targetAlloc = jobList[i].fairAlloc
#            print "result: ", i, jobList[i].id, jobList[i].targetAlloc
            jobList[i].update_slope()

    def calculate_targetAlloc(self):
        jobList = [job for job in self.running_jobs if job.service_type == self.foreground_type]
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
