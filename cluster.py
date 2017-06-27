from job import Job
from task import Task
#from block_manager import BlockManager
from machine import Machine


class Cluster:

    def __init__(self, machines):
        self.machine_number = len(machines)
        self.machines = machines
        self.running_jobs = list()
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

    def make_offers(self):
        # return the available resources to the framework scheduler
        return self.vacant_machine_list
#        offer_list = []
#        for machine in self.machines:
#            if machine.is_vacant:
#                offer_list.append(machine)
#        return offer_list

    def clear_reservation(self, job_id):
        for machineid in self.jobIdToReservedMachineId[job_id]:
            self.open_machine_number += 1
#            print "machineid:",machineid
            self.machines[machineid].is_reserved = -1
            self.jobIdToReservedNumber[job_id] -= 1

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

    def assign_task(self, machineId, task):
        if self.machines[machineId].is_reserved > -1:
            if task.job_id <> self.machines[machineId].is_reserved:
                print "Error! error! task.jobid:", task.job_id, task.stage_id, "machine reserved for:", self.machines[machineId].is_reserved
        self.machines[machineId].assign_task(task)
        if self.machines[machineId].is_vacant == False:
            self.vacant_machine_list.remove(machineId)
        if self.machines[machineId].is_reserved == -1:
            self.open_machine_number -= 1
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



