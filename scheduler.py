##Simulator submits jobs and stages to the scheduler
##Scheduler submits tasks to the cluster


from event import (EventJobSubmit, EventReAlloc, EventJobComplete, EventStageSubmit, EventStageComplete, EventTaskSubmit, EventTaskComplete, Event)
from job import Job
from stage import Stage
from task import Task
import os
import numpy as np


class Scheduler:
    def __init__(self, cluster):
        self.cluster = cluster
        self.task_buffer = list()  #tasks waiting to be scheduled
        self.task_buffer_foreground = list()
        self.task_buffer_background = list()
        self.completed_stage_ids = list()
        self.ready_stages = list() # for record to avoid duplicate submission while checking the ready stages

        self.submitted_stages_number = 0 # for record of dead data generation
        self.submit_jobs_number = 0

        # add by cc
        self.stageIdToUsedMachineId = dict()
        self.stageIdToAllowedMachineId = dict()
        self.stageIdToStage = dict()
        self.pre_reserve_threshold = 0.5

        self.pending_stages = list()
#        self.taskIdToAllowedMachineId = dict()

    def task_buffer_insert(self, taskset):
        # currently do not distinguish service type and priority
        if taskset[0].stage.job.service_type == self.cluster.foreground_type:
            self.task_buffer_foreground += taskset
        else:
            self.task_buffer_background += taskset
        self.task_buffer = self.task_buffer_foreground + self.task_buffer_background

    def task_buffer_delete(self, task):
        if task.stage.job.service_type == self.cluster.foreground_type:
            self.task_buffer_foreground.remove(task)
        else:
            self.task_buffer_background.remove(task)
        self.task_buffer.remove(task)

    def check_waiting(self):
        if len(self.task_buffer) > 0:
            return True
        else:
            return False

    def sort_tasks(self):
        sorted(self.task_buffer, key=lambda x: x.stage.job.alloc / x.stage.job.targetAlloc)
        return

    def do_allocate(self, time):
        self.sort_tasks()
        # chen: this part needs to be modified finely. Move the scheduling part from cluster.py to this function
        # achieve data locality with a heartbeat function; achieve locality with stage.locality_preference.
        # function1: sort the tasks according to the priority
        # function2: record the locality of each stage (which slots the stage initially occupies.)
        msg=list()
        if len(self.cluster.make_offers()) == 0 or len(self.task_buffer) == 0:
            return msg
        if self.cluster.open_machine_number == 0 and self.task_buffer[0].stage.job.service_type <> self.cluster.foreground_type:
            return msg
        for task in self.task_buffer:
#            print " make_offers():", len(self.cluster.make_offers()), "open number:", self.cluster.open_machine_number
            if len(self.cluster.make_offers()) == 0:
                for item in msg:
                    self.task_buffer.remove(item[0])
                return msg
            if self.cluster.open_machine_number == 0 and not self.cluster.jobIdToReservedNumber.has_key(task.job_id):
                for item in msg:
                    self.task_buffer.remove(item[0])
                return msg
            success = False
            sign = False
            t = 0
            for machineId in self.cluster.make_offers():
                t += 1
                if self.cluster.machines[machineId].is_reserved > -1 and task.job_id <> self.cluster.machines[machineId].is_reserved:
                    continue
#                print "enter inner 1", task.id, machineId
                sign = True
                if machineId in self.stageIdToAllowedMachineId[task.stage.id]:
                    task.machine_id = machineId
                    self.cluster.assign_task(machineId, task)
#                    self.cluster.task_map[task.id] = machineId
                    self.cluster.check_if_vacant()
                    success = True
                    msg.append((task, machineId, task.runtime))
                    break
            if success == False and sign == True:
                # if locality requirement is not achieved.
                # first check whether time out
                if task.first_attempt_time > 0:
                    if time - task.first_attempt_time > task.timeout:
                        for machineId in self.cluster.make_offers():
                            if self.cluster.machines[machineId].is_reserved > -1 and task.job_id <> self.cluster.machines[machineId].is_reserved:
                                continue
                            task.machine_id = machineId
                            if task.timeout == 100:
                                task.runtime = task.runtime * 1
#                                task.runtime = task.runtime * 1.2
                            else:
                                task.runtime = task.runtime * 1
#                                task.runtime = task.runtime * 5
                            self.cluster.assign_task(machineId, task)
#                            self.cluster.task_map[task.id] = machineId
                            self.cluster.check_if_vacant()
                            msg.append((task, machineId, task.runtime))
                            break
                else:
                    task.first_attempt_time = time
#            print time, task.id, task.stage_id, task.is_initial, "success:", success, "sign:",sign
        for item in msg:
            self.task_buffer.remove(item[0])
        return msg

    def submit_job(self, job):  # upon submission of a job, find the stages that are ready to be submitted
        self.cluster.running_jobs.append(job)
        self.cluster.calculate_targetAlloc()
        # DAG not allowed
        ready_stages = job.stages[0]
        self.ready_stages.append(job.stages[0])

        self.submit_jobs_number += 1
        return [ready_stages]
        # return [EventStageSubmit(time, stage) for stage in ready_stages]

    def submit_stage(self, stage, time):  # upon submission of a stage, all the tasks in the stage are ready to be submitted. Submit as many tasks as possible
        this_job = stage.job
        if stage.priority == 0:
            tmp = stage.taskset + self.task_buffer
            self.task_buffer = tmp
        else:
            self.task_buffer += stage.taskset
        # add by cc
        if len(stage.parent_ids) == 0:
            self.stageIdToAllowedMachineId[stage.id] = range(self.cluster.machine_number)
        else:
            tmpList = list()
            for id in stage.parent_ids:
                # need to change this data structure from list to dict (which can remove duplication automatically)
                tmpList += self.stageIdToUsedMachineId[id]
            # tmpList = list(set(tmpList))
            self.stageIdToAllowedMachineId[stage.id] = tmpList
        this_job.submitted_stage_ids.append(stage.id)
        self.submitted_stages_number += 1
        msg = self.do_allocate(time)
        return msg

    def stage_complete(self, stage): ##upon completion of a stage, check whether any other stage is ready to be submitted. If not, check whether the job is completed
        msg = list() # ready_stage or job (tell the simulator the entire job is done)
        stage.job.not_completed_stage_ids.remove(stage.id)
        stage.job.completed_stage_ids.append(stage.id)
        self.completed_stage_ids.append(stage.id)
        self.ready_stages.remove(stage)
        if len(stage.job.not_completed_stage_ids) != 0:
            msg.append(stage.job.stagesDict[stage.job.not_completed_stage_ids[0]])
            self.ready_stages.append(stage.job.stagesDict[stage.job.not_completed_stage_ids[0]])
        else:
            if len(stage.job.stages) == len(stage.job.completed_stage_ids):
                msg.append(stage.job)
        # after one stage completes, we shall update the ids of machines that have been used for executing the tasks within the stage
        tmpMachineList = list()
        for task in stage.taskset:
            tmpMachineList.append(task.machine_id)
        tmpMachineList = list(set(tmpMachineList))
        self.stageIdToUsedMachineId[stage.id] = tmpMachineList
        if stage.job.service_type == 0:
            machinelist = [task.machine_id for task in stage.taskset]
            machinelist = list(set(machinelist))
            print "stage complete:", stage.id, "stage tasknum:", len(stage.taskset), "used machine number:", len(machinelist)
        return msg

    def handle_job_completion(self, job):
        self.cluster.running_jobs.remove(job)
        self.cluster.calculate_targetAlloc()

    def find_ready_stages(self):
        #completed_stages = np.copy(self.completed_stage_ids) # completed in previous jobs
        #submitted_stage = list() # submitted by current jobs
        #for running_job in self.cluster.running_jobs:
        #    completed_stages += running_job.completed_stage_ids  # make sure they are lists
        #    submitted_stage  += running_job.submitted_stage_ids
        ready_stages = list()
        for running_job in self.cluster.running_jobs:
            for stage in running_job.stages:
                ready_flag = True
                for parent_id in stage.parent_ids:
                    if parent_id not in self.completed_stage_ids:
                        ready_flag = False
                        break
                if ready_flag and stage not in self.ready_stages: # avoid duplicated submission
                    ready_stages.append(stage) # if all of the parent stages are completed, this stage is ready to be submitted
        return ready_stages

    def search_core_by_task(self, task):
        for machine in self.cluster.machines:
            for core in machine.cores:
                if core.running_task == task:
                    return core
        return False

