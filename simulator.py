import os
import json
import random
from collections import OrderedDict
from logger import Log
from rdd import RDD
from block import Block
from job import Job
from stage import Stage
from task import Task
from scheduler import Scheduler
from event import (Event, EventJobComplete, EventReAlloc, EventStageSubmit, EventJobSubmit, EventStageComplete, EventTaskComplete, EventTaskSubmit)
import random

import numpy as np
import matplotlib.pyplot as plt
import collections

try:
    import Queue as Q  # ver. < 3.0
except ImportError:
    import queue as Q



class Simulator:
    ## Map from number to application
    ## 1 : PageRank
    application = OrderedDict()
    application[1] = 'Monopolize'
    application[1] = 'PageRank'



    def __init__(self, cluster, json_dir, user_number):
        self.cluster = cluster
        # self.app_name = json_dir.split('/')[-1]
        self.log = Log()
        self.json_dir = json_dir
        self.cluster = cluster
        self.scheduler = Scheduler(cluster)
        self.block_list = list()
        self.job_list = list()  # list of lists. A job list for each user.
        self.event_queue = Q.PriorityQueue()
        self.timestamp = 0
        self.user_number = user_number
        self.total_application_type = 1
        self.app_map = OrderedDict()  # map from user id to app id
        self.job_durations = {}
        self.stage_durations = {}
        self.job_execution_profile = {} # record the execution information of jobs
        # generate the job list for each user. All users share the rdd_list and block list
        for user_index in range(0, user_number):
            # each user randomly chooses an application
            #application_number = random.randint(1, self.total_application_type)
            application_number = user_index + 1
            application_name = Simulator.application[application_number]
            self.app_map[user_index] = application_number
#            stage_profile_path = 'Workloads/stage_profile.json' % (json_dir, application_name)
            stage_profile_path = "Workloads/stage_profile.json"
            self.stage_profile = json.load(open(stage_profile_path, 'r'), object_pairs_hook=OrderedDict)
            print "stage_profile loaded"

            runtime_path = "Workloads/runtime.json"
            self.runtime_profile = json.load(open(runtime_path, 'r'), object_pairs_hook=OrderedDict)
            print "runtime_profile loaded"
#            self.generate_rdd_profile(user_index)

            job_path = "Workloads/job.json"
            self.job_profile = json.load(open(job_path, 'r'), object_pairs_hook=OrderedDict)
            print "job_profile loaded"
            self.generate_job_profile(user_index)

    def run(self):

        runtime = 0
        self.log.add('Simulation Starts with %s machines.'%(len(self.cluster.machines)),0)
        current_job_index = dict()  # map from user id to its current running job index
        # This code segment shall be modified. Initially all the jobs shall be submitted.
        for user_index in range(0, self.user_number):
            current_job_index[user_index] = 0
            for job_i in range(len(self.job_list[user_index])):
#                if self.job_list[user_index][job_i].index > 8000 and self.job_list[user_index][job_i].index < 8000:
#                    continue
#                if self.job_list[user_index][job_i].index % 100000 > 10001:
#                    continue
                self.event_queue.put(EventJobSubmit(self.job_list[user_index][job_i].submit_time, self.job_list[user_index][job_i]))
        self.event_queue.put(EventReAlloc(0))

        while not self.event_queue.empty():
            event = self.event_queue.get()
            new_events = list()
            if isinstance(event, EventReAlloc):
                msg = self.scheduler.do_allocate(event.time)
                # if len(self.cluster.finished_jobs) < len(self.job_list[0]) or self.scheduler.check_waiting():
                if len(self.cluster.finished_jobs) < len(self.job_list[0]):
                    new_events.append(EventReAlloc(event.time + 1000))
                for item in msg:
                    new_events.append(EventTaskSubmit(event.time, item[0]))
                    new_events.append(EventTaskComplete(event.time + item[0].runtime, item[0], item[1]))

            if isinstance(event, EventJobSubmit):
                current_job_index[event.job.user_id] = event.job.index
                ready_stages = self.scheduler.submit_job(event.job)
                for stage in ready_stages:
                    new_events.append(EventStageSubmit(event.time, stage))

            elif isinstance(event, EventStageSubmit):
                event.stage.submit_time = event.time
                msg = self.scheduler.submit_stage(event.stage, event.time)
                for item in msg:
                    new_events.append(EventTaskSubmit(event.time, item[0]))
                    new_events.append(EventTaskComplete(event.time + item[0].runtime, item[0], item[1]))

            elif isinstance(event, EventTaskSubmit):
                event.task.start_time = event.time
                if self.cluster.isDebug:
                    print "time", event.time, " submit task ", event.task.id, "-job-", event.task.job_id, "-slot-", event.task.machine_id
                if len(event.task.stage.not_submitted_tasks) == 0:
                    event.task.stage.last_task_submit_time = event.time
                continue

            elif isinstance(event, EventTaskComplete):
                event.task.finish_time = event.time
                if self.cluster.isDebug:
                    print "time", event.time, "   finish task ", event.task.id, "-job-", event.task.job_id, "-slot-", event.task.machine_id
                self.scheduler.stageIdToAllowedMachineId[event.task.stage_id].append(event.task.machine_id)
                self.cluster.release_task(event.task)
                event.task.stage.not_completed_tasks.remove(event.task)
                event.task.stage.completed_tasks.append(event.task)
                if len(event.task.stage.not_completed_tasks) == 0:
                    new_events.append(EventStageComplete(event.time, event.task.stage))

                if len(event.task.stage.not_submitted_tasks) > 0:
                    msg = [[event.task.stage.not_submitted_tasks[0], event.task.machine_id]]
                    runtime = self.cluster.assign_task(event.task.machine_id, event.task.stage.not_submitted_tasks[0], event.time)
                else:
                    msg = self.scheduler.do_allocate(event.time)
                for item in msg:
                    new_events.append(EventTaskSubmit(event.time, item[0]))
                    new_events.append(EventTaskComplete(event.time + item[0].runtime, item[0], item[1]))

            elif isinstance(event, EventStageComplete):
                stageSlots = set()
                for i in event.stage.taskset:
                    stageSlots.add(i.machine_id)
                event.stage.finish_time = event.time
                self.stage_durations[event.stage.id] = {}
                self.stage_durations[event.stage.id]["task num"] = len(event.stage.taskset)
                self.stage_durations[event.stage.id]["used slot num"] = len(stageSlots)
                self.stage_durations[event.stage.id]["monopolize"] = event.stage.monopolize_time
                self.stage_durations[event.stage.id]["duration"] = event.stage.finish_time - event.stage.submit_time
                msg = self.scheduler.stage_complete(event.stage) # ready_stage or job (tell the simulator the entire job is done)
                for item in msg:
                    if isinstance(item, Stage): # stage ready to be submitted
                        new_events.append(EventStageSubmit(event.time, item))
                    else: # must be job, which means the job is done
                        new_events.append(EventJobComplete(event.time, item))

            elif isinstance(event, EventJobComplete):
                event.job.completion_time = event.time
                event.job.duration = event.time - event.job.submit_time
                event.job.execution_time = event.time - event.job.start_execution_time
                print "-", event.job.id, " (job) finishes, duration", event.job.duration, " job.alloc ", event.job.alloc, "PR:", float(event.job.monopolize_time) / event.job.execution_time
                print
                event.job.progress_rate = float(event.job.monopolize_time) / event.job.execution_time
                self.scheduler.handle_job_completion(event.job)
                self.job_durations[int(event.job.id.split("_")[-1])] = event.job.duration
                job_id = int(event.job.id.split("_")[-1])
                self.job_execution_profile[job_id] = {}
                self.job_execution_profile[job_id]["duration"] = event.job.duration
                self.job_execution_profile[job_id]["demand"] = len(event.job.curve)-1
                self.job_execution_profile[job_id]["execution_time"] = event.job.execution_time
#                self.job_execution_profile[job_id]["runtimes"] = [[i.runtime, i.machine_id, i.start_time, i.finish_time] for i in event.job.stages[0].taskset]
                if self.scheduler.scheduler_type == "paf":
                    self.job_execution_profile[job_id]["fair_alloc"] = event.job.fairAlloc
                    self.job_execution_profile[job_id]["target_alloc"] = event.job.targetAlloc
                else:
                    self.job_execution_profile[job_id]["fair_alloc"] = event.job.alloc
                    self.job_execution_profile[job_id]["target_alloc"] = event.job.alloc
                self.job_execution_profile[job_id]["alloc"] = event.job.alloc
                self.job_execution_profile[job_id]["progress_rate"] = event.job.progress_rate

            for new_event in new_events:
                self.event_queue.put(new_event)

        progress_rates = []
        for job in self.job_list[0]:
            progress_rates.append(job.progress_rate)
        print "total average progress rate:", sum(progress_rates)/len(progress_rates)

        if self.scheduler.scheduler_type == "paf":
            fname = "ExecutionResult/" + str(self.cluster.machine_number) + "_" + self.scheduler.scheduler_type + "_" + str(self.cluster.alpha) +".json"
        else:
            fname = "ExecutionResult/" + str(self.cluster.machine_number) + "_" + self.scheduler.scheduler_type +".json"
        f = open(fname,'w')
        json.dump(self.job_execution_profile,f,indent=2, sort_keys=True)
        f.close()
#        f = open("Workloads/job_duration.json",'w')
#        json.dump(self.job_durations,f,indent=2)
#        f.close()
#        f = open("Workloads/stage_duration.json",'w')
#        json.dump(self.stage_durations,f,indent=2)
#        f.close()

        return [runtime]

    def generate_job_profile(self, user_id):
        self.job_list.append(list())
        task_id = 0
        job_submit_time = dict()
        job_priority = dict()
        job_service_type = dict()
        job_curveString = dict()
        job_monopolize_time = dict()
        job_weight = dict()
        job_accelerate_factor = dict()
        print "enter generate_job_profile"

        stageIdToParallelism = dict()
        for c_job_id in self.job_profile:
            # temporary setting
            job_submit_time[int(c_job_id)] = self.job_profile[c_job_id]["Submit Time"]
            job_priority[int(c_job_id)] = self.job_profile[c_job_id]["Priority"]
            job_service_type[int(c_job_id)] = self.job_profile[c_job_id]["Service Type"]
            job_curveString[int(c_job_id)] = self.job_profile[c_job_id]["curve"]
            job_monopolize_time[int(c_job_id)] = self.job_profile[c_job_id]["Monopolize Time"]
            job_weight[int(c_job_id)] = self.job_profile[c_job_id]["Weight"]
            job_accelerate_factor[int(c_job_id)] = self.job_profile[c_job_id]["Accelerate Factor"]

        for stage_id in self.stage_profile:
            timeout_type = 0
            job_id = self.stage_profile[stage_id]["Job ID"]
            self.job_durations[job_id] = 0
            Job_id = 'user_%s_job_%s' % (user_id, job_id)
            Stage_id = 'user_%s_stage_%s' % (user_id, stage_id )
            task_number = self.stage_profile[stage_id]["Task Number"]
            ### change parallelism

            stageIdToParallelism[Stage_id] = task_number

            Parent_ids = list()
            if "Parents" in self.stage_profile[stage_id]:
                parent_ids = self.stage_profile[stage_id]["Parents"]
                for parent_id in parent_ids:
                    Parent_ids.append('user_%s_stage_%s' % (user_id, parent_id))
                    if stageIdToParallelism[Parent_ids[-1]] >= task_number:
                        timeout_type = 1


            # generate taskset of the stage
            taskset = list()
            max_time = 0
            for i in range(0, task_number):
                runtime = self.search_runtime(stage_id, i)
                if job_service_type[job_id] <> 0:
                    runtime *= 1
                else:
                    runtime *= 1
                if runtime > max_time:
                    max_time = runtime
                Task_id = 'user_%s_task_%s' % (user_id, task_id)
                time_out = 0
                if timeout_type == 0:
                    task = Task(Job_id, Stage_id, Task_id, i, runtime, time_out, job_priority[job_id])
                else:
#                    task = Task(Job_id, Stage_id, Task_id, i, runtime, 3000, job_priority[job_id])
                    task = Task(Job_id, Stage_id, Task_id, i, runtime, time_out, job_priority[job_id])
                task_id += 1
                task.user_id = user_id
                taskset.append(task)
            stage = Stage(Job_id, Stage_id, Parent_ids, taskset)
            stage.monopolize_time = max_time

            for id in Parent_ids:
                self.scheduler.stageIdToStage[id].downstream_parallelism += len(taskset)

            self.scheduler.stageIdToStage[Stage_id] = stage
            for task in taskset:
                task.stage = stage
            stage.user_id = user_id
#            stage.produced_rdds = Produced_rdds # self.stage_profile[stage_id]["Produced RDDs"]

            if self.search_job_by_id(Job_id, user_id) == False:
                job = Job(Job_id)
                job.index = int(job_id)
                job.user_id = user_id
                job.stages.append(stage)
                job.submit_time = job_submit_time[job_id]
                job.priority = job_priority[job_id]
                job.service_type = job_service_type[job_id]
                job.weight = job_weight[job_id]
                job.accelerate_factor = job_accelerate_factor[job_id]
                job.set_curve(job_curveString[job_id])
                job.monopolize_time = job_monopolize_time[job_id]
                self.job_list[user_id].append(job)
                stage.priority = job.priority
                stage.job = job
            else: # this job already exits
                job = self.search_job_by_id(Job_id, user_id)
                job.stages.append(stage)
                stage.priority = job.priority
                stage.job = job

        # Set the not_completed_stage_ids for all the jobs
        for job in self.job_list[user_id]:
            job.not_completed_stage_ids = [stage.id for stage in job.stages]
            for tstage in job.stages:
                job.stagesDict[tstage.id] = tstage
            job.submitted_stage_ids = list()
            job.completed_stage_ids = list()

        # this part shall be changed, sort by the submission time of a job
        self.job_list[user_id] = sorted(self.job_list[user_id], key=lambda job: job.index) #sort job_list by job_index
        print "finish generate job profile"
        print "0: tasknumber:", len(self.job_list[0][0].stages[0].taskset)


    def search_runtime(self, stage_id, task_index):
        return self.runtime_profile[str(stage_id)][str(task_index)]['runtime']

    def search_job_by_id(self, job_id, user_index):
        for job in self.job_list[user_index]:
            if job.id == job_id:
                return job
        return False

    def reset(self):
        for job in self.job_list:
            job.reset()
        self.cluster.reset()

