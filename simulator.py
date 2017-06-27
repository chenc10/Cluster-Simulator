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
        # generate the job list for each user. All users share the rdd_list and block list
        for user_index in range(0, user_number):
            # each user randomly chooses an application
            #application_number = random.randint(1, self.total_application_type)
            application_number = user_index + 1
            application_name = Simulator.application[application_number]
            self.app_map[user_index] = application_number
            stage_profile_path = '%s/%s/stage_profile.json' % (json_dir, application_name)
            if not os.path.exists(stage_profile_path):
                print('Error: can not find %s/stage_profile.json' % application_name)
                print stage_profile_path
                exit(-1)
            else:
                self.stage_profile = json.load(open(stage_profile_path, 'r'), object_pairs_hook=OrderedDict)
                print "stage_profile loaded"

#            rdd_profile_path = '%s/%s/rdd_profile.json' % (json_dir, application_name )
#            if not os.path.exists(rdd_profile_path):
#                print('Error: can not find %s/rdd.json' % application_name)
#                exit(-1)
#            else:
#                self.rdd_profile = json.load(open(rdd_profile_path, 'r'), object_pairs_hook=OrderedDict)

            runtime_path = '%s/%s/runtime.json' % (json_dir, application_name)  #runtime of each task when the input data is read from Memory
            if not os.path.exists(runtime_path):
                print('Error: can not find %s/runtime.json' % application_name)
                exit(-1)
            else:
                self.runtime_profile = json.load(open(runtime_path, 'r'), object_pairs_hook=OrderedDict)
                print "runtime_profile loaded"
#            self.generate_rdd_profile(user_index)

            job_path = '%s/%s/job.json' % (json_dir, application_name)  #runtime of each task when the input data is read from Memory
            if not os.path.exists(job_path):
                print('Error: can not find %s/job.json' % application_name)
                exit(-1)
            else:
                self.job_profile = json.load(open(job_path, 'r'), object_pairs_hook=OrderedDict)
                print "job_profile loaded"
            self.generate_job_profile(user_index)
            #            self.generate_rdd_profile(user_index)
#        self.cluster.block_manager.rdd_list = self.rdd_list

    def run(self):

        # self.init_ref_count()
#        self.cluster.block_manager.rdd_list = self.rdd_list  # to tell the block_manager how many partitions each rdd has
        print "hah"
        runtime = 0
        self.log.add('Simulation Starts with %s machines.'%(len(self.cluster.machines)),0)
        #completed_stage_ids=list()
        current_job_index = dict()  # map from user id to its current running job index
        # This code segment shall be modified. Initially all the jobs shall be submitted.
        for user_index in range(0, self.user_number):
            current_job_index[user_index] = 0
            for job_i in range(len(self.job_list[user_index])):
                if self.job_list[user_index][job_i].index > 8000 and self.job_list[user_index][job_i].index < 8000:
                    continue
                if self.job_list[user_index][job_i].index % 100000 > 10001:
                    continue
                self.event_queue.put(EventJobSubmit(self.job_list[user_index][job_i].submit_time, self.job_list[user_index][job_i]))
        self.event_queue.put(EventReAlloc(0))

        # add by cc
        drawEnabled = False
        if drawEnabled:
            ExecutorState = []
            NoE = 30
            for i in range(NoE):
                ExecutorState.append([-1]*10000)

        while not self.event_queue.empty():
            event = self.event_queue.get()
            new_events = list()
            if isinstance(event, EventReAlloc):
                msg = self.scheduler.do_allocate(event.time)
                if self.scheduler.check_waiting():
                    new_events.append(EventReAlloc(event.time + 1000))
                for item in msg:
                    new_events.append(EventTaskSubmit(event.time, item[0]))
                    new_events.append(EventTaskComplete(event.time + item[2], item[0], item[1]))

            if isinstance(event, EventJobSubmit):
                current_job_index[event.job.user_id] = event.job.index
                ready_stages = self.scheduler.submit_job(event.job)
                for stage in ready_stages:
                    new_events.append(EventStageSubmit(event.time, stage))

            elif isinstance(event, EventStageSubmit):
                event.stage.submit_time = event.time
#                print 'Stage %s submitted at %s' %(event.stage.id, event.time)
#                print "     Current idle machine number: ", len(self.cluster.make_offers()), "open machine number:", self.cluster.open_machine_number
                if event.stage.job.service_type == self.cluster.foreground_type:
                    print "reserved for this stage:", self.cluster.jobIdToReservedNumber[event.stage.job_id]
                msg = self.scheduler.submit_stage(event.stage, event.time)
                for item in msg:
                    new_events.append(EventTaskSubmit(event.time, item[0]))
                    new_events.append(EventTaskComplete(event.time + item[2], item[0], item[1]))
                    if drawEnabled:
                        for t in range(event.time/100 + 1, (event.time + item[2])/100 + 1):
                            ExecutorState[item[1]][t] = int(item[0].job_id.split("_")[-1])

            elif isinstance(event, EventTaskSubmit):
                if self.cluster.straggler_mitigation_enabled:
                    if event.task.is_initial:
                        event.task.stage.not_submitted_tasks.remove(event.task)
                else:
                    event.task.stage.not_submitted_tasks.remove(event.task)
                if len(event.task.stage.not_submitted_tasks) == 0:
                    event.task.stage.last_task_submit_time = event.time
                continue

            elif isinstance(event, EventTaskComplete):
                if event.task.has_completed:
                    continue
                event.task.has_completed = True
                self.scheduler.stageIdToAllowedMachineId[event.task.stage_id].append(event.task.machine_id)
                self.cluster.release_task(event.task)
                sign = 1
                if event.task.is_initial:
                    event.task.stage.not_completed_tasks.remove(event.task)
                    event.task.stage.completed_tasks.append(event.task)
                    if len(event.task.stage.not_completed_tasks) == 0:
                        new_events.append(EventStageComplete(event.time, event.task.stage))
                    if self.cluster.straggler_mitigation_enabled:
                        if event.task.stage.job.service_type <> self.cluster.foreground_type:
                            sign = 0
                        elif len(event.task.stage.not_submitted_tasks) != 0:
                            sign = 0
                        elif event.task.stage.has_done_speculation:
                            sign = 0
                        elif len(event.task.stage.not_completed_tasks) > self.cluster.jobIdToReservedNumber[event.task.job_id]:
                            sign = 0
                        elif len(event.task.stage.not_completed_tasks) > len(event.task.stage.taskset)/2:
                            sign = 0
                        elif len(event.task.stage.taskset) == 1:
                            sign = 0
                        if sign == 1:
                            tmpList = []
                            for task in event.task.stage.not_completed_tasks:
                                taskruntime = task.stage.taskset[int(random.random()*(len(task.stage.taskset)-1))].runtime
                                task.peer = Task(task.job_id, task.stage_id, task.id, task.index, taskruntime, task.timeout, task.priority)
                                task.peer.is_initial = False
                                task.peer.stage = task.stage
                                task.peer.peer = task
                                tmpList.append(task.peer)
                                # TODO: set the task runtime here
                            self.scheduler.task_buffer = tmpList + self.scheduler.task_buffer
                            event.task.stage.has_done_speculation = True
                        if event.task.peer and not event.task.peer.has_completed:
                            new_events.append(EventTaskComplete(event.time, event.task.peer, event.task.peer.machine_id))
                else:
                    if event.task.peer and not event.task.peer.has_completed:
                        new_events.append(EventTaskComplete(event.time, event.task.peer, event.task.peer.machine_id))
                msg = self.scheduler.do_allocate(event.time)
                for item in msg:
                    new_events.append(EventTaskSubmit(event.time, item[0]))
                    new_events.append(EventTaskComplete(event.time + item[2], item[0], item[1]))
                    if drawEnabled:
                        for t in range(event.time/100 + 1, (event.time + item[2])/100 + 1):
                            ExecutorState[item[1]][t] = int(item[0].job_id.split("_")[-1])

            elif isinstance(event, EventStageComplete):
                if event.stage.job.service_type == self.cluster.foreground_type:
                    stageSlots = set()
                    for i in event.stage.taskset:
                        stageSlots.add(i.machine_id)

                    print "stage finish: ", event.stage.id, "used slots number:", len(stageSlots), "submit interval", event.stage.last_task_submit_time - event.stage.submit_time, "currently reserved for this job:", self.cluster.jobIdToReservedNumber[event.stage.job.id]
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
                #print "#####  time:", event.time, "stage completion", event.stage.id, event.stage.job_id

            elif isinstance(event, EventJobComplete):
                if event.job.service_type == self.cluster.foreground_type:
                    self.cluster.clear_reservation(event.job.id)
                event.job.completion_time = event.time
                event.job.duration = event.time - event.job.submit_time
                self.scheduler.handle_job_completion(event.job)
                print "-", event.job.id, " (job) finishes, duration", event.job.duration
                print "     Current idle machine number: ", len(self.cluster.make_offers()), "open machine number:", self.cluster.open_machine_number
                self.job_durations[int(event.job.id.split("_")[-1])] = event.job.duration

            for new_event in new_events:
                self.event_queue.put(new_event)

        f = open("PageRank/job_duration.json",'w')
        json.dump(self.job_durations,f,indent=2)
        f.close()
        f = open("PageRank/stage_duration.json",'w')
        json.dump(self.stage_durations,f,indent=2)
        f.close()
        if drawEnabled:
            currentTaskNumber = []
            totalNumber = []
            TimeLine = []
            time = 0.0
            for i in range(len(ExecutorState[0])):
                tmp = 0
                tmp1 = 0
                for j in range(NoE):
                    if ExecutorState[j][i] == 10000:
                        tmp = tmp + 1
                    if ExecutorState[j][i] > -1:
                        tmp1 = tmp1 + 1
                currentTaskNumber.append(tmp)
                totalNumber.append(tmp1)
                time = time + 0.1
                TimeLine.append(time)
            font = {'family': 'Times New Roman',
                    'size': 26}
            N = len(ExecutorState[0])
            fig, ax = plt.subplots()
            fig.set_size_inches(9, 5)
            ax.plot(TimeLine, currentTaskNumber)
            ax.set_xlim([0, 200])
            ax.set_xlabel('Timeline (s)', **font)
            xlist = [0, 50, 100, 150, 200]
            ax.set_xticks(xlist)
            ax.set_xticklabels([str(i) for i in xlist], **font)
            ax.set_ylim([0, 50])
            ax.set_ylabel('# of Active Tasks', **font)
            ylist = [0, 20, 40, 60, 80, 100]
            ax.set_yticks(ylist)
            ax.set_yticklabels([str(i) for i in ylist], **font)
            plt.gcf().subplots_adjust(bottom=0.2)
            fig.savefig("foo.pdf")
        return [runtime]

    def generate_job_profile(self, user_id):
        self.job_list.append(list())
        task_id = 0
        job_submit_time = dict()
        job_priority = dict()
        job_service_type = dict()
        print "enter generate_job_profile"

        stageIdToParallelism = dict()
        for c_job_id in self.job_profile:
            # temporary setting
            job_submit_time[int(c_job_id)] = self.job_profile[c_job_id]["Submit Time"]
            job_priority[int(c_job_id)] = self.job_profile[c_job_id]["Priority"]
            job_service_type[int(c_job_id)] = self.job_profile[c_job_id]["Service Type"]

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
                    runtime *= 10
                if runtime > max_time:
                    max_time = runtime
                Task_id = 'user_%s_task_%s' % (user_id, task_id)
                if timeout_type == 0:
                    task = Task(Job_id, Stage_id, Task_id, i, runtime, 0, job_priority[job_id])
                else:
#                    task = Task(Job_id, Stage_id, Task_id, i, runtime, 3000, job_priority[job_id])
                    task = Task(Job_id, Stage_id, Task_id, i, runtime, 0, job_priority[job_id])
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
                if job.service_type == self.cluster.foreground_type:
                    self.cluster.jobIdToReservedNumber[job.id] = 0
                    self.cluster.jobIdToReservedMachineId[job.id] = set()
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

    def search_runtime(self, stage_id, task_index):
        return self.runtime_profile[str(stage_id)][str(task_index)]['runtime']

    def search_job_by_id(self, job_id, user_index):
        for job in self.job_list[user_index]:
            if job.id == job_id:
                return job
        return False

    def reset(self):
        for job in self.job_list:
            job.not_completed_stage_ids = [stage.id for stage in job.stages]
            job.submitted_stage_ids = list()
            job.completed_stage_ids = list()
            for stage in job.stages:
                stage.not_submitted_tasks = stage.taskset
                stage.not_completed_tasks = stage.taskset
                stage.completed_tasks = list()
        self.cluster.reset()

