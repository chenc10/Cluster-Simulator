##Simulator submits jobs and stages to the scheduler
##Scheduler submits tasks to the cluster

class Scheduler:
    def __init__(self, cluster):
        self.cluster = cluster
        self.task_buffer = list()  #tasks waiting to be scheduled
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
        self.scheduler_type = "fair"
#        self.taskIdToAllowedMachineId = dict()

    def check_waiting(self):
        print "check_waiting:", len(self.task_buffer)
        if len(self.task_buffer) > 0:
            return True
        else:
            return False

    def sort_tasks(self):
        if self.scheduler_type == "fair":
            # - fair scheduler: preferentially serve the job starved most
            self.task_buffer.sort(key=lambda x: x.job.alloc / x.job.weight)
        else:
            # - other scheduler: first define a target allocation, and then conduct progressive filling
            self.task_buffer.sort(key=lambda x: x.job.alloc / x.job.targetAlloc)
        return

    def do_allocate(self, time):
        self.sort_tasks() # - this function is very important! It determines the scheduling algorithms!
        msg=list() # - msg returns the allocation scheme
        if len(self.cluster.make_offers()) == 0 or len(self.task_buffer) == 0:
            # - if there is no idle slot or no pending tasks, skip allocation immediately
            return msg
        for stage in self.task_buffer:
            # - allocate all the idle slots to pending tasks
            tmpList = [i for i in stage.not_submitted_tasks]
            for task in tmpList:
                if len(self.cluster.make_offers()) == 0:
                    # - if there is no idle slot
                    return msg
                success = False
                sign = False
                for machineId in self.cluster.make_offers():
                    sign = True
                    if machineId in self.stageIdToAllowedMachineId[task.stage.id]:
                        # - if locality requirement is satisfied
                        success = True
                        self.cluster.assign_task(machineId, task, time)
                        msg.append((task, machineId))
                        break
                if success == False and sign == True:
                    # - if locality requirement is not satisfied.
                    if task.first_attempt_time > 0:
                        # - first check whether the locality wait has been time out
                        if time - task.first_attempt_time > task.timeout:
                            for machineId in self.cluster.make_offers():
                                # - task runtime is prolonged due to poor data locality
                                task.runtime *= 1.5
                                self.cluster.assign_task(machineId, task, time)
                                msg.append((task, machineId))
                                break
                    else:
                        task.first_attempt_time = time
        return msg

    def submit_job(self, job):  # upon submission of a job, find the stages that are ready to be submitted
        self.cluster.running_jobs.append(job)
        self.cluster.calculate_targetAlloc()
        # - current I assume each job has only one stage
        ready_stages = job.stages[0]
        self.ready_stages.append(job.stages[0])

        self.submit_jobs_number += 1
        return [ready_stages]
        # return [EventStageSubmit(time, stage) for stage in ready_stages]

    def submit_stage(self, stage, time):  # upon submission of a stage, all the tasks in the stage are ready to be submitted. Submit as many tasks as possible
        this_job = stage.job
        self.task_buffer.append(stage)
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
        self.task_buffer.remove(stage)
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
        machinelist = [task.machine_id for task in stage.taskset]
        machinelist = list(set(machinelist))
        print "stage complete:", stage.id, "stage tasknum:", len(stage.taskset), "used machine number:", len(machinelist)
        return msg

    def handle_job_completion(self, job):
        self.cluster.running_jobs.remove(job)
        self.cluster.finished_jobs.append(job)

    def find_ready_stages(self):
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

