## We parse out the following information for each stage
##    1. job id
##    2. required RDDs [[],[],[],..]
##    3. required shuffled RDDs [[],[],[],...]
##    3. produced RDDs []
##    4. parent stages
##    5. task number

## Read input as the event log file path (absolute path)
## We use the event log where no eviction happens so that we can always search in the cache to find the materialized RDD
## For each stage, the materialized RDDs could be found in  {Event: StageCompleted} : RDD Info. StorageLevel.UseMemory =True
## Remove those materialized RDDs that are produced by this stage rather than needed by this stage.
## Those RDDs appearing for the first time are produced in this stage
## Parent information of stages will also be recorded
## Result will be stored in /AppName_json/stage_profile. {"stage_id": {"Required RDDs": {rdd_id,}, "Produced RDDs":{rdd_id,}, "Parents":{parent_id,}, }}

import json
import os
import sys
import operator
import collections
import itertools

existing_rdds = list() ## for finding the required rdds for each produced rdd

def get_json(line):
    # Need to first strip the trailing newline, and then escape newlines (which can appear
    # in the middle of some of the JSON) so that JSON library doesn't barf.
    return json.loads(line.strip("\n").replace("\n", "\\n"))


def get_stage_profile(argv):
    file_path = argv
    json_dir = file_path.split('-')[0] + '-' + file_path.split('-')[1]
    f = open(file_path, "r")

    stage_profile = collections.OrderedDict() # insert order
    ref_count = collections.OrderedDict()
    ref_lists = dict() # map from stage id to refed rdds
    shuffle_ref_lists = dict() # map from stage id to refed shuffled rdds

    rdd_list =list() ## actually cached rdds, including shuffled rdds
    stage_list =list() ## actually submitted stages
    first_required_rdd_of_jobs = collections.OrderedDict() # map( job_id  ->  map (rdd id -> ref ) )
    rdd_profile = dict() # map from rdd id to {"IsShuffle": 1, "Number of Partitions": 10}

    for line in f:
        json_data = get_json(line)
        event_type = json_data["Event"]

        if event_type == "SparkListenerEnvironmentUpdate":
            spark_property = json_data["Spark Properties"]
            appName = spark_property["spark.app.name"].replace(" ","")
            job_dag = open("%s/%s-JobDAG.txt" % (json_dir, appName), 'w')
        elif event_type == "SparkListenerJobStart":
            job_id = json_data["Job ID"]
            job_ref_count = collections.OrderedDict()
        elif event_type == "SparkListenerStageSubmitted":
            stage_info = json_data["Stage Info"]
            stage_id = int(stage_info["Stage ID"])
            stage_profile[stage_id] = {}
            stage_profile[stage_id]["Job ID"] = job_id
            stage_profile[stage_id]["Stage ID"] = stage_id
            stage_profile[stage_id]["Parents"] = list()
            #task_number = 0
            # find the required and produced RDDs in this stage
            stage_info = json_data["Stage Info"]
            stage_id = stage_info["Stage ID"]
            rdds = stage_info["RDD Info"]
            produced_rdds = list()
            required_rdds = list()
            # Now find the final RDD in this stage, whose id is the largest in this stage
            largest_rdd_id = -1
            for rdd in rdds:
                rdd_id = int(rdd["RDD ID"])
                rdd_type = rdd['Name']
                largest_rdd_id = max(rdd_id, largest_rdd_id)
                storage_level = rdd["Storage Level"]
                use_memory = storage_level["Use Memory"]
                if use_memory == True:
                    if rdd_id not in rdd_list:  # produced RDD
                        rdd_list.append(rdd_id)
                        produced_rdds.append(rdd_id)

                    if rdd_id not in rdd_profile.keys():
                        rdd_profile[rdd_id] = {}
                        rdd_profile[rdd_id]['Is Shuffle'] = 0
                        rdd_profile[rdd_id]['Ref Me'] = list()
                        rdd_profile[rdd_id]['To Ref'] = list()

            if largest_rdd_id not in produced_rdds: # except job 0
                produced_rdds.append(largest_rdd_id) # add the last rdd: shuffled RDD
                rdd_profile[largest_rdd_id] = {}
                rdd_profile[largest_rdd_id]['Is Shuffle'] = 1
                rdd_profile[largest_rdd_id]['Ref Me'] = list()
                rdd_profile[largest_rdd_id]['To Ref'] = list()

            stage_profile[stage_id]['Final RDD'] = largest_rdd_id


            # now get the required rdds for each produced rdd
            [required_rdds, required_shuffle_rdds, produced_rdds, ref_list, shuffle_ref_list] = get_required_rdds(produced_rdds, rdds)
            stage_profile[stage_id]['Produced RDDs'] = produced_rdds
            stage_profile[stage_id]['Required RDDs'] = required_rdds
            stage_profile[stage_id]['Required Shuffled RDDs'] = required_shuffle_rdds
            ref_lists[stage_id] = ref_list
            shuffle_ref_lists[stage_id] =shuffle_ref_list

            # update the rdd profile
            i = 0
            for produced_rdd in produced_rdds:
                to_ref_rdds = required_rdds[i] + required_shuffle_rdds[i]
                rdd_profile[produced_rdd]['To Ref'] = to_ref_rdds
                for to_ref_rdd in to_ref_rdds:
                    rdd_profile[to_ref_rdd]['Ref Me'].append(produced_rdd)
                i += 1

            '''
            # required_rdds = find_nearest_cached_ancestors(largest_rdd_id, rdds, produced_rdds)
            # ref_list = get_ref_list(largest_rdd_id, rdds, produced_rdds)  # might be duplicated
            for rdd_id in produced_rdds:
                if 'Produced RDDs' in stage_profile[stage_id]:
                    stage_profile[stage_id]['Produced RDDs'] += ',' + str(rdd_id)
                else:
                    stage_profile[stage_id]['Produced RDDs'] = str(rdd_id)
            for rdd_id in required_rdds:
                if 'Required RDDs' in stage_profile[stage_id]:
                    stage_profile[stage_id]['Required RDDs'] += ',' + str(rdd_id)
                else: # the first required rdd in this stage
                    stage_profile[stage_id]['Required RDDs'] = str(rdd_id)
                    if job_id not in first_required_rdd_of_jobs.keys(): # the first required rdd in this job
                        first_required_rdd_of_jobs[job_id] = str(rdd_id)

            for rdd_id in ref_list:
                if rdd_id in ref_count:
                    ref_count[rdd_id] += 1
                else:
                    ref_count[rdd_id] = 1
                if rdd_id in job_ref_count:
                    job_ref_count[rdd_id] += 1
                else:
                    job_ref_count[rdd_id] = 1

            parent_ids = stage_info["Parent IDs"]
            for parent_id in parent_ids:
                if parent_id in stage_list:
                    if 'Parents' in stage_profile[stage_id]:
                        stage_profile[stage_id]['Parents'] = stage_profile[stage_id]['Parents'] + ',' + str(
                            parent_id);
                    else:
                        stage_profile[stage_id]['Parents'] = str(parent_id)
            stage_list.append(stage_id)
            '''
        # Now count the task number in each stage
        elif event_type == "SparkListenerTaskEnd":
            stage_id =json_data["Stage ID"]
            if 'Task Number' in stage_profile[stage_id]:
                stage_profile[stage_id]['Task Number'] +=1
            else:
                stage_profile[stage_id]['Task Number'] = 1
        elif event_type == "SparkListenerStageCompleted":
            stage_info = json_data["Stage Info"]
            stage_id = stage_info["Stage ID"]
            task_number = stage_profile[stage_id]['Task Number']
            ref_list = ref_lists[stage_id]
            for rdd in ref_list:
                if rdd in ref_count:
                    ref_count[rdd] += 1
                else:
                    ref_count[rdd] =1
            shuffle_ref_list = shuffle_ref_lists[stage_id]
            for shuffled_rdd in shuffle_ref_list:
                if shuffled_rdd in ref_count:
                    ref_count[shuffled_rdd] += task_number
                else:
                    ref_count[shuffled_rdd] = task_number

            produced_rdds = stage_profile[stage_id]['Produced RDDs']
            for produced_rdd in produced_rdds:
                rdd_profile[produced_rdd]['Number of Partitions'] = task_number
            required_shuffle_rdds = list(itertools.chain.from_iterable(stage_profile[stage_id]['Required Shuffled RDDs']))
            for required_shuffle_rdd in required_shuffle_rdds:
                rdd_profile[required_shuffle_rdd]['Number of Partitions'] = task_number
        '''
        elif event_type == "SparkListenerJobEnd":

            if str(job_id) == "0":
                first_required_rdd = "0"
            else:
                first_required_rdd = first_required_rdd_of_jobs[job_id]
            job_dag.write("%s-" % first_required_rdd )
            for rdd_id in job_ref_count:
                job_dag.write("%s:%s;" % (rdd_id, job_ref_count[rdd_id]))
            job_dag.write("\n")
        '''
    
    # find the parent stages of each stage
    for stage_id in stage_profile.keys():
        required_shuffle_rdds = list(itertools.chain.from_iterable(stage_profile[stage_id]['Required Shuffled RDDs']))
        required_rdds = list(itertools.chain.from_iterable(stage_profile[stage_id]['Required RDDs']))
        required_rdds = set(required_shuffle_rdds + required_rdds)
        for required_rdd in required_rdds:
            for this_stage_id in stage_profile.keys():
                this_produced_rdds = stage_profile[this_stage_id]['Produced RDDs']
                if required_rdd in this_produced_rdds or required_rdd == int(stage_profile[this_stage_id]['Final RDD']):
                    if this_stage_id != stage_id:
                        stage_profile[stage_id]['Parents'].append(this_stage_id)
                    break

    if not os.path.isdir(json_dir):
        os.makedirs(json_dir)


    with open('%s/stage_profile.json' % json_dir, 'w') as f:
        json.dump(stage_profile, f)
    #rdd profile
    with open('%s/rdd_profile.json' % json_dir, 'w') as f:
        json.dump(rdd_profile, f)

    # ref_count
    f = open("%s/%s.txt"%(json_dir, appName), 'w')
    for key in ref_count.keys():
        f.write("%s:%s\n"%(key, ref_count[key]))



    #ref_count_by_first_ = collections.OrderedDict()  # map (the first required rdd in this job  --> job reference information)

def get_required_rdds(produced_rdds, rdds):
    rdd_parents = dict()  ## map from rdd_id to its parent rdds
    rdd_type = dict() ## map from rdd_id to its type
    required_rdds = list() ## list of lists
    required_shuffle_rdds = list() ## list of lists
    produced_rdds.sort()  ## ascending order
    for rdd in rdds:
        id = int(rdd['RDD ID'])
        type = rdd['Name']
        parents = rdd['Parent IDs']
        rdd_parents[id] = parents
        rdd_type[id] = type

    for produced_rdd in produced_rdds:
        this_required_rdds = list()
        this_required_shuffle_rdds = list()
        this_parents = rdd_parents[produced_rdd]
        if isinstance(this_parents, int):
            this_parents = [this_parents]
        while len(this_parents) > 0:
            updated_parents = list(this_parents)
            for this_parent in this_parents:
                this_parent = int(this_parent)
                updated_parents.remove(this_parent)# only delete one of this_parent even if it appears multiple times
                if rdd_type[this_parent] == 'ShuffledRDD':
                    if this_parent - 1 not in existing_rdds:
                        exit(-3) # we take it for granted that for each shuffled rdd, the required rdd's id equals to ID_of_shuffledrdd - 1
                    this_required_shuffle_rdds.append(this_parent - 1)
                elif this_parent in existing_rdds: ## not shuffled but existing: block RDDs
                    this_required_rdds.append(this_parent)
                elif this_parent == 0:
                    continue
                else: # this parent is neither shuffled rdd nor existing rdd nor RDD 0, trace back to its parents
                    new_parents = rdd_parents[this_parent]
                    updated_parents += new_parents

            this_parents = list(updated_parents)
        existing_rdds.append(produced_rdd)
        required_rdds.append(this_required_rdds)
        required_shuffle_rdds.append(this_required_shuffle_rdds)

    # for the case where the last shuffled rdd is not the only produced rdd, delete it.
    if len(produced_rdds) != 1:
        del produced_rdds[-1]
        del required_shuffle_rdds[-1]
        del required_rdds[-1]
    ref_list = list(itertools.chain.from_iterable(required_rdds))
    shuffle_ref_list = list(itertools.chain.from_iterable(required_shuffle_rdds))
    return [required_rdds, required_shuffle_rdds, produced_rdds, ref_list, shuffle_ref_list]





#### The following funcs are not used any more. ####

def find_nearest_cached_ancestors(rdd_id,rdd_info, produced_rdds):
    nearest_cached_ancestors=list()
    for rdd in rdd_info:
        if rdd['RDD ID'] == rdd_id:
            parents = rdd['Parent IDs']
            break

    for parent in parents:  ## here parents are RDD ids
        flag = False # flag for whether found the parent in rdds of this stage
        for rdd in rdd_info:
            if rdd['RDD ID'] == parent:
                parent_rdd = rdd
                flag = True
                break
        if flag == False: # Parent rdd is not in this stage: (for example: subtracted rdd)
            return list()
        if parent_rdd['Name'] == 'ShuffledRDD':
            continue
        elif parent_rdd['Storage Level']['Use Memory'] == True and parent not in produced_rdds:
            nearest_cached_ancestors.append(parent) # parent is a RDD id (int)
        else: # the parent is neither shuffled RDD nor cached RDD, trace back to the parents of this parent
            p = find_nearest_cached_ancestors(parent, rdd_info, produced_rdds)
            if len(p) != 0:
                nearest_cached_ancestors=nearest_cached_ancestors+p
    ## remove the duplicated ancestors
    if len(nearest_cached_ancestors) == 0:
        return list()
    else:  ## because list(emptyset) throws error
        return list(set(nearest_cached_ancestors))


def get_ref_list(largest_rdd_id, rdd_info, produced_rdds):
    ref_list = list()
    required_rdds_list = list()
    if isinstance(largest_rdd_id, int):
        required_rdds_list.append(largest_rdd_id)
    elif len(largest_rdd_id) >= 1:
        required_rdds_list += largest_rdd_id
    else:
        return list()
    for rdd_id in required_rdds_list:
        for rdd in rdd_info:
            if rdd['RDD ID'] == rdd_id:
                parents = rdd['Parent IDs']
                break

        for parent in parents:  ## here parents are RDD ids
            flag = False  # flag for whether found the parent in rdds of this stage
            for rdd in rdd_info:
                if rdd['RDD ID'] == parent:
                    parent_rdd = rdd
                    flag = True
                    break
            if flag == False:  # Parent rdd is not in this stage: (for example: subtracted rdd)
                continue
            if parent_rdd['Name'] == 'ShuffledRDD':
                continue
            elif parent_rdd['Storage Level']['Use Memory'] == True and parent not in produced_rdds:
                ref_list.append(parent)  # parent is a RDD id (int)
            else:  # the parent is neither shuffled RDD nor cached RDD, trace back to the parents of this parent
#                if parent in produced_rdds:
#                    ref_list.append(parent)
                p = get_ref_list(parent, rdd_info, produced_rdds)
                if len(p) != 0:
                    ref_list = ref_list + p
    return ref_list

if __name__ == '__main__':
    get_stage_profile('/Users/yuyinghao/Desktop/trace-analysis/simulator/cachepolicysimulator/profile_new/TPC-H')# trace-analysis/simulator/cachepolicysimulator/EC2/MF-default-ec2')#(sys.argv[1])#
