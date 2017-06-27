
## Read input as the eventlog name (absolute path)
## Output the nearest cached ancestors of cachde RDDs in rdd_profile.json.
## The nearest cached ancestors: the rdds required to compute the given rdd

import json
import os
import sys


def get_json(line):
    # Need to first strip the trailing newline, and then escape newlines (which can appear
    # in the middle of some of the JSON) so that JSON library doesn't barf.
    return json.loads(line.strip("\n").replace("\n", "\\n"))


def get_rdd_profile(argv):
    rdd_profile = dict();  # pair as  #: ancestor list

    file_path = argv;  # absolute path, example :   /Users/yuyinghao/spark-1.6.1/log/PageRank-0-1-hd   0-1 means the storage.memoryFraction=0.1 hd is means hard disk is allowed for storing Rdds
    json_dir = file_path.split('-')[0] + '-' + file_path.split('-')[1] + '_json'  # /Users/yuyinghao/spark-1.6.1/log/PageRank_json
    # task_profile = open("%s_task_profile.txt" % filename, "w");
    f = open(file_path, "r")

    for line in f:
        json_data = get_json(line)
        event_type = json_data["Event"]
        if event_type == "SparkListenerStageSubmitted":
            stage_info = json_data['Stage Info']
            rdd_info = stage_info['RDD Info']
            for rdd in rdd_info:
                rdd_id = rdd['RDD ID']
                storage_level = rdd['Storage Level']
                use_memory = storage_level['Use Memory']
                if use_memory == True and rdd_id not in rdd_profile:
                    ancestors = find_nearest_cached_ancestors(rdd_id,rdd_info)  ## we can always find its cached ancestors, if there is any, right in the stage_info
                    rdd_profile[rdd_id]=ancestors


    #directory=os.path.dirname(os.path.abspath(file_path))

    if not os.path.isdir(json_dir):
        os.makedirs(json_dir)

    # Writing JSON data
    with open('%s/rdd_profile.json'%json_dir, 'w') as f:
        json.dump(rdd_profile, f)

    # Reading data back
    #with open('data.json', 'r') as f:
     #   data = json.load(f)


def find_nearest_cached_ancestors(rdd_id, rdd_info):
    nearest_cached_ancestors=list()
    for rdd in rdd_info:
        if rdd['RDD ID'] == rdd_id:
            parents = rdd['Parent IDs']
            break
    for parent in parents:  ## parents are RDD ids
        for rdd in rdd_info:
            if rdd['RDD ID'] == parent:
                parent_rdd = rdd
                break
        if parent_rdd['Name'] == 'ShuffledRDD':
            continue
        elif parent_rdd['Storage Level']['Use Memory'] == True:
            nearest_cached_ancestors.append(parent) # parent is a RDD id (int)
        else: # the parent is neither shuffled RDD nor cached RDD, trace back to the parents of this parent
            p = find_nearest_cached_ancestors(parent, rdd_info)
            if len(p) != 0:
                nearest_cached_ancestors=nearest_cached_ancestors+p
    ## remove the duplicated ancestors
    if len(nearest_cached_ancestors) == 0:
        return list()
    else:  ## because list(emptyset) throws error
        return list(set(nearest_cached_ancestors))

if __name__ == "__main__":
    get_rdd_profile('/Users/yuyinghao/Desktop/PageRank-default-ssd')#(sys.argv[1])