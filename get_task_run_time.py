
## Read input as the event log file path (absolute path)
## Parse out the task runtime and categrate it by the data read method (from Hadoop, Disk or Memory)
## In the same directory as the input file, Updata three json files: runtime_from_Hadoop.json. runtime_from_Memory.json and runtime_from_Disk.json
## To get a median runtime in multiple experiments, we record the count of trials we have made. Therefore, nested dictionary is used with a form as
## {stage_id :{task_index:{task_runtime, trial_count}}}


import json
import os
import sys
import collections

def get_json(line):
  # Need to first strip the trailing newline, and then escape newlines (which can appear
  # in the middle of some of the JSON) so that JSON library doesn't barf.
  return json.loads(line.strip("\n").replace("\n", "\\n"))

def get_task_run_time(argv):

    file_path = argv  # absolute path :example :   /Users/yuyinghao/spark-1.6.1/log/PageRank-0-1-hd   0-1 means the storage.memoryFraction=0.1 hd is means hard disk is allowed for storing Rdds
    json_dir=file_path.split('-')[0]  +'-'+file_path.split('-')[1]   # /Users/yuyinghao/spark-1.6.1/log/PageRank_json
    if not os.path.isdir(json_dir):
        os.makedirs(json_dir)
    #check if the json files exist
    if os.path.exists(json_dir+'/runtime.json'):
        with open(json_dir+'/runtime.json', 'r') as f:
            runtime = json.load(f)
    else:
        runtime = collections.OrderedDict();  # nested dictionary {stage_id :{task_index:{task_runtime, trial_count}}}



    f = open(file_path, "r")
    for line in f:
        json_data = get_json(line)
        event_type = json_data["Event"]

        if event_type == "SparkListenerTaskEnd":
            stage_id=json_data["Stage ID"]
            task_info = json_data["Task Info"];
            launch_time = task_info["Launch Time"];
            finish_time = task_info["Finish Time"];
            task_runtime = long(finish_time) - long(launch_time);
            task_id = task_info["Task ID"];
            task_index=task_info["Index"]
            task_metrics = json_data["Task Metrics"];

            if str(stage_id) in runtime:
                if str(task_index) in runtime[str(stage_id)]:
                        pre_runtime=runtime[str(stage_id)][str(task_index)]['runtime']
                        pre_count=runtime[str(stage_id)][str(task_index)]['count']
                        runtime[str(stage_id)][str(task_index)]['runtime']=(long(pre_runtime)*long(pre_count)+task_runtime)/(pre_count+1)
                        runtime[str(stage_id)][str(task_index)]['count']+=1
                else:
                        runtime[str(stage_id)][str(task_index)] = {}
                        runtime[str(stage_id)][str(task_index)]['runtime'] = task_runtime
                        runtime[str(stage_id)][str(task_index)]['count'] = 1
            else:
                runtime[str(stage_id)] = {}
                runtime[str(stage_id)][str(task_index)]={}
                runtime[str(stage_id)][str(task_index)]['runtime']=task_runtime
                runtime[str(stage_id)][str(task_index)]['count'] = 1


    #update the runtime json files

    with open(json_dir + '/runtime.json', 'w') as f:
        json.dump(runtime, f)
if __name__ == "__main__":
    get_task_run_time('/Users/yuyinghao/Desktop/trace-analysis/simulator/cachepolicysimulator/profile_new/Page-Disk')#(sys.argv[1])#('/Users/yuyinghao/spark-1.6.1/log/PageRank-default-ssd') #
