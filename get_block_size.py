
## Read input as the eventlog name (absolute path)
## Write the block_size of cached RDDs in block_size.json in the same directory as the input file

import json
import os
import sys
import collections


def get_json(line):
    # Need to first strip the trailing newline, and then escape newlines (which can appear
    # in the middle of some of the JSON) so that JSON library doesn't barf.
    return json.loads(line.strip("\n").replace("\n", "\\n"))


def get_block_size(argv):

    block_size = collections.OrderedDict();  # pair as  rdd_#_#: size

    file_path = argv;  # absolute path, example :   /Users/yuyinghao/spark-1.6.1/log/PageRank-0-1-hd   0-1 means the storage.memoryFraction=0.1 hd is means hard disk is allowed for storing Rdds
    json_dir = file_path.split('-')[0] + '-' + file_path.split('-')[1]  # /Users/yuyinghao/spark-1.6.1/log/PageRank
    # task_profile = open("%s_task_profile.txt" % filename, "w");
    f = open(file_path, "r")

    for line in f:
        json_data = get_json(line)
        event_type = json_data["Event"]
        if event_type == "SparkListenerTaskEnd":
            task_metrics = json_data["Task Metrics"];
            if "Updated Blocks" in task_metrics:
                updated_blocks = task_metrics["Updated Blocks"];
                for block in updated_blocks:
                    block_id = block["Block ID"];
                    status = block["Status"];
                    storage_level=status["Storage Level"]
                    use_memory=storage_level["Use Memory"]
                    memory_size = status["Memory Size"]
                    if use_memory == True:
                        if block_id not in block_size:
                            block_size[block_id]=memory_size
                        elif block_size[block_id]!= memory_size:
                            print "Two different sizes for %s!"%block_id

    #directory=os.path.dirname(os.path.abspath(file_path))

    if not os.path.isdir(json_dir):
        os.makedirs(json_dir)

    # Writing JSON data
    with open('%s/block_profile.json'%json_dir, 'w') as f:
        json.dump(block_size, f)

    # Reading data back
    #with open('data.json', 'r') as f:
     #   data = json.load(f)

if __name__ == "__main__":
    get_block_size('/Users/yuyinghao/Desktop/trace-analysis/simulator/cachepolicysimulator/profile/MatrixFactorization-default-ssd')#(sys.argv[1])