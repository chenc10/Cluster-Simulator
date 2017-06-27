import os
import json
from collections import OrderedDict
import math
import sys


def get_reference_distance(argv):

    json_dir = argv
    stage_profile_path = '%s/stage_profile.json' % json_dir
    if not os.path.exists(stage_profile_path):
        exit(-1)
    else:
        stage_profile = json.load(open(stage_profile_path, 'r'), object_pairs_hook=OrderedDict)
    access_distance = open("%s/access_distance.txt" % json_dir, "w")
    access_order = open("%s/access_order.txt" % json_dir, "w")
    effective_stage_id = -1  # those stages that produce rdds
    rdd_last_access_stage_id = dict() # map (rdd_id, last_access_stage_id) the most recent stage this rdd accessed or generated
    for stage_id in stage_profile:
        #job_id = stage_profile[stage_id]["Job ID"]

        if "Produced RDDs" in stage_profile[stage_id]:
            produced_rdds = stage_profile[stage_id]["Produced RDDs"].split(",")
            for produced_rdd in produced_rdds:
                effective_stage_id += 1
                rdd_last_access_stage_id[produced_rdd] = effective_stage_id

    effective_stage_id = -1  # some stages are skipped
    for stage_id in stage_profile:
        if "Produced RDDs" in stage_profile[stage_id]:
            produced_rdds = stage_profile[stage_id]["Produced RDDs"].split(",")
            for produced_rdd in produced_rdds:
                effective_stage_id += 1
                rdd_last_access_stage_id[produced_rdd] = effective_stage_id
        if "Required RDDs" in stage_profile[stage_id]:
            required_rdds = stage_profile[stage_id]["Required RDDs"].split(",")
            for required_rdd in required_rdds:
                access_order.write("%s\n" % required_rdd)
                last_referenced_stage_id = rdd_last_access_stage_id[required_rdd]
                distance = math.fabs(effective_stage_id-last_referenced_stage_id)
                access_distance.write("%s\n" % distance)
                rdd_last_access_stage_id[required_rdd] = effective_stage_id
                
    access_distance.close()


if __name__ == '__main__':
    get_reference_distance(sys.argv[1])#('/Users/yuyinghao/Desktop/trace-analysis/simulator/cachepolicysimulator/profile/PageRank')#






