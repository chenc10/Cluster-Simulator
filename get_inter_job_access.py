import os
import json
from collections import OrderedDict
import math
import sys


def get_inter_job_access(argv):

    json_dir = argv
    stage_profile_path = '%s/stage_profile.json' % json_dir
    if not os.path.exists(stage_profile_path):
        exit(-1)
    else:
        stage_profile = json.load(open(stage_profile_path, 'r'), object_pairs_hook=OrderedDict)
    inter_job_access = open("%s/inter_job_access.txt" % json_dir, "w")
    total_inter_job_access = open("/Users/yuyinghao/Desktop/trace-analysis/simulator/cachepolicysimulator/total_inter_job_access.txt", "a+")
    total_inter_job_access.write("JobDistance\n")
    # LateDetection\tEarlyDetection
    late_detection = 0.0 # late_detection: detect dead later than its real death
    early_detection = 0.0
    dead_detection = 0.0 # total times of predictions for dead
    rdd_job = dict() # map (rdd_id, job_id)
    job_final_rdd = dict() #map (job_id, rdd_id) finally generated rdd of each job
    required_rdds_by_job = dict()  # map(job_id, rdd_id_list)
    produced_rdds_by_job = dict()  # map(job_id, rdd_id_list)
    for stage_id in stage_profile:
        job_id = stage_profile[stage_id]["Job ID"]
        if "Produced RDDs" in stage_profile[stage_id]:
            produced_rdds = stage_profile[stage_id]["Produced RDDs"].split(",")
            for produced_rdd in produced_rdds:
                rdd_job[produced_rdd] = job_id
                job_final_rdd[job_id] = produced_rdd
                if job_id not in produced_rdds_by_job.keys():
                    produced_rdds_by_job[job_id] = list()
                    produced_rdds_by_job[job_id].append(produced_rdd)
                elif produced_rdd not in produced_rdds_by_job[job_id]:
                    produced_rdds_by_job[job_id].append(produced_rdd)

    inter_job_access.write("JobID\tFinalRDD\n ")
    for job_id in job_final_rdd.keys():
        inter_job_access.write("%s\t%s\n" % (job_id, job_final_rdd[job_id]))
    inter_job_access.write("JobID\tStageID\tRequiredRDD\tIsInterJobAccess\tIsFinalRDD\tJobDistance\n")
    max_job_id = 0
    for stage_id in stage_profile:
        job_id = stage_profile[stage_id]["Job ID"]
        max_job_id = job_id
        if "Required RDDs" in stage_profile[stage_id]: # as rdd_ids
            required_rdds = stage_profile[stage_id]["Required RDDs"].split(",")
            for required_rdd in required_rdds:
                inter_job_access.write("%s:\t%s\t\t%s\t\t" % (job_id, stage_id, required_rdd))
                if rdd_job[required_rdd] == job_id:
                    inter_job_access.write("False\n")
                    total_inter_job_access.write("0\n")
                else:
                    job_distance = job_id - rdd_job[required_rdd]
                    inter_job_access.write("True\t\t")
                    total_inter_job_access.write("%s\n" % job_distance)
                    if required_rdd in job_final_rdd.values():
                        inter_job_access.write("True\t\t%s\n" % job_distance)
                    else:
                        inter_job_access.write("False\t\t%s\n" % job_distance)
                if job_id not in required_rdds_by_job.keys():
                    required_rdds_by_job[job_id] = list()
                    required_rdds_by_job[job_id].append(required_rdd)
                elif required_rdd not in required_rdds_by_job[job_id]:
                    required_rdds_by_job[job_id].append(required_rdd)
    dead_rdds = list() # predicted dead rdd
    reserved_rdds = list()# if the rdd is not used in this job, reserve it
    # reserve_distance = 1 # reserve the dead rdd by one job

    for job_id in range(0, max_job_id+1):
        if job_id in produced_rdds_by_job.keys():
            produced_rdds = produced_rdds_by_job[job_id]
        else:
            produced_rdds = list()
        if job_id in required_rdds_by_job.keys():
            required_rdds = required_rdds_by_job[job_id]
        else:
            required_rdds = list()
        a = 1
        for required_rdd in required_rdds:
            if required_rdd in reserved_rdds:
                dead_rdds.append(required_rdd)
                dead_detection += 1
                reserved_rdds.remove(required_rdd) # hit in this job, correct delay. Remove it after that
            elif required_rdd in dead_rdds: # early detection
                early_detection += 1
        for reserved_rdd in reserved_rdds: # late detection
            late_detection += 1
            dead_detection += 1
            reserved_rdds.remove(reserved_rdd)
        for produced_rdd in produced_rdds:
            #if produced_rdd in required_rdds:  # it is consumed in this job
                #dead_rdds.append(produced_rdd)
                #dead_detection += 1
            #else:
                reserved_rdds.append(produced_rdd)

    total_inter_job_access.write("Detection times:\t%s\nPremature Detection:\t%s\nLate Detection:\t%s\n" %
                                 (dead_detection, early_detection, late_detection) )
    inter_job_access.close()
    total_inter_job_access.close()


if __name__ == '__main__':
    get_inter_job_access(sys.argv[1])#('/Users/yuyinghao/Desktop/trace-analysis/simulator/cachepolicysimulator/profile/MatrixFactorization')#






