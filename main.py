import sys
import os
from machine import Machine
from cluster import Cluster
from simulator import Simulator
from math import pow

## data preparation
# get_block_size(json_dir+"-default-ssd")
# get_stage_profile(json_dir + "-default-ssd")
# get_task_run_time(json_dir + "-default-ssd")


user_number = 1
machine_number = 4000
core_number = 1
json_dir = "./"

machines = [Machine(i, core_number) for i in range(0, machine_number)]
cluster = Cluster(machines)

simulator = Simulator(cluster, json_dir, user_number)
cluster.alpha = 0.8
simulator.scheduler.scheduler_type = "fair"

simulator.run()
print "finish"
