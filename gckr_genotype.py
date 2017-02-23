import sevenbridges as sbg
from sevenbridges.errors import SbgError
from sevenbridges.http.error_handlers import *
import datetime
import binpacking
import re
import os
import math


print("SBG library imported.")
prof = 'cgc'
config_file = sbg.Config(profile=prof)
api = sbg.Api(config=config_file,error_handlers=[rate_limit_sleeper,maintenance_sleeper,general_error_sleeper])
print "Api Configured!!"
print "Api Username : ", api.users.me()
my_project_name = 'GCKR_LOF'
my_project = [p for p in api.projects.query().all() if p.name == my_project_name][0]
print("Found project %s with project details %s" % (my_project.name,my_project.description))

input_port_reference = "reference"
input_port_bam_file = "BAMfile"
input_port_targets = "targets"

reference_list = ['GRCh37-lite', 'GRCh37-lite-+-HPV_Redux-build', 'GRCh37-lite_WUGSC_variant_1', 'GRCh37-lite_WUGSC_variant_2', 'HG19_Broad_variant']
bam = "BAM"

bams = [f for f in api.files.query(project=my_project.id,limit=100, metadata={'reference_genome':reference_list, 'data_format': bam }).all() if f.name.endswith(".bam")]
bams_by_sample = {bam_file.metadata['sample_uuid']: bam_file for bam_file in bams}
print "Loaded BAMs"

reference_file = api.files.get(id='5890a37ee4b03eab16190778')
targets_file = api.files.get(id='58aebfcbe4b098bb307ba37a')

app = api.apps.get(id="s_iakhnin/gckr-lof/scatter-freebayes")

inputs = {}

num_files = len(bams_by_sample)
num_hosts = 10 #set in workflow
jobs_per_host = 8 #set in workflow
minutes_per_run = 3 #estimated
runs_per_hour = 60 / minutes_per_run
tasks_per_run = runs_per_hour * jobs_per_host * num_hosts
tasks_per_run = float(3700) #Set manually 
num_runs = int(math.ceil(num_files / tasks_per_run))

for run_index in range(num_runs): 
    low_bound = int(run_index * tasks_per_run)
    high_bound = int(min((run_index + 1) * tasks_per_run, num_files))
    
    included_keys = bams_by_sample.keys()[low_bound:high_bound]

    input_files = [bams_by_sample[my_key] for my_key in included_keys]
    task_name = "Freebayes multi-gene genotyping task {} {}".format(run_index+1, datetime.datetime.now())
    
    inputs[input_port_bam_file] = input_files
    inputs[input_port_reference] = reference_file
    inputs[input_port_targets] = targets_file
    
    my_task = api.tasks.create(name=task_name, project=my_project, 
                              app=app, inputs=inputs, run=False)
    if my_task.errors:
        print(my_task.errors)
    else: 
        print('Your task %s is ready to go' % my_task.name)
