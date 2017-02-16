import sevenbridges as sbg
from sevenbridges.errors import SbgError
from sevenbridges.http.error_handlers import *
import datetime
import binpacking
import re
import os

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
input_port_bai_file = "BAIfile"

reference_list = ['GRCh37-lite', 'GRCh37-lite-+-HPV_Redux-build', 'GRCh37-lite_WUGSC_variant_1', 'GRCh37-lite_WUGSC_variant_2', 'HG19_Broad_variant']
bam = "BAM"
bam_index = "BAI"

bams = [f for f in api.files.query(project=my_project.id,limit=100, metadata={'reference_genome':reference_list, 'data_format': bam }).all()]
bams_by_sample = {bam_file.metadata['sample_uuid']: bam_file for bam_file in bams}
bams_by_filename = {os.path.splitext(bam_file.name)[0]: bam_file for bam_file in bams_by_sample.values()}

bam_indices = [f for f in api.files.query(project=my_project.id,limit=100, metadata={'reference_genome':reference_list, 'data_format': bam_index}).all()]
bam_indices_by_filename = {os.path.splitext(bam_index.name)[0]: bam_index for bam_index in bam_indices}
dupe_keys = set(bam_indices_by_filename.keys()) - set(bams_by_filename.keys())


reference_file = api.files.get(id='5890a37ee4b03eab16190778')


# get the ID of the app. The ID of the APP is your username/projectname/appID
# You can find that from the PAGE url for your app/workflow in question. 
app = api.apps.get(id="s_iakhnin/gckr-lof/scatter-freebayes/10")

# Inputs need to be set to inputs for the App you are running. 
# If there are certain fixed inputs, you can set them outside the for loop
inputs = {}

input_files = bams[0:6]
input_indices = bam_indices[0:6]
 
task_name = 'API task test'

inputs[input_port_bam_file] = input_files
inputs[input_port_bai_file] = input_indices
inputs[input_port_reference] = reference_file
my_task = api.tasks.create(name=task_name, project=my_project, 
                          app=app, inputs=inputs, run=False)
if my_task.errors:
    print(my_task.errors())
else: 
    print('Your task %s is ready to go' % my_task.name)
    # my_task.run()
    
# Running a few of the draft tasks:
draft_tasks = [t for t in api.tasks.query(project=my_project,limit=100).all() if t.status == "DRAFT"]
print(len(draft_tasks))
for t in draft_tasks:
    t.run()