import sevenbridges as sbg
from sevenbridges.errors import SbgError
from sevenbridges.http.error_handlers import *
import datetime
import binpacking
import re

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

tasks = [t for t in api.tasks.query(project=my_project, limit=100).all() if (t.status == "COMPLETED")]

# input_port = "SPECIFY_INPUT_PORT_HERE"
# for t in tasks:
#     files_processed = t.inputs[input_port]
#     for f in files_processed:
#         if 'ProcessedFiles' not in f.tags: 
#             f.tags = ['ProcessedFiles']
#             f.save()
#     print("Tagged %d files for task %s" %(len(files_processed),t.name))

file_size_limit = 6 *1024 * 1024 * 1024

reference_list = ['GRCh37-lite', 'GRCh37-lite-+-HPV_Redux-build', 'GRCh37-lite_WUGSC_variant_1', 'GRCh37-lite_WUGSC_variant_2', 'HG19_Broad_variant']
bam = "BAM"
bam_index = "BAI"

bams = [f for f in api.files.query(project=my_project.id,limit=100, metadata={'reference_genome':reference_list, 'data_format': bam }).all()]
bam_indices = [f for f in api.files.query(project=my_project.id,limit=100, metadata={'reference_genome':reference_list, 'data_format': bam_index = "BAI"
 }).all()]
reference_file = api.files.get(id='5890a37ee4b03eab16190778')


# get the ID of the app. The ID of the APP is your username/projectname/appID
# You can find that from the PAGE url for your app/workflow in question. 
app = api.apps.get(id="s_iakhnin/gckr_lof/scatter_freebayes")

# Inputs need to be set to inputs for the App you are running. 
# If there are certain fixed inputs, you can set them outside the for loop
inputs = {}

input_files = bams[0:6]
input_indices = bam_indices[0:6]
 
task_name = 'API task test')

inputs[input_port_bam_file] = input_files
inputs[input_port_bai_file] = input_indices

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