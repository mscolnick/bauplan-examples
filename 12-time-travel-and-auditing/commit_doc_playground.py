### SETUP

# import a client and point to my dev to avoid polluting prod - the profile should be delete in the docs
import bauplan
client = bauplan.Client(profile='prod')
# read the user info to parametrize automatically the branch names below
user = client.info().user
username = user.username
full_name = user.full_name
# make sure we have all the info available
assert full_name is not None and username is not None and user is not None
# important global branch / table variables
source_branch_name = 'main'
my_branch_name = f'{username}.commit_flow'
my_test_table_name = 'my_taxxxi_zones'  # this is the model created by the companion project
my_compliance_dataset_tag = 'compliance_tag' # this is semantic tag over a commit to allow for fast retrieval during audit
# make sure we start from scratch (as it's a demo flow!)
if client.has_branch(my_branch_name):
    client.delete_branch(my_branch_name)
    
    
### GOING BACK AND FORTH ON THE "MULTI-VERSE" WITH THE NEW APIs

client.create_branch(my_branch_name, from_ref=source_branch_name)
# make sure we start from scratch (as it's a demo flow!)
if client.has_table(my_test_table_name, ref=my_branch_name):
    client.delete_table(my_test_table_name, branch=my_branch_name)
# get the latest commits on each branch
my_branch_last_commit = client.get_commits(my_branch_name, limit=1)[0]
source_branch_last_commit = client.get_commits(source_branch_name, limit=1)[0]
# Q: what's the difference between the two branches?
# A: in theory, this is a zero-copy op, so no difference should be there yet! Let's check:
assert my_branch_last_commit.ref.hash == source_branch_last_commit.ref.hash
# Q: what happens if we now materialize a model through a run?
run_1 = client.run(
    project_dir='./my_project',
    ref=my_branch_name,
    cache='off',
    parameters={'run_id': 1},
)
print(f"Bauplan run completed: {run_1.job_id, run_1.job_status}")
assert run_1.job_id is not None and run_1.job_status == 'SUCCESS'
my_branch_last_commit = client.get_commits(my_branch_name, limit=1)[0]
# A: these should now be different, since we did a write operation in our branch
# through the above run!
assert my_branch_last_commit.ref.hash != source_branch_last_commit.ref.hash
# Note that the commit message specifies the job_id responsible for the change!
print(f"The last commit on my branch was {my_branch_last_commit.message}")
# we can also access the job_id programmaticaly thanks to the Bauplan-specific properties
job_id_in_the_commit = my_branch_last_commit.properties['bpln_job_id']
assert job_id_in_the_commit == run_1.job_id

# let's create a second commit now, so that we can test the other APIs as well
# by introducing a timeline of run_1 > run_2 in the branch
run_2 = client.run(
    project_dir='./my_project',
    ref=my_branch_name,
    cache='off',
    parameters={'run_id': 2},
)
# Q: if I query the table for the run_id, what should I expect to find on my branch as the current HEAD?
run_id_query = f'SELECT run_id FROM {my_test_table_name}'
rows = client.query(run_id_query, ref=my_branch_name).to_pylist()
# A: the latest run_id, which is 2!
assert rows == [{'run_id': 2}]
# Q: if I now query the table for the run_id but pointing to the previous commit (the one generated by run_1)
# what should I expect to get?
rows = client.query(run_id_query, ref=my_branch_last_commit.ref).to_pylist()
# A: the original run_id, which is 1!
assert rows == [{'run_id': 1}]
# since we particularly liked run_1, let's tag it for future reference
# run_1 may be tagged with an Id of a customer or some compliance id to allow easy retrieval
# first, make sure we start from scratch (it'a demo!)
if client.has_tag(my_compliance_dataset_tag):
    client.delete_tag(my_compliance_dataset_tag)

tag_1_ref = client.create_tag(my_compliance_dataset_tag, my_branch_last_commit.ref)

### THIS IS A GOOD MOMENT TO AUDIT OUR TEAM - WHO DID WHAT?

# Q: if I know a user, can I get his latest commits?
my_author_commit_history = client.get_commits(my_branch_name, filter_by_author_name=full_name, limit=5)
# A: this should return the last 5 commits by the user who ran this script, which should include the
# commit from run_1 above for example!
for c in my_author_commit_history:
    if c.ref.hash == my_branch_last_commit.ref.hash:
        print(f"Found the commit from run_1 by {full_name}!")
        break

# Q: if I know a job Id, can I get the commits associated to it?
# Note: this API works but it's not "officially" supported: use with care
job_id_1_commit = client._get_commit_by_job_id(run_1.job_id)
# A: this should be the same commit as we got back from the commit API immediately after running run_1 above!
assert job_id_1_commit.ref.hash == my_branch_last_commit.ref.hash

# Q: if we tagged a dataset as compliant in the past (even if not a current version anymore), can we find it?
target_tag = client.get_tag(my_compliance_dataset_tag)
rows = client.query(run_id_query, ref=target_tag).to_pylist()
# A: this should return the original run_id, which is 1!
assert rows == [{'run_id': 1}]

### LET'S DO SOME DEBUGGING AND MAINTENANCE NOW

# after a few more runs, an error happens
# we simulate it by injecting 5 as the run_id, which will trigger a Python error during the run
run_5 = client.run(
    project_dir='./my_project',
    ref=my_branch_name,
    cache='off',
    parameters={'run_id': 5}
)
print(f"Bauplan faulty run completed: {run_5.job_id, run_5.job_status}")
assert run_5.job_status != 'SUCCESS' and run_5.job_id is not None
# remember: by default, any run that involves a WRITE (like ours) will be automatically run
# transactionally by the system - i.e. a temporary branch is started from the current branch
# and all the intermediate artifacts are materialized there (in case the error is in model 2
# but the first one run fine).

# Let's also verify our current development branch is untouched by this failed run, as expected
assert client.query(run_id_query, ref=my_branch_name).to_pylist() == [{'run_id': 2}]

# ok, ok, after long pondering, we realized we really like our compliant dataset more
# then the current one. We will now REVERT our table to the state corresponding to the compliant version!

revert_ref = client.revert_table(
    table=my_test_table_name,
    source_ref=target_tag,
    into_branch=my_branch_name,
    # add a memo, which gets appended to the standard commit message!
    commit_body=f'Revert to tag {my_compliance_dataset_tag}',
    replace=True
)
# Q: is the revert op itself tracked?
my_branch_last_commit = client.get_commits(my_branch_name, limit=1)[0]
# A: yes, the last commit should be this op
print(f"Last commmit recorded on the branch was {my_branch_last_commit.message}")
# Q: did we really revert? 
rows = client.query(run_id_query, ref=my_branch_name).to_pylist()
# A: the HEAD on the branch is back to run_id 1
assert rows == [{'run_id': 1}]

### CLEAN-UP

# make sure we delete our branch
client.delete_branch(my_branch_name)