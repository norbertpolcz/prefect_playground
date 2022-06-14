from random import random

from prefect import task, Flow, case
from prefect.tasks.control_flow import merge
from prefect.storage import Azure
from prefect.run_configs import DockerRun


# define tasks
@task
def check_condition():
    return random() < .5

@task
def action_if_true():
    return "I am true!"

@task
def action_if_false():
    return "I am false!"

@task
def another_action(val):
    print(val)


# create conditional flow
with Flow("conditional-branches") as flow:
    cond = check_condition()

    # if condition is true
    with case(cond, True):
        val1 = action_if_true()

    # if condition is false
    with case(cond, False):
        val2 = action_if_false()

    val = merge(val1, val2)

    another_action(val)



flow.storage = Azure(container="rawzone", connection_string_secret="azureblobstorage", stored_as_script=True, blob_name="23542_prefect_pipelines/flows/conditional_flow.py")
flow.run_config = DockerRun(labels={"test"})
flow.register(project_name="tutorial")
