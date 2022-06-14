import prefect
from prefect import task, Flow, Parameter
from prefect.storage import Azure
from prefect.run_configs import DockerRun


@task
def say_hello(name):
    logger = prefect.context.get("logger")
    logger.info(f"Hello, {name}!")


with Flow("hello-flow") as flow:
    # An optional parameter "people", with a default list of names
    people = Parameter("people", default=["Arthur", "Ford", "Marvin"])
    # Map `say_hello` across the list of names
    say_hello.map(people)


flow.storage = Azure(container="rawzone", connection_string_secret="azureblobstorage", stored_as_script=True, blob_name="23542_prefect_pipelines/flows/hello_flow.py")
flow.run_config = DockerRun(labels={"test"})
flow.register(project_name="tutorial")
