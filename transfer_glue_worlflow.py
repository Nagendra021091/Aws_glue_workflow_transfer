import boto3
import json

# Define AWS regions and profiles
SOURCE_PROFILE = "source-account"
DESTINATION_PROFILE = "destination-account"
AWS_REGION = "us-east-1"

# Initialize sessions for source and destination accounts
source_session = boto3.Session(profile_name=SOURCE_PROFILE, region_name=AWS_REGION)
destination_session = boto3.Session(profile_name=DESTINATION_PROFILE, region_name=AWS_REGION)

source_glue = source_session.client("glue")
destination_glue = destination_session.client("glue")

def export_workflow(workflow_name):
    """Export workflow and its related resources."""
    print(f"Exporting workflow: {workflow_name}")
    workflow = source_glue.get_workflow(Name=workflow_name, IncludeGraph=True)
    workflow_graph = workflow["Workflow"]["Graph"]

    # Export jobs
    jobs = {}
    for node in workflow_graph.get("Nodes", []):
        if node["Type"] == "JOB":
            job_name = node["Name"]
            jobs[job_name] = source_glue.get_job(JobName=job_name)["Job"]

    # Export crawlers
    crawlers = {}
    for node in workflow_graph.get("Nodes", []):
        if node["Type"] == "CRAWLER":
            crawler_name = node["Name"]
            crawlers[crawler_name] = source_glue.get_crawler(Name=crawler_name)["Crawler"]

    # Export triggers
    triggers = {}
    for trigger in workflow_graph.get("Edges", []):
        trigger_name = trigger["Id"]
        triggers[trigger_name] = source_glue.get_trigger(Name=trigger_name)["Trigger"]

    # Save everything into a dictionary
    return {
        "workflow": workflow,
        "jobs": jobs,
        "crawlers": crawlers,
        "triggers": triggers,
    }

def import_workflow(exported_data):
    """Import workflow and its related resources into the destination account."""
    print(f"Importing workflow: {exported_data['workflow']['Workflow']['Name']}")

    # Create workflow
    workflow_name = exported_data["workflow"]["Workflow"]["Name"]
    destination_glue.create_workflow(
        Name=workflow_name,
        Description=exported_data["workflow"]["Workflow"].get("Description", ""),
        DefaultRunProperties=exported_data["workflow"]["Workflow"].get("DefaultRunProperties", {}),
    )

    # Import jobs
    for job_name, job_data in exported_data["jobs"].items():
        print(f"Creating job: {job_name}")
        job_data["Job"].pop("Name", None)
        job_data["Job"].pop("CreatedOn", None)
        job_data["Job"].pop("LastModifiedOn", None)
        destination_glue.create_job(Name=job_name, **job_data["Job"])

    # Import crawlers
    for crawler_name, crawler_data in exported_data["crawlers"].items():
        print(f"Creating crawler: {crawler_name}")
        crawler_data["Crawler"].pop("Name", None)
        crawler_data["Crawler"].pop("CreationTime", None)
        crawler_data["Crawler"].pop("LastUpdated", None)
        crawler_data["Crawler"].pop("LastCrawl", None)
        destination_glue.create_crawler(Name=crawler_name, **crawler_data["Crawler"])

    # Import triggers
    for trigger_name, trigger_data in exported_data["triggers"].items():
        print(f"Creating trigger: {trigger_name}")
        trigger_data["Trigger"].pop("Name", None)
        trigger_data["Trigger"].pop("CreationTime", None)
        trigger_data["Trigger"].pop("LastModifiedTime", None)
        destination_glue.create_trigger(Name=trigger_name, **trigger_data["Trigger"])

    print("Workflow and resources imported successfully!")

def main():
    # Name of the workflow to transfer
    WORKFLOW_NAME = "my-workflow"

    # Export workflow and its related resources
    exported_data = export_workflow(WORKFLOW_NAME)

    # Save exported data to JSON for backup (optional)
    with open(f"{WORKFLOW_NAME}_export.json", "w") as f:
        json.dump(exported_data, f, default=str)

    # Import workflow into the destination account
    import_workflow(exported_data)

if __name__ == "__main__":
    main()
