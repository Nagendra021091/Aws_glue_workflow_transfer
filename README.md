# Aws_glue_workflow_transfer


1.Replace the WORKFLOW_NAME in the script with the name of the workflow you want to transfer.
2.Run the script:

python transfer_glue_workflow.py



# What the Script Does

1.Exports:
Workflow metadata.
Associated Glue jobs, crawlers, and triggers.

2.Imports:
Recreates the workflow, jobs, crawlers, and triggers in the destination account.

3.Backup: Saves the exported data in a JSON file for reference or manual restoration.
