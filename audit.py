import os
import requests
import json
import csv
import pandas as pd
import datetime
import logging
from joblib import Parallel, delayed
import sys
import configparser

### Vars and log settings
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
config = configparser.ConfigParser()
config.optionxform=str
config.read('report_config.ini')

project_name = os.getenv('DOMINO_PROJECT_NAME')
project_id = os.getenv('DOMINO_PROJECT_ID')
api_host = os.getenv('DOMINO_API_HOST')
api_key = os.getenv('DOMINO_USER_API_KEY')

columns_to_expand = ['stageTime', 'startedBy', 'commitDetails', 'statuses', 'environment', 'startState', 'endState']
columns_to_datetime = ['stageTime-submissionTime', 'stageTime-runStartTime', 'stageTime-completedTime']
if config.has_section('column_order'):
    column_order = config['column_order']
else:
    exit(1)
output_location = '/mnt/results'


headers = {'X-Domino-api-key': api_key}


def get_jobs(api_host, project_id):
    """
    This will return a list of all job IDs from the selected project.
    """
    endpoint = f"/v4/jobs?projectId={project_id}&page_size=1000"
    url = f"{api_host}{endpoint}"
    
    r = requests.request("GET", url, headers=headers)
    r = r.json()
    jobs = r.get("jobs", None)
    job_ids = []
    for job in jobs:
        job_ids.append(job.get("id", None))
    return job_ids


def get_project_owner(api_host, project_id):
    """
    This will return a list of all job IDs from the selected project.
    """
    url = f"{api_host}/v4/projects/{project_id}"
    
    r = requests.request("GET", url, headers=headers)
    r = r.json()
    owner_username = r.get("ownerUsername", None)

    return owner_username


def aggregate_job_data(api_host, job_ids, parallelize=True):
    jobs = {}
    if parallelize:
        def process(job_id):
            return get_job_data(api_host, job_id)
        result = Parallel(n_jobs=os.cpu_count())(delayed(process)(job_id) for job_id in job_ids)
        for job in result:
            jobs[job.get("id", None)] = job
    else:
        for job_id in job_ids:
            job = get_job_data(api_host, job_id)
            jobs[job.get("id", None)] = job
    return jobs


def get_job_data(api_host, job_id):
    endpoints = [f"/v4/jobs/{job_id}",
                 f"/v4/jobs/{job_id}/runtimeExecutionDetails",
                 f"/v4/jobs/{job_id}/comments",
                 f"/v4/jobs/job/{job_id}/artifactsInfo"]
    result = {}
    for endpoint in endpoints:
        url = f"{api_host}{endpoint}"
        r = requests.request("GET", url, headers=headers)
        r = r.json()
        if r is not None:
            result.update(r)
    return result


def get_goals(api_host, project_id):
    url = f"{api_host}/v4/projectManagement/{project_id}/goals"
    r = requests.request("GET", url, headers=headers)
    goals = r.json()
    goal_ids = {}
    for goal in goals:
        goal_ids[goal.get("id", None)] = goal.get("title", None)
    return goals
    

def generate_report(data, filename):
    df = pd.DataFrame.from_dict(data, orient='index')
    df.to_csv(filename, header=True, index=False)

    
def expand(c, job, jobs):
    for x in jobs[job][c]:
        jobs[job][f'{c}-{x}'] = jobs[job][c].get(x)
    jobs[job].pop(c)
    
    
def convert_datetime(time_str):
    return datetime.datetime.fromtimestamp(time_str / 1e3, tz=datetime.timezone.utc).strftime('%F %X:%f %Z')


def clean_comments(job):
    comments = []
    for c in job['comments']:
        comment = {
            'comment-username': c['commenter']['username'],
            'comment-timestamp': convert_datetime(c['created']),
            'comment-value': c['commentBody']['value']
        }
        comments.append(comment)
    return comments


def clean_goals(job, jobs, goals):
    goal_names = []
    if len(jobs[job]['goalIds']) > 0:
        for goal_id in jobs[job]['goalIds']:
            goal_names.append(goals[goal_id])
    return goal_names

def clean_datasets(job, jobs, datasets):
    exit(1)
    


def sorter(job, jobs, column_order):
    result = {}
    for c in column_order:
        result[c] = jobs[job].get(c)
    return result


def clean_jobs(jobs, columns_to_expand, column_order, columns_to_datetime, goals, project_name):
    for job in jobs:
        for c in (c for c in list(jobs[job]) if c in columns_to_expand):
            expand(c, job, jobs)
        for c in (c for c in jobs[job] if c in columns_to_datetime):
            jobs[job][c] = convert_datetime(jobs[job][c])
        if 'comments' in jobs[job].keys():
            jobs[job]['comments'] = clean_comments(jobs[job])
        jobs[job]['goals'] = clean_goals(job, jobs, goals)
        git_repos = []
        for repo in jobs.get(job, None).get("dependentRepositories"):
            repo_uri = repo.get("uri", None)
            git_repos.append(repo_uri)
        jobs[job]['dependentRepositories'] = git_repos
        dataset_names = []
        for dataset in jobs.get(job, None).get("dependentDatasetMounts"):
            dataset_name = dataset.get("datasetName", None)
            dataset_names.append(dataset_name)
        jobs[job]['dependantDatasets'] = dataset_names
        jobs[job]['projectName'] = project_name
        endStateCommit = jobs.get(job, None).get("endState-commitId")
        project_owner = get_project_owner(api_host, project_id)
        commit_url = f"{api_host}/u/{project_owner}/{project_name}/browse?commitId={endStateCommit}"
        audit_url = f"{api_host}/projects/{project_id}/auditLog"
        jobs[job]["endStateCommitURL"] = commit_url
        jobs[job]["auditURL"] = audit_url
        jobs[job] = sorter(job, jobs, column_order)
        jobs[job]["Username"] = jobs[job].pop("startedBy-username")
        jobs[job]["Execution Status"] = jobs[job].pop("statuses-executionStatus")
        jobs[job]["Submission Time"] = jobs[job].pop("stageTime-submissionTime")
        jobs[job]["Run StartTime"] = jobs[job].pop("stageTime-runStartTime")
        jobs[job]["Completed Time"] = jobs[job].pop("stageTime-completedTime")
        jobs[job]["Environment Name"] = jobs[job].pop("environment-environmentName")
        jobs[job]["Environment Version"] = jobs[job].pop("environment-revisionNumber")
        jobs[job]["Execution Status Completed"] = jobs[job].pop("statuses-isCompleted")
        jobs[job]["Execution Status Archived"] = jobs[job].pop("statuses-isArchived")
        jobs[job]["Execution Status Scheduled"] = jobs[job].pop("statuses-isScheduled")
    return jobs


def main():
    ### starting timer
    t0 = datetime.datetime.now()

    logging.info(f"Generating audit report for {project_name}...")
    logging.info("Generating list of project IDs for report...")
    job_ids = get_jobs(api_host, project_id)
    goals = get_goals(api_host, project_id)
    logging.info(f"Found {len(job_ids)} jobs to report. Aggregating job metadata...")

    # ### extract available metadata from each job 
    try:
        logging.info(f"Attempting parallelized API queries...")
        t = datetime.datetime.now()
        jobs_raw = aggregate_job_data(api_host, job_ids, parallelize=True)
        t = datetime.datetime.now() - t 
        logging.info(f"Queries succeeded in {str(round(t.total_seconds(),1))} seconds.")
    except:
        logging.info(f"Parallel queries failed, attempting single-threaded API queries...")
        t = datetime.datetime.now()
        jobs_raw = aggregate_job_data(api_host, job_ids, parallelize=False)
        t = datetime.datetime.now() - t
        logging.info(f"Queries succeeded in {str(round(t.total_seconds(),1))} seconds.")


    ### clean up data, merge with additional sources 
    logging.info(f"Cleaning data...")
    jobs = clean_jobs(jobs_raw, columns_to_expand, column_order, columns_to_datetime, goals, project_name)


    ### write list of jobs to csv
    filename = f"{output_location}/{project_name}_audit_report_{datetime.datetime.now(tz=datetime.timezone.utc).strftime('%Y-%m-%d_%X%Z')}.csv"
    logging.info(f"Saving report to: {filename}")
    generate_report(jobs, filename)

    ### timing info
    t = datetime.datetime.now() - t0
    logging.debug(f"Audit report generated in {str(round(t.total_seconds(),1))} seconds.")

if __name__ == '__main__':
    main()
