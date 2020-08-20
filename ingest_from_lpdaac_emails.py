#!/usr/bin/env python

import os
import re
import time
import json
import argparse
import requests
import logging
from submit_job import main as submit_job
from hysds.celery import app

# create order_granule.log
LOG_FILE_NAME = 'ingest_from_lpdaac_emails.log'
logging.basicConfig(filename=LOG_FILE_NAME,
                    filemode='a', level=logging.INFO)
logger = logging

# ingest lpdaac prod job specs
JOB_NAME = "job-ingest_lpdaac_prod"
JOB_VERSION = "dev"
QUEUE = "factotum-job_worker-small"
PRIORITY = 5
TAG = "{}-ingest_from_lpdaac-id-{}"
PARAMS = {"lpdaac_download_url": ""}


def main(args):
    '''Localizes and ingests product from input metadata blob'''
    # get list of all emails in directory
    emails = import_lpdaac_emails(args)
    # loop through the emails
    logger.info("ORDER_ID, LPDAAC_DOWNLOAD_LINK")
    for email in emails:
        # get order id and lpdaac download link
        order = scrape_emails(email)
        if order:
            order_id = order[0]
            # if there is a job queued or job completed with order id, continue to the next email
            if query_es(order_id) != 0:
                continue
            # else, submit ingest_lpdaac_prod job with order_id and lpdaac download link.
            else:
                lpdaac_download_link = order[1]
                logger.info("{}, {}".format(order_id, lpdaac_download_link))
                tag = TAG.format(time.strftime('%Y%m%d'), order_id)
                params = PARAMS['lpdaac_download_url'] = lpdaac_download_link
                submit_job(JOB_NAME, params, JOB_VERSION, QUEUE, PRIORITY, tag)


def import_lpdaac_emails(args):
    '''Import AST HDF met file from LPDAAC as dictionary'''
    emails = []

    # check for a given directory
    if (args.dir):
        directory = args.dir
    else:
        # load parameters
        ctx = load_context()
        directory = ctx.get("lpdaac_email_directory", False)
        # check if lpdaac_download_url has a trailing "/" character
        if directory[-1] != "/":
            directory = "{}{}".format(directory, "/")
        print("lpdaac_email_directory: {}".format(directory))

    # from directory, return list of all emails
    files = os.listdir(directory)
    for f in files:
        email_file_path = os.path.join(directory, f)
        if not os.path.isfile(email_file_path):
            raise RuntimeError(
                "Failed to find docs file {}".format(email_file_path))
        else:
            emails.append(email_file_path)
    return emails


def scrape_emails(email_file_dir):
    '''Extract Latitiude and Longitiude vectors from met file'''
    parsed_lines = []
    order_id = False
    download_link_index = False
    try:
        with open(email_file_dir, encoding='utf-8',
                  errors='ignore') as f:
            for i, line in enumerate(f):
                parsed_lines.append(line.strip())
                if order_id and download_link_index:  # if order_id and download_link_index are true, exit function
                    order = [order_id, parsed_lines[download_link_index]]
                    print(order)
                    f.close()
                    return order
                if "ORDERID" in line:  # extract order ID
                    order_id = ''.join(list(filter(str.isdigit, line)))
                if "Download Links" in line:  # extract LPDAAC download link
                    download_link_index = i+1
    except:
        print("could not find ORDERID and Download Links in email: {}".format(
            email_file_dir))


def query_es(uid):
    '''simple single elasticsearch query, used for existence. returns count of result.'''
    mozart_ip = app.conf['JOBS_ES_URL']
    idx = "job_status-current"
    mozart_url = '{0}/{1}/_search'.format(mozart_ip, idx)
    es_query = {"query": {"bool": {"must": [{"query_string": {"default_field": "_all", "query": uid}}], "must_not": [
        {"query_string": {"default_field": "status", "query": "job-failed"}}], "should": []}}, "from": 0, "size": 10, "sort": [], "aggs": {}}
    print('querying: {} with {}'.format(mozart_url, es_query))
    response = requests.post(
        mozart_url, data=json.dumps(es_query), verify=False)
    try:
        response.raise_for_status()
    except:
        # if there is an error (or 404,just publish
        return 0
    results = json.loads(response.text, encoding='ascii')
    #results_list = results.get('hits', {}).get('hits', [])
    total_count = results.get('hits', {}).get('total', 0)
    return int(total_count)


def load_context():
    '''loads the context file into a dict'''
    try:
        context_file = '_context.json'
        with open(context_file, 'r') as fin:
            context = json.load(fin)
        return context
    except:
        raise Exception('unable to parse _context.json from work directory')


if __name__ == "__main__":
    # Gather arguements for script
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--dir", required=False,
                        help="name of the directory containing LPDAAC emails")
    args = parser.parse_args()

    # run main funciton
    main(args)
