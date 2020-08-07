#!/usr/bin/env python

'''
Ingests Product from the existing on-prem AVA cluster
'''

from __future__ import print_function
import os
import glob
import json
import subprocess
import urllib3
import dateutil.parser
import requests
from hysds.celery import app

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

VERSION = "v1.0"
ALLOWED_EXTENSIONS = ['tif', 'jpg', 'jpeg', 'png']
# determined globals
PROD = "{}-{}-{}"  # eg: AST_L1T-20190514T341405_20190514T341435-v1.0
INDEX = 'grq_{}_{}'  # e.g. grq_v1.0_ast_09t
INDEX_METADATA = 'grq_{}_metadata-{}'  # e.g. grq_v1.0_metadata-ast_09t


def main():
    '''Localizes and ingests product from input metadata blob'''
    # load parameters
    ctx = load_context()
    lpdaac_download_url = ctx.get("lpdaac_download_url", False)
    print("lpdaac_download_url: {}".format(lpdaac_download_url))

    # download granules from lpdaac_download_url
    granule_download_dir = localize_granules(lpdaac_download_url)
    print("granule_download_dir: {}".format(granule_download_dir))

    # get list of granules from lpdaac_download_url
    granule_ids = list_granules(granule_download_dir)
    print("granule_ids: {}".format(granule_ids))

    # query metadata in AVA based on version, acquisition_date, and short_name
    for id in granule_ids:
        id_items = id.split('_')
        short_name = "{}_{}".format(id_items[0], id_items[1])
        version_acquisition_date = id_items[2]
        idx = INDEX.format(VERSION, short_name.lower())
        if exists(idx, version_acquisition_date, short_name):
            print("granule ID {} already exists in AVA".format(id))
            continue
        else:
            idx = INDEX_METADATA.format(VERSION, short_name.lower())
            if exists(idx, version_acquisition_date, short_name):
                granule_results = query_es_metdata(idx, version_acquisition_date, short_name)
                # generate product id
                granule_metadata_id = granule_results['_id']
                prod_id = gen_prod_id(granule_metadata_id)
                # attempt to localize product
                hdf_items = id.split('.')
                granule_hdf = "{}.{}".format(hdf_items[0], hdf_items[1])
                metadata = granule_results.get("_source",False).get("metadata", False)
                localize_product(lpdaac_download_url, granule_hdf, prod_id, metadata)
                # generate product
                granule_metadata_source = granule_results.get("_source",False)
                dst, met = gen_jsons(prod_id, granule_metadata_source)
                # save the metadata files
                save_product_met(prod_id, dst, met)


# def ingest_product(shortname, starttime, endtime, location, metadata):
#     '''determines if the product is localized. if not localizes and ingests the product'''
#     # generate product id
#     prod_id = gen_prod_id(shortname, starttime, endtime)
#     # determine if product exists on grq
#     if exists(prod_id, shortname):
#         print('product with id: {} already exists. Exiting.'.format(prod_id))
#         return
#     # attempt to localize product
#     print('attempting to localize product: {}'.format(prod_id))
#     localize_product(prod_id, metadata)
#     # generate product
#     dst, met = gen_jsons(prod_id, starttime, endtime, location, metadata)
#     # save the metadata files
#     save_product_met(prod_id, dst, met)


def gen_prod_id(id):
    '''generates the product id from the input metadata & params'''
    id_items = id.split('-')
    short_name = id_items[1]
    start_end = id_items[2]
    return PROD.format(short_name, start_end, VERSION)


def exists(idx, uid, short_name):
    '''queries grq to see if the input id exists. Returns True if it does, False if not'''
    # idx = INDEX.format(VERSION, short_name.lower())
    # .replace(':9200', '').replace('http://', 'https://')
    grq_ip = app.conf['GRQ_ES_URL']
    grq_url = '{0}/{1}/_search'.format(grq_ip, idx)
    es_query = {"query":{"bool":{"must":[{"query_string":{"default_field":"_all","query":uid}},{"query_string":{"default_field":"metadata.short_name.raw","query":short_name}}],"must_not":[],"should":[]}},"from":0,"size":1,"sort":[],"aggs":{}}
    return query_es(grq_url, es_query)


def query_es(grq_url, es_query):
    '''simple single elasticsearch query, used for existence. returns count of result.'''
    print('querying: {} with {}'.format(grq_url, es_query))
    response = requests.post(grq_url, data=json.dumps(es_query), verify=False)
    try:
        response.raise_for_status()
    except:
        # if there is an error (or 404,just publish
        return 0
    results = json.loads(response.text, encoding='ascii')
    #results_list = results.get('hits', {}).get('hits', [])
    total_count = results.get('hits', {}).get('total', 0)
    return int(total_count)

def query_es_metdata(idx, uid, short_name):
    '''simple single elasticsearch query, used for existence. returns count of result.'''
    grq_ip = app.conf['GRQ_ES_URL']
    grq_url = '{0}/{1}/_search'.format(grq_ip, idx)
    es_query = {"query":{"bool":{"must":[{"query_string":{"default_field":"_all","query":uid}},{"query_string":{"default_field":"metadata.short_name.raw","query":short_name}}],"must_not":[],"should":[]}},"from":0,"size":1,"sort":[],"aggs":{}}
    print('querying: {} with {}'.format(grq_url, es_query))
    response = requests.post(grq_url, data=json.dumps(es_query), verify=False)
    try:
        response.raise_for_status()
    except:
        # if there is an error (or 404,just publish
        return 0
    results = json.loads(response.text, encoding='ascii')
    results_list = results.get('hits', {}).get('hits', [])
    return results_list[0]


def localize_product(lpdaac_download_url, granule_hdf, prod_id, metadata):
    '''attempts to localize the product'''
    if not os.path.exists(prod_id):
        os.mkdir(prod_id)
    ava_url = metadata.get('ava_url', False)
    if ava_url is False:
        # get granule hdf from lpdaac url
        ava_url = "{}{}".format(lpdaac_download_url, granule_hdf)
        prod_path = os.path.join(prod_id, granule_hdf)
        localize_file(ava_url, prod_path)
    else:
        # get granule hdf from ava
        prod_path = os.path.join(prod_id, '{}.{}'.format(prod_id, 'hdf'))
        localize_file(ava_url, prod_path)
    for obj in metadata.get('links', []):
        # localize links from extensions
        url = obj.get('href', False)
        extension = os.path.splitext(url)[1].strip('.')
        if extension in ALLOWED_EXTENSIONS:
            product_path = os.path.join(
                prod_id, '{}.{}'.format(prod_id, extension))
            if not os.path.exists(product_path):
                localize_file(url, product_path)
        if extension in ['jpg', 'jpeg', 'png']:
            # attempt to generate browse
            generate_browse(product_path, prod_id)


def localize_granules(url):
    '''attempts to localize the product'''
    wd = os.getcwd()
    granule_download_dir = os.path.join(wd, "Downloads")
    cmd = ['wget', '-r', '-np', '-nd', '-A',
           '.met', url, '-P', granule_download_dir]
    status = subprocess.call(cmd)
    # status = os.system(
    #     'wget -r -np -nd -A .met {} -P {}'.format(url, granule_download_dir))
    if status == 0:
        # succeeds
        if os.path.exists(granule_download_dir):
            print("localized products from url: {} to {}".format(url, granule_download_dir))
            return granule_download_dir
    raise Exception("unable to localize products from url: {} to {}".format(url, granule_download_dir))

def localize_file(url, prod_path):
    '''attempts to localize the product'''
    cmd  = ['wget', '--no-check-certificate', '-O', prod_path, url]
    status = subprocess.call(cmd)
    # status = os.system('wget --no-check-certificate -O {} {}'.format(prod_path, url))
    if status == 0:
        #succeeds
        if os.path.exists(prod_path):
            print("localized products from url: {} to {}".format(url, prod_path))
            return
    raise Exception("unable to localize products from url: {} to {}".format(url, prod_path))


def list_granules(granule_download_dir):
    granule_ids = []
    try:
        for file in os.listdir(granule_download_dir):
            if file.endswith(".hdf.met"):
                granule_ids.append(file)
        return granule_ids
    except:
        raise Exception(
            "Could not get list of granule_ids from {}".format(granule_download_dir))


def generate_browse(product_path, prod_id):
    '''attempts to generate browse if it doesn't already exist'''
    browse_path = os.path.join(prod_id, '{}.browse.png'.format(prod_id))
    browse_small_path = os.path.join(
        prod_id, '{}.browse_small.png'.format(prod_id))
    if os.path.exists(browse_path):
        return
    # conver to png
    os.system("convert {} {}".format(product_path, browse_path))
    # convert to small png
    os.system(
        "convert {} -resize 300x300 {}".format(product_path, browse_small_path))
    os.remove(product_path)


def gen_jsons(prod_id, metadata):
    '''generates ds and met json blobs'''
    starttime = metadata.get('starttime', False)
    endtime = metadata.get('endtime', False)
    location = metadata.get('location', False)
    shortname = metadata.get('shortname', False)
    ds = {"label": prod_id, "starttime": starttime,
          "endtime": endtime, "location": location, "version": VERSION}
    met = metadata.get('metadata', False)
    return ds, met


def save_product_met(prod_id, ds_obj, met_obj):
    '''generates the appropriate product json files in the product directory'''
    if not os.path.exists(prod_id):
        os.mkdir(prod_id)
    outpath = os.path.join(prod_id, '{}.dataset.json'.format(prod_id))
    with open(outpath, 'w') as outf:
        json.dump(ds_obj, outf)
    outpath = os.path.join(prod_id, '{}.met.json'.format(prod_id))
    with open(outpath, 'w') as outf:
        json.dump(met_obj, outf)


def load_context():
    '''loads the context file into a dict'''
    try:
        context_file = '_context.json'
        with open(context_file, 'r') as fin:
            context = json.load(fin)
        return context
    except:
        raise Exception('unable to parse _context.json from work directory')


if __name__ == '__main__':
    main()
