#!/usr/bin/env python

'''
Ingests Product from the existing on-prem AVA cluster
'''

from __future__ import print_function
import os
import json
import urllib3
import dateutil.parser
import requests
from hysds.celery import app

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

VERSION = "v1.0"
ALLOWED_EXTENSIONS = ['tif', 'jpg', 'jpeg', 'png']
# determined globals
PROD = "{}-{}-{}" # eg: AST_L1T-20190514T341405_20190514T341435-v1.0
INDEX = 'grq_{}_{}'

def main():
    '''Localizes and ingests product from input metadata blob'''
    # load parameters
    ctx = load_context()
    metadata = ctx.get("metadata", False)
    on_ava = ctx.get("on_ava", False)
    if not on_ava:
        raise Exception("Product is not on the AVA. Cannot localize.")
    ava_url = ctx.get("ava_url", False)
    starttime = ctx.get("starttime", False)
    endtime = ctx.get("endtime", False)
    location = ctx.get("location", False)
    shortname = metadata.get('short_name')
    #ingest the product
    ingest_product(shortname, starttime, endtime, location, metadata)

def ingest_product(shortname, starttime, endtime, location, metadata):
    '''determines if the product is localized. if not localizes and ingests the product'''
    # generate product id
    prod_id = gen_prod_id(shortname, starttime, endtime)
    # determine if product exists on grq
    if exists(prod_id, shortname):
        print('product with id: {} already exists. Exiting.'.format(prod_id))
        return
    #attempt to localize product
    print('attempting to localize product: {}'.format(prod_id))
    localize_product(prod_id, metadata)
    # generate product
    dst, met = gen_jsons(prod_id, starttime, endtime, location, metadata)
    # save the metadata fo;es
    save_product_met(prod_id, dst, met)

def gen_prod_id(shortname, starttime, endtime):
    '''generates the product id from the input metadata & params'''
    start = dateutil.parser.parse(starttime).strftime('%Y%m%dT%H%M%S')
    end = dateutil.parser.parse(endtime).strftime('%Y%m%dT%H%M%S')
    time_str = '{}_{}'.format(start, end)
    return PROD.format(shortname, time_str, VERSION)

def exists(uid, shortname):
    '''queries grq to see if the input id exists. Returns True if it does, False if not'''
    idx = INDEX.format(VERSION, shortname)
    grq_ip = app.conf['GRQ_ES_URL']#.replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/{1}/_search'.format(grq_ip, idx)
    es_query = {"query": {"bool": {"must": [{"term": {"id.raw": uid}}]}}, "from": 0, "size": 1}
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

def localize_product(prod_id, metadata):
    '''attempts to localize the product'''
    if not os.path.exists(prod_id):
        os.mkdir(prod_id)
    ava_url = metadata.get('ava_url', False)
    if ava_url is False:
        raise Exception('cannot localize product. metadata.ava_url parameter is empty')
    product_path = os.path.join(prod_id, '{}.{}'.format(prod_id, 'hdf'))
    localize(ava_url, prod_path)
    for obj in metadata.get('links', []):
        url = obj.get('href', False)
        extension = os.path.splitext(url)[1].strip('.')
        if extension in ALLOWED_EXTENSIONS:
            product_path = os.path.join(prod_id, '{}.{}'.format(prod_id, extension))
            if not os.path.exists(product_path):
                localize(url, product_path)
        if extension in ['jpg', 'jpeg', 'png']:
            #attempt to generate browse
            generate_browse(product_path, prod_id)

def localize(url, prod_path):
    '''attempts to localize the product'''
    status = os.system('wget -O {} {}'.format(prod_path, url))
    if status == 0:
        #succeeds
        if os.path.exists(prod_path):
            return
    raise Exception("unable to localize product from url: {}".format(url))

def generate_browse(product_path, prod_id):
    '''attempts to generate browse if it doesn't already exist'''
    browse_path = os.path.join(prod_id, '{}.browse.png'.format(prod_id))
    browse_small_path = os.path.join(prod_id, '{}.browse_small.png'.format(prod_id))
    if os.path.exists(browse_path):
        return
    #conver to png
    os.system("convert {} {}".format(product_path, browse_path))
    #convert to small png
    os.system("convert {} -resize 300x300 {}".format(product_path, browse_small_path))
    os.remove(product_path)

def gen_jsons(prod_id, starttime, endtime, location, metadata):
    '''generates ds and met json blobs'''
    ds = {"label": prod_id, "starttime": starttime, "endtime": endtime, "location": location, "version": VERSION}
    met = metadata
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
