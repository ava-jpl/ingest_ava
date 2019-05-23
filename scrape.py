#!/usr/bin/env python

'''
scrapes the current AVA and submits jobs for 09T or L1B products
'''
from __future__ import print_function
import os
import json
import math
import shutil
import urllib3
import dateutil.parser
import requests
from hysds.celery import app
from hysds.dataset_ingest import ingest
import hysds.orchestrator

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

VERSION = "v1.0"
PROD = "MET-{}-{}-{}"
PROD_TYPE = "grq_{}_metadata-{}"
CMR_URL = 'https://cmr.earthdata.nasa.gov/search/granules.json?granule_ur={}&provider-id=LPDAAC_ECS'
AVA_URL = 'https://ava.jpl.nasa.gov/retrieve/list_{}.php'

def main():
    '''
    Scrapes the AVA for 09T or L1B, then ingests the metadata for those products, allowing for future ingest.
    '''
    # load parameters
    ctx = load_context()
    shortname = ctx.get("short_name", False)
    if not shortname:
        raise Exception("short_name must be specified.")

    #query the ava
    print('querying the AVA for {} products...'.format(shortname))
    ava_url = AVA_URL.format(shortname)
    #ave returns a very simple json
    response = requests.get(ava_url, timeout=450)
    response.raise_for_status()
    ava_gran_dct = json.loads(response.text)
    print('ava returned {} items.'.format(len(ava_gran_dct.keys())))

    #for each item, see if it's been ingested. if it has query the CMR and get the metadata
    for granule_ur, product_url in ava_gran_dct.items():
        cmr_url = CMR_URL.format(granule_ur)
        response = requests.get(cmr_url, timeout=10)
        response.raise_for_status()
        granule = json.loads(response.text)["feed"]["entry"][0]
        granule['ava_url'] = product_url
        granule['on_ava'] = True
        granule['short_name'] = shortname
        ds, met = gen_product(granule, shortname)
        uid = ds.get('label')
        print('ingesting: {}'.format(uid))
        if exists(uid, shortname):
            continue
        #save_product_met(uid, ds, met)
        ingest_product(uid, ds, met)

def gen_temporal_str(starttime, endtime):
    '''generates the temporal string for the cmr query'''
    start_str = ''
    end_str = ''
    if starttime:
        start_str = dateutil.parser.parse(starttime).strftime('%Y-%m-%dT%H:%M:%SZ')
    if endtime:
        end_str = dateutil.parser.parse(endtime).strftime('%Y-%m-%dT%H:%M:%SZ')
    # build query
    temporal_span = ''
    if starttime or endtime:
        temporal_span = '&temporal={0},{1}'.format(start_str, end_str)
    return temporal_span

def gen_spatial_str(location):
    '''generates the spatial string for the cmr query'''
    if not location:
        return ''
    coords = location['coordinates'][0]
    if get_area(coords) > 0: #reverse orde1r if not clockwise
        coords = coords[::-1]
    coord_str = ','.join([','.join([format_digit(x) for x in c]) for c in coords])
    return '&polygon={}'.format(coord_str)
    
def get_area(coords):
    '''get area of enclosed coordinates- determines clockwise or counterclockwise order'''
    n = len(coords) # of corners
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        area += coords[i][1] * coords[j][0]
        area -= coords[j][1] * coords[i][0]
    #area = abs(area) / 2.0
    return area / 2

def format_digit(digit):
    return "{0:.8g}".format(digit)

def gen_product(result, shortname):
    '''generates a dataset.json and met.json dict for the product'''
    starttime = result["time_start"]
    dt_str = dateutil.parser.parse(starttime).strftime('%Y%m%d')
    endtime = result["time_end"]
    location = parse_location(result)
    prod_id = gen_prod_id(shortname, starttime, endtime)
    ds = {"label": prod_id, "starttime": starttime, "endtime": endtime, "location": location, "version": VERSION}
    met = result
    met['shortname'] = shortname
    met['short_name'] = shortname
    return ds, met

def gen_prod_id(shortname, starttime, endtime):
    '''generates the product id from the input metadata & params'''
    start = dateutil.parser.parse(starttime).strftime('%Y%m%dT%H%M%S')
    end = dateutil.parser.parse(endtime).strftime('%Y%m%dT%H%M%S')
    time_str = '{}_{}'.format(start, end)
    return PROD.format(shortname, time_str, VERSION)

def ingest_product(uid, ds, met):
    '''publish a product directly'''
    shortname = met.get('short_name', False)
    save_product_met(uid, ds, met)
    ds_dir = os.path.join(os.getcwd(), uid)
    if exists(uid, shortname):
        print('Product already exists with uid: {}. Passing on publish...'.format(uid))
        return
    print('Product with uid: {} does not exist. Publishing...'.format(uid))
    try:
        ingest(uid, './datasets.json', app.conf.GRQ_UPDATE_URL, app.conf.DATASET_PROCESSED_QUEUE, ds_dir, None) 
        if os.path.exists(uid):
            shutil.rmtree(uid)
    except:
        raise Exception('failed on submission of {0}'.format(uid))

def parse_location(result):
    '''parse out the geojson from the CMR return'''
    poly = result["polygons"][0][0]
    coord_list = poly.split(' ')
    coords = [[[float(coord_list[i+1]), float(coord_list[i])] for i in range(0, len(coord_list), 2)]]
    location = {"type": "Polygon", "coordinates": coords}
    return location

def get_session(verbose=False):
    '''returns a CMR requests session'''
    #user = os.getenv('CMR_USERNAME')
    #passwd = os.getenv('CMR_PASSWORD')
    #if user is None or passwd is None:
    #    if verbose > 1:
    #        print ("Environment username & password not found")
    return requests.Session()
    #token_url = os.path.join(cmr_url, 'legacy-services/rest/tokens')
    #if verbose: print('token_url: %s' % token_url)
    #headers = {'content-type': 'application/json'}
    #info = {'token': {'username': user, 'password': passwd, 'client_id': user, 'user_ip_address': '127.0.0.1'}}
    #data = json.dumps(info, indent=2)
    #if verbose: print(data)
    #session = requests.Session()
    #r = session.post(token_url, data=data, headers=headers)
    #if verbose: print(r.text)
    #r.raise_for_status()
    #robj = xml.etree.ElementTree.fromstring(r.text)
    #token = robj.find('id').text
    #if verbose: print("using session token:".format(token))
    #return session

def run_query(query_url, verbose=False):
    """runs a scrolling query over the given url and returns the result as a dictionary"""
    if verbose:
        print('querying url: {0}'.format(query_url))
    granule_list = []
    session = get_session(verbose=verbose)
    #initial query
    response = session.get(query_url)
    response.raise_for_status()
    granule_list.extend(json.loads(response.text)["feed"]["entry"])
    #get headers for scrolling
    tot_granules = response.headers["CMR-Hits"]
    scroll_id = response.headers["CMR-Scroll-Id"]
    headers = {'CMR-Scroll-Id' : scroll_id}
    if len(granule_list) is 0:
        if verbose > 0:
            print('no granules returned')
        return []
    pages = int(math.ceil(float(tot_granules) / len(granule_list)))
    if verbose > 1:
        print("total granules matching query: {0}".format(tot_granules))
        print("Over {0} pages".format(pages))
        print("Using scroll-id: {0}".format(scroll_id))
    if verbose > 2:
        print("response text: {0}".format(response.text))
        print("response headers: {0}".format(response.headers))
    for i in range(1, pages):
        if verbose > 1:
            print("querying page {0}".format(i+1))
        response = session.get(query_url, headers=headers)
        response.raise_for_status()
        granule_returns = json.loads(response.text)["feed"]["entry"]
        if verbose > 1:
            print("query returned {0} granules".format(len(granule_returns)))
        if verbose > 2:
            print("response text: {0}".format(response.text))
            print("response headers: {0}".format(response.headers))
            print('with {0} granules'.format(len(granule_returns)))
        granule_list.extend(granule_returns)
    #text = json.dumps(granule_list, sort_keys=True, indent=4, separators=(',', ': '))
    if verbose:
        print("query returned {0} total granules".format(len(granule_list)))
    if len(granule_list) != int(tot_granules):
        raise Exception("Total granules returned from query do not match expected granule count")
    return granule_list

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


def exists(uid, shortname):
    '''queries grq to see if the input id exists. Returns True if it does, False if not'''
    grq_ip = app.conf['GRQ_ES_URL']#.replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/{1}/_search'.format(grq_ip, PROD_TYPE.format(VERSION, shortname))
    es_query = {"query":{"bool":{"must":[{"term":{"id.raw":uid}}]}},"from":0,"size":1}
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
