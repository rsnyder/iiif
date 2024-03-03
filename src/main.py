#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

logging.basicConfig(format='%(asctime)s : %(filename)s : %(levelname)s : %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

import argparse, os, sys, json
from hashlib import sha256
from time import time as now

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(SCRIPT_DIR)
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.responses import Response
from fastapi.middleware.cors import CORSMiddleware

from starlette.responses import RedirectResponse

from prezi_upgrader import Upgrader
from manifest import generate as get_manifest

import requests
logging.getLogger('requests').setLevel(logging.WARNING)

from gh import gh_dir_list

app = FastAPI(title='IIIF Presentation API', root_path='/')

app.add_middleware(
  CORSMiddleware,
  allow_origins=['*'],
  allow_methods=['*'],
  allow_headers=['*'],
  allow_credentials=True,
)

IMAGE_SERVICE_BASEURL = 'https://bxw3h77njs6t5nf7bo2vykqxvi0lzkxb.lambda-url.us-east-1.on.aws'

from s3 import Bucket as Cache
manifest_cache = Cache(bucket='mdpress-manifests')

def _find_item(obj, type, attr=None, attr_val=None, sub_attr=None):
  if 'items' in obj and isinstance(obj['items'], list):
    for item in obj['items']:
      if item.get('type') == type and (attr is None or item.get(attr) == attr_val):
          return item[sub_attr] if sub_attr else item
      return _find_item(item, type, attr, attr_val, sub_attr)

def _update_image_service(manifest):
  image_data = _find_item(manifest, type='Annotation', attr='motivation', attr_val='painting', sub_attr='body')
  image_service = image_data['service'][0]
  image_hash = image_service['id'].split('/')[-1]
  image_service['id'] = f'{IMAGE_SERVICE_BASEURL}/iiif/3/{image_hash}'
  manifest['thumbnail'][0]['id'] =  f'{IMAGE_SERVICE_BASEURL}/iiif/3/{image_hash}/full/400,/0/default.jpg'
  return manifest

def _manifestid_to_url(manifestid):
  if manifestid.startswith('gh:'):
    acct, repo, *path = manifestid[3:].split('/')
    return f'https://raw.githubusercontent.com/{acct}/{repo}/main/{"/".join(path)}'
  elif manifestid.startswith('wc:'):
    return manifestid

@app.get('/')
def docs():
  return RedirectResponse(url='/docs')

@app.get('{manifestid:path}/manifest.json')
async def manifest(manifestid: str, refresh: Optional[str] = None):
  start = now()
  refresh = refresh in ('', 'true')
  url = _manifestid_to_url(manifestid)
  imageid = sha256(url.encode('utf-8')).hexdigest()
  manifest = json.loads(manifest_cache.get(imageid) or 'null') if not refresh else None
  cached = manifest is not None
  if not manifest:
    manifest = get_manifest(manifestid=manifestid, refresh=refresh)
    manifest_cache[imageid] = json.dumps(manifest)
  logger.info(f'manifest: manifestid={manifestid} cached={cached} refresh={refresh} elapsed={round(now()-start,3)}')
  return _update_image_service(manifest)

@app.get('{manifestid:path}')
async def image_viewer(manifestid: str):
  viewer_html = open(f'{SCRIPT_DIR}/index.html', 'r').read()
  if manifestid.startswith('gh:'):
    acct, repo, *path = manifestid[3:].split('/')
    path = '/'.join(path)
    if '.' not in path:
      images_list_el = '<ul id="gh-images" style="display:none;">\n'
      for img in [f'{manifestid}/{fname}' for fname in gh_dir_list(acct, repo, path) if fname.split('.')[-1] in ('jpg', 'jpeg', 'png', 'tif', 'tiff')]:
        images_list_el += f'  <li>{img}</li>\n'
      images_list_el += '</ul>'
      viewer_html = viewer_html.replace('<mdp-', f'    {images_list_el}\n    <mdp-')
      viewer_html = viewer_html.replace('src=""', 'data="gh-images"')
  else:
    viewer_html = viewer_html.replace('src=""', f'src="{manifestid}"')
  return Response(content=viewer_html, media_type='text/html')
  
@app.get('/prezi2to3/')
@app.post('/prezi2to3/')
async def prezi2to3(request: Request, manifest: Optional[str] = None):
  if request.method == 'GET':
    input_manifest = requests.get(manifest).json()
  else:
    body = await request.body()
    input_manifest = json.loads(body)
  manifest_version = 3 if 'http://iiif.io/api/presentation/3/context.json' in input_manifest.get('@context') else 2
  if manifest_version == 3:
    v3_manifest = input_manifest
  else:
    upgrader = Upgrader(flags={
      'crawl': False,        # NOT YET IMPLEMENTED. Crawl to linked resources, such as AnnotationLists from a Manifest
      'desc_2_md': True,     # If true, then the source's `description` properties will be put into a `metadata` pair. If false, they will be put into `summary`.
      'related_2_md': False, # If true, then the `related` resource will go into a `metadata` pair. If false, it will become the `homepage` of the resource.
      'ext_ok': False,       # If true, then extensions are allowed and will be copied across.
      'default_lang': 'en',  # The default language to use when adding values to language maps.
      'deref_links': False,  # If true, the conversion will dereference external content resources to look for format and type.
      'debug': False,        # If true, then go into a more verbose debugging mode.
      'attribution_label': '', # The label to use for requiredStatement mapping from attribution
      'license_label': ''}   # The label to use for non-conforming license URIs mapped into metadata
    )
    v3_manifest = upgrader.process_resource(input_manifest, True) 
    v3_manifest = upgrader.reorder(v3_manifest)
  
  return v3_manifest

if __name__ == '__main__':
  import uvicorn
  logger.setLevel(logging.INFO)
  parser = argparse.ArgumentParser(description='IIIF dev server')  
  parser.add_argument('--reload', type=bool, default=True, help='Reload on change')
  parser.add_argument('--port', type=int, default=8088, help='HTTP port')
  args = vars(parser.parse_args())
  
  uvicorn.run('main:app', port=args['port'], log_level='info', reload=args['reload'])
else:
  from mangum import Mangum
  handler = Mangum(app)
