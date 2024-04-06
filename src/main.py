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

import gh
import wc

app = FastAPI(title='IIIF Presentation API', root_path='/')

app.add_middleware(
  CORSMiddleware,
  allow_origins=['*'],
  allow_methods=['*'],
  allow_headers=['*'],
  allow_credentials=True,
)

IMAGE_SERVICE_BASEURL = 'https://bxw3h77njs6t5nf7bo2vykqxvi0lzkxb.lambda-url.us-east-1.on.aws'
LOCAL_WC = os.environ.get('LOCAL_WC', 'false').lower() == 'true'
LOCAL_WC_PORT = os.environ.get('LOCAL_WC_PORT', '5173')

from s3 import Bucket as Cache
manifest_cache = Cache(bucket='mdpress-manifests')
image_cache = Cache(bucket='mdpress-images')

def _find_item(obj, type, attr=None, attr_val=None, sub_attr=None):
  if 'items' in obj and isinstance(obj['items'], list):
    for item in obj['items']:
      if item.get('type') == type and (attr is None or item.get(attr) == attr_val):
          return item[sub_attr] if sub_attr else item
      return _find_item(item, type, attr, attr_val, sub_attr)

def _update_image_service(manifest):
  image_data = _find_item(manifest, type='Annotation', attr='motivation', attr_val='painting', sub_attr='body')
  if image_data['type'] != 'Image':
    return manifest

  width = image_data['width']
  orientation = ([rec['value'].get('en', rec['value'].get('none'))[0] for rec in manifest.get('metadata', []) if rec['label'].get('en', rec['label'].get('none'))[0] == 'orientation'] or [1])[0]
  orientation = orientation[0] if isinstance(orientation, list) else orientation
  rotation = 0 if orientation == 1 else 90 if orientation == 6 else 180 if orientation == 3 else 270
  logger.info(f'_update_image_service: width={width} rotation={rotation}')
  if width > 512:
    image_service = image_data['service'][0]
    image_hash = image_service['id'].split('/')[-1]
    image_service['id'] = f'{IMAGE_SERVICE_BASEURL}/iiif/3/{image_hash}'
    manifest['thumbnail'][0]['id'] =  f'{IMAGE_SERVICE_BASEURL}/iiif/3/{image_hash}/full/400,/{rotation}/default.jpg'
  else:
    del image_data['service']
    manifest['thumbnail'][0]['id'] =  image_data['id'].replace(' ', '%20')
  return manifest

def _manifestid_to_url(manifestid):
  if manifestid.startswith('gh:'):
    return gh.manifestid_to_url(manifestid)
  elif manifestid.startswith('wc:'):
    return wc.manifestid_to_url(manifestid)

def _images_from_dir_list(dir_list):
  files = [item for item in dir_list if item['type'] == 'file']
  images = [item for item in files if item['name'].split('.')[-1].lower() in ('gif', 'jpg', 'jpeg', 'png', 'tif', 'tiff')]
  for file in files:
    if file['name'].split('.')[-1].lower() in ('yaml',) and 'iiif-props' not in file['name']:
      fname_base = '.'.join(file['name'].split('.')[:-1]).lower()
      found = False
      for image in images:
        image_base = '.'.join(image['name'].split('.')[:-1]).lower()
        if fname_base == image_base:
          found = True
          break
      if not found:
        images.append(file)
  return images

@app.get('/')
def docs():
  return RedirectResponse(url='/docs')

@app.get('{manifestid:path}/manifest.json')
async def manifest(manifestid: str, refresh: Optional[str] = None):
  start = now()
  refresh = refresh in ('', 'true')
  url = _manifestid_to_url(manifestid)
  imageid = sha256(url.encode('utf-8')).hexdigest()
  manifest = json.loads(manifest_cache.get(imageid, '{}')) if not refresh else None
  cached = manifest is not None
  if not manifest:
    manifest = get_manifest(manifestid=manifestid, refresh=refresh)
    manifest_cache[imageid] = json.dumps(manifest)
  logger.info(f'manifest: manifestid={manifestid} cached={cached} refresh={refresh} elapsed={round(now()-start,3)}')
  return _update_image_service(manifest)

@app.post('manifest/')
@app.post('manifest')
async def get_or_create_manifest(request: Request, refresh: Optional[str] = None):
  start = now()
  refresh = refresh in ('', 'true')
  payload = await request.body()
  payload = json.loads(payload)
  url = payload.get('url')
  imageid = sha256(url.encode('utf-8')).hexdigest()
  manifest = json.loads(manifest_cache.get(imageid, '{}')) if not refresh else None
  cached = manifest is not None
  if not manifest:
    manifest = get_manifest(refresh=refresh, **payload)
    manifest_cache[imageid] = json.dumps(manifest)
  logger.info(f'manifest: url={url} cached={cached} refresh={refresh} elapsed={round(now()-start,3)}')
  return _update_image_service(manifest)

@app.get('thumbnail/{manifestid:path}')
async def thumbnail(manifestid: str, refresh: Optional[str] = None):
  refresh = refresh in ('', 'true')
  url = _manifestid_to_url(manifestid)
  imageid = sha256(url.encode('utf-8')).hexdigest()
  logger.info(f'thumbnail: imageid={imageid} exists={imageid+".tif" in image_cache}')
  manifest = json.loads(manifest_cache.get(imageid, '{}')) if not refresh else None
  if not manifest:
    manifest = get_manifest(manifestid=manifestid, refresh=refresh)
    manifest_cache[imageid] = json.dumps(manifest)
  return RedirectResponse(url=_update_image_service(manifest)['thumbnail'][0]['id'])

def breadcrumb_el(acct, repo, path, baseurl='https://iiif.mdpress.io'):
  el = '<sl-breadcrumb>'
  el += f'<sl-breadcrumb-item>{acct}</sl-breadcrumb-item>'
  el += f'<sl-breadcrumb-item href="{baseurl}/gh:{acct}/{repo}">{repo}</sl-breadcrumb-item>'
  path_elems = [pe for pe in path.split('/') if pe]
  for idx in range(len(path_elems)):
    el += f'<sl-breadcrumb-item href="{baseurl}/gh:{acct}/{repo}/{"/".join(path_elems[:idx+1])}">{path_elems[idx]}</sl-breadcrumb-item>'
  el += '</sl-breadcrumb>'
  return el

def gh_dirs_el(acct, repo, path, dirs, baseurl='https://iiif.mdpress.io'):
  el = '<div class="dirs">'
  for dirname in dirs:
    el += f'<sl-button href="{baseurl}/gh:{acct}/{repo}/{path}{"/" if path else ""}{dirname}" size="small"pill><sl-icon slot="prefix" name="folder"></sl-icon>{dirname}</sl-button>'
  el += '</div>'
  return el

@app.get('prezi2to3/')
@app.post('prezi2to3/')
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

@app.get('gh-dir/{path:path}')
async def ghdir(path: str, filter: Optional[str] = None):
  acct, repo, *path = path.split('/')
  path = '/'.join(path)
  logger.info(f'ghdir: acct={acct} repo={repo} path={path} filter={filter}')
  dir_list = gh.gh_dir_list(acct, repo, path)
  if filter == 'images':
    return _images_from_dir_list(dir_list)
  else:
    return dir_list


@app.get('gh-token')
async def gh_token(code: Optional[str] = None, hostname: Optional[str] = None):
  token = gh.GH_UNSCOPED_TOKEN
  status_code = 200
  if code:
    if hostname in ('127.0.0.1', 'localhost') or hostname.startswith('192.168.'):
      token = os.environ.get('GH_ACCESS_TOKEN')
    else:
      gh_client_id = os.environ.get(f'GH_CLIENT_ID_{hostname.replace(".","_").replace("-","_").upper()}')
      if gh_client_id:
        gh_client_secret = os.environ.get(f'GH_CLIENT_SECRET_{hostname.replace(".","_").replace("-","_").upper()}')
        resp = requests.post(
          'https://github.com/login/oauth/access_token',
          headers={'Accept': 'application/json'},
          data={
            'client_id': gh_client_id,
            'client_secret': gh_client_secret,
            'code': code
          }
        )
        status_code = resp.status_code
        token_obj = resp.json()
        token = token_obj['access_token'] if status_code == 200 else ''
  logger.info(f'gh_token: code={code} hostname={hostname} token={token}')
  return Response(status_code=status_code, content=token, media_type='text/plain')
  
@app.get('{manifestid:path}')
async def image_viewer(request: Request, manifestid: str):
  baseurl = str(request.base_url)[:-1]
  if manifestid.startswith('gh:'):
    acct, repo, *path = manifestid[3:].split('/')
    path = '/'.join(path)
    if '.' not in path:
      viewer_html = open(f'{SCRIPT_DIR}/browser.html', 'r').read()

      breadcrumbs = breadcrumb_el(acct, repo, path, baseurl=baseurl)
      viewer_html = viewer_html.replace('<div class="breadcrumbs"></div>', breadcrumbs)
      
      dirs = [item['name'] for item in gh.gh_dir_list(acct, repo, path) if item['type'] == 'dir']
      gh_dirs = gh_dirs_el(acct, repo, path, dirs, baseurl=baseurl)
      viewer_html = viewer_html.replace('<div class="dirs"></div>', gh_dirs)
      
      viewer_html = viewer_html.replace('gh-dir=""', f'gh-dir="{manifestid[3:]}"')
      if LOCAL_WC:
        viewer_html = viewer_html.replace('https://www.mdpress.io/wc/dist/js/index.js', f'http://localhost:{LOCAL_WC_PORT}/main.ts')
      return Response(content=viewer_html, media_type='text/html')

  viewer_html = open(f'{SCRIPT_DIR}/image.html', 'r').read()
  viewer_html = viewer_html.replace('src=""', f'src="{manifestid}"')
  if LOCAL_WC:
    viewer_html = viewer_html.replace('https://www.mdpress.io/wc/dist/js/index.js', f'http://localhost:{LOCAL_WC_PORT}/main.ts')
  return Response(content=viewer_html, media_type='text/html')

  
if __name__ == '__main__':
  import uvicorn
  logger.setLevel(logging.INFO)
  parser = argparse.ArgumentParser(description='IIIF dev server')  
  parser.add_argument('--reload', type=bool, default=True, help='Reload on change')
  parser.add_argument('--port', type=int, default=8088, help='HTTP port')
  parser.add_argument('--localwc', default=False, action='store_true', help='Use local web components')
  parser.add_argument('--wcport', type=int, default=5173, help='Port used by local WC server')

  args = vars(parser.parse_args())
  
  os.environ['LOCAL_WC'] = str(args['localwc'])
  os.environ['LOCAL_WC_PORT'] = str(args['wcport'])

  logger.info(f'LOCAL_WC={os.environ["LOCAL_WC"]} LOCAL_WC_PORT={os.environ["LOCAL_WC_PORT"]} ')

  uvicorn.run('main:app', port=args['port'], log_level='info', reload=args['reload'])
else:
  from mangum import Mangum
  handler = Mangum(app)
