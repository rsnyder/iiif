#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

logging.basicConfig(format='%(asctime)s : %(filename)s : %(levelname)s : %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

import argparse, os, sys, json
from hashlib import sha256
from time import time as now
from urllib.parse import quote
import re
import httpx
import boto3
from botocore.exceptions import ClientError
import io
from PIL import Image

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(SCRIPT_DIR)
from typing import Tuple, Optional

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import Response
from fastapi.middleware.cors import CORSMiddleware

from starlette.responses import RedirectResponse, StreamingResponse

from prezi_upgrader import Upgrader

from manifest import generate as get_manifest

import requests
logging.getLogger('requests').setLevel(logging.WARNING)

import gh
import wc
import wd

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
  logger.debug(f'_update_image_service: width={width} rotation={rotation}')
  # if width > 512:
  if width > 0:
    image_service = image_data['service'][0]
    image_hash = image_service['id'].split('/')[-1]
    image_service['id'] = f'{IMAGE_SERVICE_BASEURL}/iiif/3/{image_hash}'
    manifest['thumbnail'][0]['id'] =  f'{IMAGE_SERVICE_BASEURL}/iiif/3/{image_hash}/full/400,/{rotation}/default.jpg'
  else:
    del image_data['service']
    manifest['thumbnail'][0]['id'] =  image_data['id'].replace(' ', '%20')
  return manifest

def _manifestid_to_url(manifestid):
  logger.debug(f'_manifestid_to_url: manifestid={manifestid}')
  if manifestid.startswith('gh:'):
    return manifestid, gh.manifestid_to_url(manifestid)
  elif manifestid.startswith('wc:') or manifestid.startswith('https://upload.wikimedia.org/wikipedia/commons'):
    if manifestid.startswith('http'):
      manifestid = f'wc:{manifestid.split("/")[-1]}'
    return manifestid, wc.manifestid_to_url(manifestid)
  elif manifestid.startswith('wd:'):
    return manifestid, wd.manifestid_to_url(manifestid)
  elif manifestid.startswith('default:'):
    return manifestid[8:], manifestid[8:]
  elif manifestid.startswith('http'):
    return manifestid, manifestid
  
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

@app.post('manifest/')
@app.post('manifest')
async def get_or_create_manifest(request: Request, refresh: Optional[str] = None):
  start = now()
  refresh = refresh in ('', 'true')
  payload = await request.body()
  payload = json.loads(payload)
  _, payload['url'] = _manifestid_to_url(payload['url'])
  url = payload.get('url')
  imageid = sha256(url.encode('utf-8')).hexdigest()
  manifest = json.loads(manifest_cache.get(imageid, '{}')) if not refresh else None
  cached = manifest is not None
  if not manifest:
    manifest = get_manifest(refresh=refresh, **payload)
    manifest_cache[imageid] = json.dumps(manifest)
  logger.debug(f'manifest: url={url} cached={cached} refresh={refresh} elapsed={round(now()-start,3)}')
  return _update_image_service(manifest)

@app.get('thumbnail/{manifestid:path}')
async def thumbnail(manifestid: str, url: Optional[str] = None, refresh: Optional[str] = None):
  refresh = refresh in ('', 'true')
  manifestid, url = url or _manifestid_to_url(manifestid)
  imageid = sha256(url.encode('utf-8')).hexdigest()
  logger.debug(f'thumbnail: imageid={imageid} exists={imageid+".tif" in image_cache}')
  manifest = json.loads(manifest_cache.get(imageid, '{}')) if not refresh else None
  if not manifest:
    manifest = get_manifest(manifestid=manifestid, refresh=refresh)
    manifest_cache[imageid] = json.dumps(manifest)
  return RedirectResponse(url=_update_image_service(manifest)['thumbnail'][0]['id'])

def s3_key_exists(bucket_name: str, key: str) -> bool:
    """
    Check whether a given key exists in the specified S3 bucket.

    :param bucket_name: Name of the S3 bucket (e.g., "juncture-thumbnail-cache")
    :param key:         The object key to check (e.g., "image/foo.png")
    :return:            True if the object exists, False if it does not.
    """
    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as e:
        # If a 404 Not Found error is thrown, the key does not exist.
        if e.response["Error"]["Code"] == "404":
            return False
        # For any other error (403, 400, etc.), you may want to re-raise or handle differently.
        raise

def upload_image_to_s3(bucket_name: str, key: str, image_bytes: bytes, content_type: str = "image/png"):
  """
  Uploads an image (raw bytes) to S3.

  :param bucket_name:  The target S3 bucket name (e.g. "juncture-thumbnail-cache")
  :param key:          The S3 key/path under which to store the image (e.g. "image/foo.png")
  :param image_bytes:  The image data as raw bytes
  :param content_type: The MIME type of the image (e.g. "image/png" or "image/jpeg")
  """
  s3 = boto3.client('s3')
  s3.put_object(
    Bucket=bucket_name,
    Key=key,
    Body=image_bytes,
    ContentType=content_type,
    CacheControl="max-age=86400"  # optional: instruct CloudFront (and browsers) to cache for 1 day
  )

def resize_image(img: Image.Image, size: str) -> Image.Image:
  """
  Given a PIL Image `img` and a comma-separated `size` string "w,h",
  return a new Image resized as follows:
    - If both w>0 and h>0: force resize to (w, h)
    - If h==0: scale by width only
    - If w==0: scale by height only
    - If both are 0: return original img unchanged
  """
  # 1) Parse "w,h" into two integers, blank→0
  parts = size.split(",", 1)
  if len(parts) < 2:
    parts += [""] * (2 - len(parts))

  # Convert blank or whitespace string to 0, else int(...)
  target_w, target_h = [
    int(p.strip()) if p.strip() else 0
    for p in parts
  ]
  buf = io.BytesIO()  
  
  # Case A: both dimensions > 0 → force exact resize
  if target_w > 0 and target_h > 0:
    resized_img = img.resize((target_w, target_h), resample=Image.LANCZOS)
    resized_img.save(buf, format='JPEG')

  # Case B: height == 0 → scale by width only
  if target_h == 0 and target_w > 0:
    # Make a copy so we don't mutate original
    resized_img = img.copy()
    # Provide a huge max-height so only width is constrained
    max_width = target_w
    max_height = sys.maxsize
    resized_img.thumbnail((max_width, max_height), resample=Image.LANCZOS)
    resized_img.save(buf, format='JPEG')

  # Case C: width == 0 → scale by height only
  elif target_w == 0 and target_h > 0:
    # Make a copy so we don't mutate original
    resized_img = img.copy()
    max_width = sys.maxsize
    max_height = target_h
    resized_img.thumbnail((max_width, max_height), resample=Image.LANCZOS)
    resized_img.save(buf, format='JPEG')
    
  else: # Case D: both w==0 and h==0 → return original
    img.save(buf, format='JPEG')
  
  return buf.getvalue()
  
async def _get_image(image_key, transformations: Optional[str] = '') -> Optional[bytes]:
  # Parse transformation string like: w_300,h_200,c_fill
  params = {}
  for part in transformations.split(','):
    if '_' in part:
      key, val = part.split('_', 1)
      params[key] = val
  
  if 'w' in params:
    if 'h' in params: size = f'{params["w"]},{params["h"]}'
    else: size = f'{params["w"]},'
  elif 'h' in params: size = f',{params["h"]}'
  else: 
    transformations = 'w_1000'
    size = '1000,'
  
  _, url = _manifestid_to_url(image_key)
  imageid = sha256(url.encode('utf-8')).hexdigest()  
  s3_key =  f'image/{image_key}/{transformations}'
  iiif_url = f'https://bxw3h77njs6t5nf7bo2vykqxvi0lzkxb.lambda-url.us-east-1.on.aws/iiif/3/{imageid}/full/{size}/0/default.jpg'
  
  print(f'_get_image: image_key={image_key} transformations={transformations} s3_key={s3_key} iiif_url={iiif_url} s3_key_exists={s3_key_exists("juncture-thumbnail-cache", s3_key)}')

  if s3_key_exists('juncture-thumbnail-cache', s3_key):
    s3_client = boto3.client('s3')
    try:
      resp = s3_client.get_object(Bucket='juncture-thumbnail-cache', Key=s3_key)
    except ClientError as e:
        # If the object does not exist, return a 404; else re‐raise
        error_code = e.response['Error']['Code']
        if error_code in ('NoSuchKey', '404'):
            raise HTTPException(status_code=404, detail='Object not found in S3')
        raise

    # Read the object’s body into bytes
    body_stream = resp['Body']
    content = body_stream.read()

    # Determine the Content-Type from the S3 response headers (default to application/octet-stream)
    content_type = resp.get('ContentType', 'application/octet-stream')

    # Return as a streaming response (suitable for large files)
    # return StreamingResponse(io.BytesIO(content), media_type=content_type)
    return StreamingResponse(io.BytesIO(content), media_type=content_type, headers={'X-Origin': 'Lambda'})
  else:
    try:
      async with httpx.AsyncClient() as client:
        iiif_response = await client.get(iiif_url)
        if iiif_response.status_code == 200:
          image = iiif_response.content
          upload_image_to_s3(bucket_name='juncture-thumbnail-cache', key=s3_key, image_bytes=image, content_type='image/jpeg')
          return RedirectResponse(url=iiif_url)
        else:
          manifest = get_manifest_as_json(image_key)
          image_data = _find_item(manifest, type='Annotation', attr='motivation', attr_val='painting', sub_attr='body')
          image_response = await client.get(image_data['id'])
          if image_response.status_code == 200:
            image = resize_image(Image.open(io.BytesIO(image_response.content)), size=size)
            upload_image_to_s3(bucket_name='juncture-thumbnail-cache', key=s3_key, image_bytes=image, content_type='image/jpeg')
            return StreamingResponse(io.BytesIO(image), media_type='image/jpeg')
          else:
            return Response(content=f'Error fetching image: {iiif_response.status_code} - {iiif_response.text}', media_type='text/plain', status_code=iiif_response.status_code)
    except httpx.RequestError as e:
      # Network or DNS issues
      raise HTTPException(status_code=502, detail=f"Network error: {str(e)}")

def is_valid_transformations(transform_str: str) -> bool:
    """
    Returns True if transform_str is a comma-separated list of key_value pairs
    where:
      - key is one or more letters (a–z, case-insensitive)
      - value is one or more alphanumeric characters (no commas or slashes)
    Examples of valid strings:
      "w_300"
      "w_300,h_200,c_fill"
    """
    # Each segment must match `<alpha>_<alnum>`
    segment_pattern = re.compile(r"^[A-Za-z]+_[A-Za-z0-9]+$")
    for part in transform_str.split(","):
        if not segment_pattern.match(part):
            return False
    return True

@app.get('image/{image_key:path}/{transformations:path}')
async def get_image_with_transformations(transformations: str, image_key: str):
  if not is_valid_transformations(transformations):
    image_key += f'/{transformations}'
    transformations = ''
  return await _get_image(image_key, transformations)

@app.get('image/{image_key:path}')
async def get_image_without_transformations(image_key: str):  
  return await _get_image(image_key)

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

@app.get('v3/{manifest:path}')
@app.get('v3/')
@app.get('v3')
@app.get('prezi2to3/')
@app.post('prezi2to3/')
async def prezi2to3(request: Request, manifest: Optional[str] = None):
  logger.debug(f'prezi2to3: manifest={manifest}')
  if request.method == 'GET':
    m = re.match(r'^(?P<before>.+)(?P<arkIdentifier>ark:\/\w+\/\w+)(?P<after>.+)?', manifest)
    if m:
      manifest = f'{m.group("before")}{quote(m.group("arkIdentifier").replace("/","%2F"))}{m.group("after")}'
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
  logger.debug(f'gh_token: code={code} hostname={hostname} token={token}')
  return Response(status_code=status_code, content=token, media_type='text/plain')

def is_browser(user_agent):
    # List of common browser signatures
    browser_signatures = ['Chrome', 'Firefox', 'Safari', 'Edge', 'Opera', 'Gecko', 'WebKit']

    # Check if any known browser signatures are present in the user agent string
    return any(signature in user_agent for signature in browser_signatures)

@app.get('{manifestid:path}/manifest.json')
async def manifest(manifestid: str, refresh: Optional[str] = None):
  return get_manifest_as_json(manifestid, refresh)

@app.get('{manifestid:path}')
async def image_viewer(request: Request, manifestid: str, refresh: Optional[str] = None):
  if is_browser(request.headers['user-agent']):
    return Response(content=get_image_viewer_html(request, manifestid), media_type='text/html')
  else:
    return get_manifest_as_json(manifestid, refresh)

def get_manifest_as_json(manifestid: str, refresh: Optional[str] = None):
    start = now()
    refresh = refresh in ('', 'true')
    manifestid, url = _manifestid_to_url(manifestid)
    imageid = sha256(url.encode('utf-8')).hexdigest()
    manifest = json.loads(manifest_cache.get(imageid, '{}')) if not refresh else None
    cached = manifest is not None
    if not manifest:
      manifest = get_manifest(manifestid=manifestid, refresh=refresh)
      manifest_cache[imageid] = json.dumps(manifest)
    logger.debug(f'manifest: manifestid={manifestid} cached={cached} refresh={refresh} elapsed={round(now()-start,3)}')
    return _update_image_service(manifest)

def get_image_viewer_html(request: Request, manifestid: str):
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
        viewer_html = viewer_html.replace('https://cdn.jsdelivr.net/npm/juncture-digital/js/index.js', f'http://localhost:{LOCAL_WC_PORT}/main.ts')
        viewer_html = viewer_html.replace('https://v3.juncture-digital.org/wc/dist/js/index.js', f'http://localhost:{LOCAL_WC_PORT}/main.ts')
      return viewer_html

  viewer_html = open(f'{SCRIPT_DIR}/image.html', 'r').read()
  viewer_html = viewer_html.replace('src=""', f'src="{manifestid}"')
  if LOCAL_WC:
    viewer_html = viewer_html.replace('https://cdn.jsdelivr.net/npm/juncture-digital/js/index.js', f'http://localhost:{LOCAL_WC_PORT}/main.ts')
    viewer_html = viewer_html.replace('https://v3.juncture-digital.org/wc/dist/js/index.js', f'http://localhost:{LOCAL_WC_PORT}/main.ts')
  return viewer_html
  
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
  handler = Mangum(app, lifespan='off')
