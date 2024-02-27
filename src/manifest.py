#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

logging.basicConfig(format='%(asctime)s : %(filename)s : %(levelname)s : %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

import argparse
import boto3
import concurrent.futures
import datetime
import enum
import exif
import ffmpeg
from hashlib import sha256
import json
import os
from time import time as now
import traceback
import yaml

from gh import gh_repo_info, get_gh_file, gh_user_info

from PIL import Image
Image.MAX_IMAGE_PIXELS = 1000000000

import pyvips
logging.getLogger('pyvips').setLevel(logging.WARNING)

import requests
logging.getLogger('requests').setLevel(logging.WARNING)

BUCKET_NAME = 'mdpress-images'

if 'AWS_LAMBDA_FUNCTION_NAME' in os.environ:
  s3 = boto3.client('s3')
else:
  s3 = boto3.Session(
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
  ).client('s3')

def exists(key):
  _exists = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=key)['KeyCount'] > 0
  logger.debug(f'exists: bucket={BUCKET_NAME} key={key} exists={_exists}')
  return _exists

def download_image(url, image_hash):
  start = now()
  resp = requests.get(url, headers={'User-agent': 'IIIF service'})
  path = None
  if resp.status_code == 200:
    path = f'/tmp/{image_hash}'
    with open(path, 'wb') as fp:
      fp.write(resp.content)
  logger.debug(f'download_image: url={url} image_hash={image_hash} elapsed={round(now()-start,3)}')
  return path

def _decimal_coords(coords, ref):
  decimal_degrees = coords[0] + coords[1] / 60 + coords[2] / 3600
  if ref == 'S' or ref == 'W':
      decimal_degrees = -decimal_degrees
  return decimal_degrees

def exif_data(img):
  data = {}
  try:
    exifImg = exif.Image(img)
    for exif_key in sorted(exifImg.list_all()):
      if exif_key.startswith('_'): continue
      if type(exifImg.get(exif_key)) in (int, float, str, bool) or exifImg.get(exif_key) is None:
        data[exif_key] = exifImg.get(exif_key)
      elif type(exifImg.get(exif_key)) == tuple:
        data[exif_key] = [val for val in exifImg.get(exif_key)]
      elif isinstance(exifImg.get(exif_key), enum.Enum):
        data[exif_key] = str(exifImg.get(exif_key)).split('.')[-1]
      else:
        data[exif_key] = str(exifImg.get(exif_key))    
  except:
    logger.debug(traceback.format_exc())
  return data

def av_info(path):
  return ffmpeg.probe(path)['streams'][0]

def image_info(image_hash, refresh=False):
  s3_key = f'{image_hash}.json'
  info = json.loads(s3.get_object(Bucket='mdpress-image-info', Key=s3_key)['Body'].read()) if not refresh and exists(s3_key) else {}
  if info: return info
  try:
    path = f'/tmp/{image_hash}'
    img = Image.open(path)
    info.update({
      'format': Image.MIME[img.format],
      'width': img.width,
      'height': img.height,
      'size': os.stat(path).st_size
    })
    _exif = exif_data(path)
    info.update({'exif': _exif})
    if 'datetime_original' in _exif:
      info['created'] = datetime.datetime.strptime(_exif['datetime_original'], '%Y:%m:%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%SZ')
      if 'gps_longitude' in _exif and 'gps_latitude' in _exif:
        lat = round(_decimal_coords(_exif['gps_latitude'], _exif['gps_latitude_ref']), 6)
        lon = round(_decimal_coords(_exif['gps_longitude'], _exif['gps_longitude_ref']), 6)
        info['coords'] = f'{lat},{lon}'
    s3.put_object(Bucket='mdpress-image-info', Key=s3_key, Body=json.dumps(info))
  except:
    logger.error(traceback.format_exc())
  return info

def convert(image_hash, quality=50, refresh=False, **kwargs):
  start = now()
  dest = f'{image_hash}.tif'
  _exists = exists(dest)
  logger.debug(f'convert: image_hash={image_hash} exists={_exists} refresh={refresh} quality={quality} elapsed={round(now()-start,3)}')
  if _exists and not refresh:
    return

  try:
    img = pyvips.Image.new_from_file(f'/tmp/{image_hash}')
    img.tiffsave(
      f'/tmp/{dest}',
      tile=True,
      compression='jpeg',
      pyramid=True, 
      Q=quality,
      tile_width=512,
      tile_height=512
    )
    save_to_s3(dest)
    os.remove(f'/tmp/{dest}')
  except Exception as e:
    logger.error(f'convert: image_hash={image_hash} error={e}')

def save_to_s3(image_hash):
  logger.debug(f'save_to_s3: bucket={BUCKET_NAME} image_hash={image_hash}')
  s3.upload_file(f'/tmp/{image_hash}', BUCKET_NAME, image_hash)

def get_image_data(**kwargs):
  start = now()
  refresh = kwargs.get('refresh', False)
  url = kwargs['url']
  image_hash = sha256(url.encode('utf-8')).hexdigest()
  _image_info = json.loads(s3.get_object(Bucket='mdpress-image-info', Key=f'{image_hash}.json')['Body'].read()) if not refresh and exists(f'{image_hash}.json') else {}
  if not _image_info:
    download_image(url, image_hash)
    convert(image_hash, **kwargs)
    _image_info = image_info(image_hash, refresh)
    os.remove(f'/tmp/{image_hash}')
  _image_info['url'] = url
  logger.info(f'get_image_data: url={url} elapsed={round(now()-start,3)}')
  return _image_info

def make_manifest(manifestid, image_hash, image_info, image_metadata, baseurl='https://iiif.mdpress.io'):
  manifest = {
    '@context': 'http://iiif.io/api/presentation/3/context.json',
    'id': f'{baseurl}/{manifestid}/manifest.json',
    'type': 'Manifest',
    'label': { 'none': [ image_metadata['label'] ] },
    'items': [{
      'type': 'Canvas',
      'id': f'{baseurl}/{manifestid}/canvas/p1',
      'items': [{
        'type': 'AnnotationPage',
        'id': f'{baseurl}/{manifestid}/p1/1',
        'items': [{
          'type': 'Annotation',
          'id': f'{baseurl}/{manifestid}/annotation/p0001-image',
          'motivation': 'painting',
          'target': f'{baseurl}/{manifestid}/canvas/p1',
          'body': {
            'id': image_info['url'],
            'type': 'Image',
            'format': image_info['format'],
            'width': image_info['width'],
            'height': image_info['height'],
            'service': [
              {
                'id': f'BASEURL ADDED BY ENDPOINT HANDLER/{image_hash}',
                'profile': 'level2',
                'type': 'ImageService3'
              }
            ]
          }
        }]
      }],
      'format': image_info['format'],
      'width': image_info['width'],
      'height': image_info['height']
    }],
    'thumbnail': [
      {
        'id': f'BASEURL ADDED BY ENDPOINT HANDLER/{image_hash}',
        'type': 'Image'
      }
    ],
  }
  if 'metadata' in image_metadata:
    manifest['metadata'] = []
    for md_item in image_metadata['metadata']:
      for key, value in md_item.items():
        manifest['metadata'].append({
          'label': { 'none': [ key ] },
          'value': { 'none': [ value ] }
        })
  if 'summary' in image_metadata:
    manifest['summary'] = { 'none': [ image_metadata['summary'] ] }

  return manifest

def get_gh_image_metadata(manifestid):
  start = now()
  acct, repo, *path = manifestid[3:].split('/')
  repo_info = gh_repo_info(acct, repo)
  ref = repo_info['default_branch']
  user_info = gh_user_info(repo_info['owner']['login'])
  image_metadata = {
    'label': 'label',
    'metadata': [
      { 'owner': user_info['name'] },
      { 'owner_gh_url': user_info['html_url'] }
    ]
  }

  path[-1] = '.'.join(path[-1].split('.')[:-1]) + '.yaml'
  gh_metadata = yaml.load(get_gh_file(acct, repo, ref, '/'.join(path)) or '', Loader=yaml.FullLoader) or {}
  image_metadata.update(gh_metadata)
  logger.info(f'get_gh_image_metadata: manifestid={manifestid} elapsed={round(now()-start,3)}')
  return image_metadata
  
def generate(**kwargs):
  start = now()
  manifestid = kwargs.get('manifestid')
  if manifestid.startswith('gh:'):
    acct, repo, *path = manifestid[3:].split('/')
    url = f'https://raw.githubusercontent.com/{acct}/{repo}/main/{"/".join(path)}'
    image_hash = sha256(url.encode('utf-8')).hexdigest()
    kwargs['url'] = url
  
    manifest_data = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
      futures = {
        executor.submit(get_gh_image_metadata, manifestid): 'metadata',
        executor.submit(get_image_data, **kwargs): 'image-info'
      }
      
      for future in concurrent.futures.as_completed(futures):
        try:
          manifest_data[futures[future]] = future.result()
        except Exception as exc:
          logger.debug(traceback.format_exc())
  logger.debug(json.dumps(manifest_data, indent=2))
  
  manifest = make_manifest(manifestid, image_hash, manifest_data['image-info'], manifest_data['metadata'])
  logger.info(f'generate: manifestid={manifestid} elapsed={round(now()-start,3)}')
  return manifest

if __name__ == '__main__':
  logger.setLevel(logging.INFO)
  parser = argparse.ArgumentParser(description='IIIF Manifest Generator')
  parser.add_argument('url', help='Image URL')
  parser.add_argument('--quality', help='Image quality', type=int, default=50)
  parser.add_argument('--refresh', default=False, action='store_true', help='Force refresh if exists')

  print(json.dumps(generate(**vars(parser.parse_args()))))