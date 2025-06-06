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
import magic
import os
from time import time as now
from urllib.parse import unquote
import traceback
import yaml

import gh
import wc
import wd

from PIL import Image
Image.MAX_IMAGE_PIXELS = 1000000000

import pyvips
logging.getLogger('pyvips').setLevel(logging.ERROR)

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

cc_licenses = {
  # Creative Commons Licenses
  'PD': {'label': 'Public Domain', 'url': ''},
  'PUBLIC DOMAIN': {'label': 'Public Domain', 'url': ''},
  'PUBLIC-DOMAIN': {'label': 'Public Domain', 'url': ''},
  'PDM': {'label': 'Public Domain Mark', 'url': ''},

  'CC0': {'label': 'Public Domain Dedication', 'url': 'http://creativecommons.org/publicdomain/zero/1.0/'},
  'CC-BY': {'label': 'Attribution', 'url': 'http://creativecommons.org/licenses/by/4.0/'},
  'CC-BY-SA': {'label': 'Attribution-ShareAlike', 'url': 'http://creativecommons.org/licenses/by-sa/4.0/'},
  'CC-BY-ND': {'label': 'Attribution-NoDerivs', 'url': 'http://creativecommons.org/licenses/by-nd/4.0/'},
  'CC-BY-NC': {'label': 'Attribution-NonCommercial', 'url': 'http://creativecommons.org/licenses/by-nc/4.0/'},
  'CC-BY-NC-SA': {'label': 'Attribution-NonCommercial', 'url': 'http://creativecommons.org/licenses/by-nc-sa/4.0/'},
  'CC-BY-NC-ND': {'label': 'Attribution-NonCommercial-NoDerivs', 'url': 'http://creativecommons.org/licenses/by-nc-nd/4.0/'}
}

def exists(key):
  _exists = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=key)['KeyCount'] > 0
  logger.debug(f'exists: bucket={BUCKET_NAME} key={key} exists={_exists}')
  return _exists

def download(url, url_hash):
  start = now()
  extension = url.split('/')[-1].split('.')[-1].lower()
  path = None
  if 'raw.githubusercontent.com' in url and extension not in ('gif', 'jpg', 'jpeg', 'mp3', 'mp4', 'ogg', 'ogv', 'png', 'tif', 'tiff', 'webm'):
    acct, repo, ref, *path = url.split('/')[3:]
    path[-1] = f'{path[-1].replace(".yaml","")}.yaml'
    logger.info(f'get_gh_file: acct={acct} repo={repo} ref={ref} path={path}')
    gh_metadata = yaml.load(gh.get_gh_file(acct, repo, ref, '/'.join(path)), Loader=yaml.FullLoader)
    url = gh_metadata.get('image_url', url)
    print(f'GH Metadata: {json.dumps(gh_metadata, indent=2)}')
    print(f'GH URL: {url}')
  resp = requests.get(url, headers={'User-agent': 'IIIF service'}, verify=False)
  if resp.status_code == 200:
    path = f'/tmp/{url_hash}'
    with open(path, 'wb') as fp:
      fp.write(resp.content)
  else:
    logger.warning(f'download failed: url={url} code={resp.status_code} msg={resp.text}')
  logger.debug(f'download: url={url} url_hash={url_hash} elapsed={round(now()-start,3)}')
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
      if type(exifImg.get(exif_key)) in (int, float, str, bool) or exifImg.get(exif_key) is None or exif_key in ('orientation',):
        data[exif_key] = exifImg.get(exif_key)
      elif type(exifImg.get(exif_key)) == tuple:
        data[exif_key] = [val for val in exifImg.get(exif_key)]
      elif isinstance(exifImg.get(exif_key), enum.Enum):
        data[exif_key] = str(exifImg.get(exif_key)).split('.')[-1]
      else:
        data[exif_key] = str(exifImg.get(exif_key))    
  except:
    logger.warning(traceback.format_exc())
  logger.debug(json.dumps(data, indent=2))
  return data

def av_info(path):
  return ffmpeg.probe(path)['streams'][0]

def image_info(url_hash, refresh=False):
  s3_key = f'{url_hash}.json'
  info = json.loads(s3.get_object(Bucket='mdpress-image-info', Key=s3_key)['Body'].read()) if not refresh and exists(s3_key) else {}
  if info: return info
  try:
    path = f'/tmp/{url_hash}'
    img = Image.open(path)
    info.update({
      'type': 'Image',
      'format': Image.MIME[img.format],
      'width': img.width,
      'height': img.height,
      'size': os.stat(path).st_size,
      'id': sha256(img.tobytes()).hexdigest()[0:8]
    })
    if 'exif' in info: return info
    _exif = exif_data(path)
    info.update({'exif': _exif})
    logger.debug(json.dumps(_exif, indent=2, sort_keys=True))
    if 'orientation' in _exif:
      info['orientation'] = _exif['orientation']
    if 'datetime_original' in _exif:
      info['created'] = datetime.datetime.strptime(_exif['datetime_original'], '%Y:%m:%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%SZ')
    if 'gps_longitude' in _exif and 'gps_latitude' in _exif:
      lat = round(_decimal_coords(_exif['gps_latitude'], _exif['gps_latitude_ref']), 6)
      lon = round(_decimal_coords(_exif['gps_longitude'], _exif['gps_longitude_ref']), 6)
      info['location'] = {'coords': [lat, lon]}
    if 'make' in _exif and 'model' in _exif:
      info['camera'] = f"{_exif['make']} {_exif['model']}"
    if 'focal_length' in _exif and 'exposure_time' in _exif and 'f_number' in _exif and 'photographic_sensitivity' in _exif:
      info['exposure'] = f"{_exif.get('focal_length_in_35mm_film', _exif.get('focal_length'))}mm 1/{round(1/_exif['exposure_time']) if _exif['exposure_time'] < 1 else _exif['exposure_time']}s f/{_exif['f_number']} ISO {_exif['photographic_sensitivity']}"
    if 'exposure_mode' in _exif and 'exposure_program' in _exif:
      info['mode'] = f"{_exif['exposure_mode']}, {_exif['exposure_program']}"
    info['size'] = f"{info['width']} x {info['height']} {info['format'].split('/')[-1]}"
    s3.put_object(Bucket='mdpress-image-info', Key=s3_key, Body=json.dumps(info, indent=2))
  except:
    logger.error(traceback.format_exc())
  logger.debug(json.dumps(info, indent=2))
  return info

def hms_to_secs(time_str):
    h, m, s = time_str.split(':')
    return int(h) * 3600 + int(m) * 60 + int(float(s))
  
def media_info(path):
  _media_info = {}
  mime = magic.from_file(path, mime=True)
  _type = mime.split('/')[0]
  logger.info(f'media_info: path={path} mime={mime} type={_type}')
  if _type in ('audio', 'video'):
    _av_info = av_info(path)
    if _av_info:
      logger.debug(json.dumps(_av_info, indent=2))
      if 'display_aspect_ratio' in _av_info:
        wh = [int(v) for v in _av_info['display_aspect_ratio'].split(':')]
        aspect = wh[0]/wh[1]
        _av_info['width'] = round(_av_info['height'] * aspect)
      _media_info = {
        'type': _type.replace('audio', 'sound').capitalize(),
        'format': mime,
        'size': os.stat(path).st_size
      }
      for fld in ('duration', 'height', 'width'):
        if fld in _av_info:
          _media_info[fld] = _av_info[fld]
      if 'tags' in _av_info and 'DURATION' in _av_info['tags']:
        _media_info['duration'] = hms_to_secs(_av_info['tags']['DURATION'])
        
      if 'duration' in _media_info:
        _media_info['duration'] = round(float(_media_info['duration']), 1)
  return _media_info

def convert(url_hash, quality=50, refresh=False, **kwargs):
  start = now()
  dest = f'{url_hash}.tif'
  _exists = exists(dest)
  logger.debug(f'convert: url_hash={url_hash} exists={_exists} refresh={refresh} quality={quality} elapsed={round(now()-start,3)}')
  if _exists and not refresh:
    return

  try:
    img = pyvips.Image.new_from_file(f'/tmp/{url_hash}')
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
    logger.error(f'convert: url_hash={url_hash} error={e}')

def save_to_s3(url_hash):
  logger.debug(f'save_to_s3: bucket={BUCKET_NAME} url_hash={url_hash}')
  s3.upload_file(f'/tmp/{url_hash}', BUCKET_NAME, url_hash)

def get_image_data(**kwargs):
  start = now()
  refresh = kwargs.get('refresh', False)
  url = kwargs['url']
  url_hash = sha256(url.encode('utf-8')).hexdigest()
  
  extension = url.split('.')[-1].lower()
  _media_info = json.loads(s3.get_object(Bucket='mdpress-image-info', Key=f'{url_hash}.json')['Body'].read()) if not refresh and exists(f'{url_hash}.json') else {}
  if not _media_info:
    path = download(url, url_hash)
    _type = 'av' if extension in ('mp3', 'mp4', 'webm', 'oga', 'ogg', 'ogv') else 'image'
    if _type == 'av':
      _media_info = media_info(path)
    else:
      convert(url_hash, **kwargs)
      _media_info = image_info(url_hash, refresh)
      os.remove(f'/tmp/{url_hash}')
  _media_info['url'] = url
  logger.info(f'get_image_data: url={url} elapsed={round(now()-start,3)}')
  return _media_info

def make_manifest(manifestid, url_hash, image_info, image_metadata, baseurl='https://iiif.mdpress.io'):
  manifestid = manifestid or url_hash
  lang = image_metadata.get('language', 'none')
  manifest = {
    '@context': [
      'http://iiif.io/api/extension/navplace/context.json',
      'http://iiif.io/api/presentation/3/context.json'
    ],
    'id': f'{baseurl}/{manifestid.replace(" ", "_")}/manifest.json',
    'type': 'Manifest',
    'label': { image_metadata.get('language', lang): [ image_metadata['label'] ] },
    'items': [{
      'type': 'Canvas',
      'id': f'{baseurl}/{url_hash}/canvas/p1',
      'items': [{
        'type': 'AnnotationPage',
        'id': f'{baseurl}/{url_hash}/p1/1',
        'items': [{
          'type': 'Annotation',
          'id': f'{baseurl}/{url_hash}/annotation/p0001-image',
          'motivation': 'painting',
          'target': f'{baseurl}/{url_hash}/canvas/p1',
          'body': {
            'id': image_info['url'],
            'type': image_info['type'],
            'format': image_info['format']
          }
        }]
      }],
      'format': image_info['format']
    }],
    'metadata': []
  }
  
  canvas = manifest['items'][0]
  annotation = canvas['items'][0]['items'][0]
  annotation_body = annotation['body']
  _type = image_info.get('type').lower()
  if _type in ('sound', 'video'):
    if 'duration' in image_info:
      canvas['duration'] = image_info.get('duration')
      annotation_body['duration'] = image_info.get('duration')
  if _type in ('image', 'video'):
    canvas['width'] = image_info['width']
    canvas['height'] = image_info['height']
    annotation_body['width'] = image_info['width']
    annotation_body['height'] = image_info['height']
  if _type == 'image':
    annotation_body['service'] = [{
      'id': f'BASEURL ADDED BY ENDPOINT HANDLER/{url_hash}',
      'profile': 'level2',
      'type': 'ImageService3'
    }]
    manifest['thumbnail'] = [{
      'id': f'BASEURL ADDED BY ENDPOINT HANDLER/{url_hash}',
      'type': 'Image'
    }]
  
  if 'summary' in image_metadata: manifest['summary'] = { lang: [ image_metadata['summary'] ] }
  if 'rights' in image_metadata: manifest['rights'] = image_metadata['rights']
  if 'requiredStatement' in image_metadata: manifest['requiredStatement'] = image_metadata['requiredStatement']
  if 'metadata' in image_metadata: manifest['metadata'] = image_metadata['metadata']
  if 'created' in image_metadata or 'created' in image_info: manifest['navDate'] = image_metadata.get('created', image_info.get('created'))
  if 'location' in image_metadata or 'location' in image_info:
    location = image_metadata.get('location', image_info.get('location'))
    manifest['navPlace'] = {
      'id' : f'{baseurl}/{url_hash}/iiif/feature-collection/2',
      'type' : 'FeatureCollection',
      'features':[{
        'id': f'{baseurl}/{url_hash}/iiif/feature/2',
        'type': 'Feature',
        'geometry': {
          'type': 'Point',
          'coordinates': location['coords']
        }
      }]
    }
    if location.get('label'): 
      manifest['navPlace']['features'][0]['properties'] = {
        'label': { lang: [ location['label'] ] },
        'description': { lang: [ location['description'] ] },
        'id': { lang: [ location['id'] ] }
      }

  existing_metadata_keys = [m['label'][lang][0] for m in manifest['metadata']]
  for key in ('camera', 'exposure', 'mode', 'orientation', 'size'):   
    if key in image_info and key not in existing_metadata_keys:
      manifest['metadata'].append({
        'label': { 'en': [ key ] },
        'value': { 'en': [ image_info[key] ] }
      })
  
  if 'id' in image_info: manifest['metadata'].append({'label': { 'en': [ 'annoid' ] },'value': { 'en': [ image_info['id'] ] }})

  return manifest

def metadata_from_obj(**kwargs):
  kwargs = dict([(k.lower(), v) for k,v in kwargs.items()])
  url = kwargs.get('url')
  lang = kwargs.get('language', 'none')
  label = kwargs['label'] if 'label' in kwargs else kwargs['title'] if 'title' in kwargs else unquote(url.split('/')[-1].split('.')[0]).replace('_',' ')
  summary = kwargs['summary'] if 'summary' in kwargs else kwargs['description'] if 'description' in kwargs else None
  
  owner = kwargs['owner'] if 'owner' in kwargs else 'Unspecified'
  
  license_code = kwargs['license'].upper() if 'license' in kwargs else None
  license_url = cc_licenses[license_code]['url'] if license_code in cc_licenses else None
  license_label = cc_licenses[license_code]['label'] if license_code in cc_licenses else None
  
  metadata = {
    'language': kwargs.get('language', kwargs.get('lang', 'none')),
    'label': label,
    'metadata': [
      { 'label': { lang: [ 'title' ] }, 'value': { lang: [ label ] }},
      { 'label': { lang: [ 'source' ] }, 'value': { lang: [ url ] } }
    ]
  }
  if summary:
    metadata['summary'] = summary

  if license_url:
    metadata['rights'] = license_url
    
  if owner:
    metadata['metadata'].append({
      'label': { lang: [ 'author' ] }, 
      'value': { lang: [ owner ] }
    })
  
  if 'attribution' in kwargs:
    attribution_statement = kwargs['attribution']
  elif owner and license_url and license_code not in ('PD', 'PUBLIC DOMAIN', 'PDM'):
    attribution_statement = f'Image <em>{label}</em> provided by {owner} under a <a href="{license_url}">{license_label} ({license_code.replace("CC-", "CC ")})</a> license'
  else:
    attribution_statement = None

  if attribution_statement:
    metadata['requiredStatement'] = {
      'label': { lang: [ 'attribution' ] },
      'value': { lang: [ attribution_statement ] }
    }
  return metadata

def generate(**kwargs):
  start = now()
  metadata_fn = metadata_from_obj

  manifestid = kwargs.get('manifestid')
  url = kwargs.get('url') or manifestid

  logger.info(f'generate: manifestid={manifestid}')
  
  if manifestid:
    if manifestid.startswith('gh:'):
      url = gh.manifestid_to_url(manifestid)
      metadata_fn = gh.get_iiif_metadata
    
    elif manifestid.startswith('wc:'):
      url = wc.manifestid_to_url(manifestid)
      metadata_fn = wc.get_iiif_metadata
    
    elif manifestid.startswith('wd:'):
      url = wd.manifestid_to_url(manifestid)
      metadata_fn = wd.get_iiif_metadata

  if metadata_fn:
    url_hash = sha256(url.encode('utf-8')).hexdigest()
    kwargs['url'] = url

    manifest_data = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
      futures = {
        executor.submit(metadata_fn, **kwargs): 'metadata',
        executor.submit(get_image_data, **kwargs): 'image-info'
      }
      
      for future in concurrent.futures.as_completed(futures):
        try:
          manifest_data[futures[future]] = future.result()
        except Exception as exc:
          logger.error(traceback.format_exc())
  
  logger.info(json.dumps(manifest_data, indent=2))
  
  manifest = make_manifest(manifestid, url_hash, manifest_data['image-info'], manifest_data['metadata'])
  logger.info(f'generate: manifestid={manifestid} elapsed={round(now()-start,3)}')
  return manifest

if __name__ == '__main__':
  logger.setLevel(logging.INFO)
  parser = argparse.ArgumentParser(description='IIIF Manifest Generator')
  parser.add_argument('url', help='Image URL')
  parser.add_argument('--quality', help='Image quality', type=int, default=50)
  parser.add_argument('--refresh', default=False, action='store_true', help='Force refresh if exists')

  print(json.dumps(generate(**vars(parser.parse_args()))))