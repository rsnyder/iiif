#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
logging.basicConfig(format='%(asctime)s : %(filename)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

import hashlib

import json
from time import time as now
from urllib.parse import quote, unquote
import re

from bs4 import BeautifulSoup

import requests
logging.getLogger('requests').setLevel(logging.INFO)

from expiringdict import ExpiringDict
image_urls = ExpiringDict(max_len=100, max_age_seconds=1800) # cache image urls for 30 minutes
wd_entities = ExpiringDict(max_len=100, max_age_seconds=1800) # cache entities for 30 minutes
wc_entities = ExpiringDict(max_len=100, max_age_seconds=1800)

licenses = {
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
  'CC-BY-NC-ND': {'label': 'Attribution-NonCommercial-NoDerivs', 'url': 'http://creativecommons.org/licenses/by-nc-nd/4.0/'},

  # Rights Statements 
  'InC': {'label': 'IN COPYRIGHT', 'url': 'http://rightsstatements.org/vocab/InC/1.0/'},
  'InC-OW-EU': {'label': 'IN COPYRIGHT - EU ORPHAN WORK', 'url': 'http://rightsstatements.org/vocab/InC-OW-EU/1.0/'},
  'InC-EDU': {'label': 'IN COPYRIGHT - EDUCATIONAL USE PERMITTED', 'url': 'http://rightsstatements.org/vocab/InC-EDU/1.0/'},
  'InC-NC': {'label': 'IN COPYRIGHT - NON-COMMERCIAL USE PERMITTED', 'url': 'http://rightsstatements.org/vocab/InC-NC/1.0/'},
  'InC-RUU': {'label': 'IN COPYRIGHT - RIGHTS-HOLDER(S) UNLOCATABLE OR UNIDENTIFIABLE', 'url': 'http://rightsstatements.org/vocab/InC-RUU/1.0/'},
  'NoC-CR': {'label': 'NO COPYRIGHT - CONTRACTUAL RESTRICTIONS', 'url': 'http://rightsstatements.org/vocab/NoC-CR/1.0/'},
  'NoC-NC': {'label': 'NO COPYRIGHT - NON-COMMERCIAL USE ONLY', 'url': 'http://rightsstatements.org/vocab/NoC-NC/1.0/'},
  'NoC-OKLR': {'label': 'NO COPYRIGHT - OTHER KNOWN LEGAL RESTRICTIONS', 'url': 'http://rightsstatements.org/vocab/NoC-OKLR/1.0/'},
  'NoC-US': {'label': 'NO COPYRIGHT - UNITED STATES', 'url': 'http://rightsstatements.org/vocab/NoC-US/1.0/'},
  'CNE': {'label': 'COPYRIGHT NOT EVALUATED', 'url': 'http://rightsstatements.org/vocab/CNE/1.0/'},
  'UND': {'label': 'COPYRIGHT UNDETERMINED', 'url': 'http://rightsstatements.org/vocab/UND/1.0/'},
  'NKC': {'label': 'NO KNOWN COPYRIGHT', 'url': 'http://rightsstatements.org/vocab/NKC/1.0/'}
}

def _extract_text(val, lang='en'):
  soup = BeautifulSoup(val, 'html5lib')
  _elem = soup.select_one(f'[lang="{lang}"]')
  return (_elem.text if _elem else soup.text).strip()
  
def _get_wc_metadata(title):
  url = f'https://commons.wikimedia.org/w/api.php?format=json&action=query&titles=File:{quote(title)}&prop=imageinfo&iiprop=extmetadata|size|mime'
  resp = requests.get(url)
  logger.debug(f'{url} {resp.status_code}')
  if resp.status_code == 200:
    return list(resp.json()['query']['pages'].values())[0]
  
def _get_wc_entity(pageid):
  if pageid not in wc_entities:
    url = f'https://commons.wikimedia.org/wiki/Special:EntityData/M{pageid}.json'
    resp = requests.get(url)
    logger.debug(f'get_wc_entity: url={url} status={resp.status_code}')
    if resp.status_code == 200:
      wc_entities[pageid] = resp.json()['entities'][f'M{pageid}']
  return wc_entities.get(pageid)

def _get_wd_entity(qid):
  if qid not in wd_entities:
    resp = requests.get(f'https://www.wikidata.org/wiki/Special:EntityData/{qid}.json')
    if resp.status_code == 200:
      results = resp.json()
      if qid in results['entities']:
        wd_entities[qid] = results['entities'][qid]
  return wd_entities.get(qid)

def _digital_representation_of(entity):
  if entity:
    statements = entity['statements'] if 'statements' in entity else entity['claims']
    if 'P6243' in statements:
      return statements['P6243'][0]['mainsnak']['datavalue']['value']['id']
  return []

def _get_entity_labels(qids, lang='en'):
  values = ' '.join([f'(<http://www.wikidata.org/entity/{qid}>)' for qid in qids])
  query = f'SELECT ?item ?label WHERE {{ VALUES (?item) {{ {values} }} ?item rdfs:label ?label . FILTER (LANG(?label) = "{lang}" || LANG(?label) = "en") .}}'
  resp = requests.get(
    f'https://query.wikidata.org/sparql?query={quote(query)}',
    headers = {
      'Content-Type': 'application/x-www-form-urlencoded', 
      'Accept': 'application/sparql-results+json',
      'User-Agent': 'Juncture Client'
    }
  )
  return dict([(rec['item']['value'].split('/')[-1],rec['label']['value']) for rec in resp.json()['results']['bindings']]) if resp.status_code == 200 else {}

def _get_location_data(qid, lang='en'):
  query = f'SELECT ?item ?label ?description ?coords WHERE {{ VALUES (?item) {{ (wd:{qid}) }} ?item rdfs:label ?label; schema:description ?description . FILTER (LANG(?label) = "{lang}" || LANG(?description) = "en") . FILTER (LANG(?description) = "{lang}" || LANG(?label) = "en") . OPTIONAL {{ ?item wdt:P625 ?coords . }} }}'
  resp = requests.get(
    f'https://query.wikidata.org/sparql?query={quote(query)}',
    headers = {
      'Content-Type': 'application/x-www-form-urlencoded', 
      'Accept': 'application/sparql-results+json',
      'User-Agent': 'Juncture Client'
    }
  )
  label = description = coords = None
  if resp.status_code == 200:
    results = resp.json()['results']['bindings']
    if len(results) > 0:
      label = results[0]['label']['value']
      description = results[0]['description']['value']
      coords = [float(coord) for coord in results[0]['coords']['value'].replace('Point(','').replace(')','').split(' ')] if 'coords' in results[0] else None
  return label, description, coords

def wc_title_to_url(title, width=None):
  title = unquote(title).replace(' ','_')
  md5 = hashlib.md5(title.encode('utf-8')).hexdigest()
  logger.debug(f'wc_title_to_url: title={title} md5={md5}')
  ext = title.split('.')[-1]
  baseurl = 'https://upload.wikimedia.org/wikipedia/commons/'
  if ext == 'svg':
    url = f'{baseurl}thumb/{md5[:1]}/{md5[:2]}/{quote(title)}/{width}px-${quote(title)}.png'
  elif ext in ('tif', 'tiff'):
    url = f'{baseurl}thumb/{md5[:1]}/{md5[:2]}/{quote(title)}/{width}px-${quote(title)}.jpg'
  else:
    url = f'{baseurl}thumb/{md5[:1]}/{md5[:2]}/{quote(title)}/{width}px-${quote(title)}' if width is None else f'{baseurl}{md5[:1]}/{md5[:2]}/{quote(title)}'
  return url

def _get_wd_image_url(qid):
  url = image_urls.get(qid)
  if not url:
    query = f'SELECT ?image WHERE {{ wd:{qid} wdt:P18 ?image . }}'
    resp = requests.get(
      f'https://query.wikidata.org/sparql?query={quote(query)}',
      headers = {
        'Content-Type': 'application/x-www-form-urlencoded', 
        'Accept': 'application/sparql-results+json',
        'User-Agent': 'Juncture Client'
      }
    )
    if resp.status_code == 200:
      results = resp.json()['results']['bindings']
      urls = [rec['image']['value'] for rec in results]
      title = urls[0].split('/')[-1].replace('File:','') if len(urls) > 0 else None
      if title:
        url = wc_title_to_url(title)
        image_urls[qid] = url
  return url
      
def manifestid_to_url(manifestid):
  qid = manifestid[3:]
  return _get_wd_image_url(qid)

def get_iiif_metadata(**kwargs):
  manifestid = kwargs.get('manifestid')
  qid = manifestid[3:]
  image_url = _get_wd_image_url(qid)
  title = unquote(image_url.split('/')[-1]).replace(' ','_')
  start = now()
  
  dro_qid = None
  props = {}
  props['wc_metadata'] = _get_wc_metadata(title)
  if 'pageid' in props['wc_metadata']:
    props['wc_entity'] = _get_wc_entity(props['wc_metadata']['pageid'])
    dro_qid = _digital_representation_of(props['wc_entity'])
    props['dro_entity'] = _get_wd_entity(dro_qid) if dro_qid else None
  
  imageinfo = props['wc_metadata']['imageinfo'][0] if 'imageinfo' in props['wc_metadata'] else {}
  extmetadata = imageinfo['extmetadata'] if 'extmetadata' in imageinfo else {}

  logger.debug(json.dumps(props.get('wc_entity', {}), indent=2))

  lang = 'none'
  
  label = _extract_text(extmetadata['ObjectName']['value']) if 'ObjectName' in extmetadata else None
  summary = _extract_text(extmetadata['ImageDescription']['value']) if 'ImageDescription' in extmetadata else None
  
  license_str = None
  license_code = None
  license_url = None
  license_label = None
  for fld in ('LicenseShortName', 'License'):
    if fld in extmetadata:
      license_str = extmetadata[fld]['value'].upper()
      break
  if license_str:
    _match = re.search(r'-?(\d\.\d)\s*$', license_str)
    version = _match[1] if _match else None
    license_code = re.sub(r'-?\d\.\d\s*$', '', license_str).strip().replace(' ','-')
    if license_code in licenses:
      license_url = re.sub(r'\/\d\.\d/', f'/{version}/', licenses[license_code]['url'])
      license_label = licenses[license_code]['label']
    logger.debug(f'license_code={license_code} version={version} url={license_url} label={license_label}')

  author = None
  for fld in ['Attribution', 'Artist']:
    if fld in extmetadata:
      author = extmetadata[fld]['value'].replace('<big>','').replace('</big>','')
      break
  
  attribution_statement = f'Image <em>{label}</em> provided by {author} under a <a href="{license_url}">{license_label} ({license_code.replace("CC-", "CC ")})</a> license'

  entity_data = props.get('wc_entity')
  if not entity_data: entity_data = {'labels': {}, 'descriptions': {}, 'statements': {}}
  
  created = entity_data['statements']['P571'][0]['mainsnak']['datavalue']['value']['time'][1:] if 'P571' in entity_data['statements'] else None # inception
  
  location_coords = None
  location_id = None
  location_label = None
  location_description = None
  if 'P9149' in entity_data['statements']: # coordinates of depicted place
    prop = entity_data['statements']['P9149'][0]['mainsnak']['datavalue']['value']
    location_coords = [prop['latitude'], prop['longitude']]
  elif 'P1259' in entity_data['statements']: # coordinates of the point of view
    prop = entity_data['statements']['P1259'][0]['mainsnak']['datavalue']['value']
    location_coords = [prop['latitude'], prop['longitude']]
  elif 'P1071' in entity_data['statements']: # location of creation
    location_id = entity_data['statements']['P1071'][0]['mainsnak']['datavalue']['value']['id']
    location_label, location_description, location_coords = _get_location_data(location_id, lang)
  elif 'P921' in entity_data['statements']: # main subject
    location_id = entity_data['statements']['P921'][0]['mainsnak']['datavalue']['value']['id']
    location_label, location_description, location_coords = _get_location_data(location_id, lang)

  dro = None
  if dro_qid:
      dro_label = _get_entity_labels([dro_qid], lang).get(dro_qid, dro_qid)
      dro = f'<a href="https://www.wikidata.org/entity/{dro_qid}">{dro_label}</a>'
    
  # camera, exposure, mode, size
  make = None
  if 'P4082' in entity_data['statements']:
    make_qids = [item['mainsnak']['datavalue']['value']['id'] for item in entity_data['statements']['P4082']]
    make_labels = _get_entity_labels(make_qids, lang)
    make = '; '.join([make_labels[qid] for qid in make_qids])
  focal_length =  int(float(entity_data['statements']['P2151'][0]['mainsnak']['datavalue']['value']['amount'].replace('+','').replace('-',''))) if 'P2151' in entity_data['statements'] else None
  exposure_time = float(entity_data['statements']['P6757'][0]['mainsnak']['datavalue']['value']['amount'].replace('+','').replace('-','')) if 'P6757' in entity_data['statements'] else None
  f_number = float(entity_data['statements']['P6790'][0]['mainsnak']['datavalue']['value']['amount'].replace('+','').replace('-','')) if 'P6790' in entity_data['statements'] else None
  iso = int(entity_data['statements']['P6789'][0]['mainsnak']['datavalue']['value']['amount'].replace('+','').replace('-','')) if 'P6789' in entity_data['statements'] else None
  
  depicts = [item['mainsnak']['datavalue']['value']['id'] for item in entity_data['statements']['P180']] if 'P180' in entity_data['statements'] else []
  if len(depicts) > 0:
    labels = _get_entity_labels(depicts, lang)
    depicts = [f'<a href="https://www.wikidata.org/entity/{qid}">{labels[qid]}</a>' for qid in depicts]
    
  metadata = {
    'language': lang,
    'label': label,
    'metadata': [
      { 'label': { lang: [ 'title' ] }, 'value': { lang: [ label ] }},
      { 'label': { lang: [ 'author' ] }, 'value': { lang: [ author ] }},
      { 'label': { lang: [ 'source' ] }, 'value': { lang: [ f'https://commons.wikimedia.org/wiki/File:{title}' ] } }
    ]
  }
  if summary:
    metadata['summary'] = summary

  metadata['rights'] = license_url
  if license_code not in ('PD', 'PUBLIC DOMAIN', 'PDM'):
    metadata['requiredStatement'] = {
      'label': { lang: [ 'attribution' ] },
      'value': { lang: [ attribution_statement ] }
    }
  
  if depicts:
    metadata['metadata'].append({ 'label': { lang: [ 'depicts' ] }, 'value': { lang: depicts }})
    
  if dro:
    metadata['metadata'].append({ 'label': { lang: [ 'digital_representation_of' ] }, 'value': { lang: [ dro ] }})

  if created:
    metadata['created'] = created
  
  if location_coords:
    metadata['location'] = {'coords': location_coords, 'id': location_id, 'label': location_label, 'description': location_description}

  if make:
    metadata['metadata'].append({ 'label': { lang: [ 'camera' ] }, 'value': { lang: [ make ] }})
  
  exposure = []
  if focal_length: exposure.append(f"{focal_length}mm")
  if exposure_time: exposure.append(f"1/{round(1/exposure_time)}s")
  if f_number: exposure.append(f"f/{f_number}")
  if iso: exposure.append(f"ISO {iso}")  
  if len(exposure) > 0:
    metadata['metadata'].append({ 'label': { lang: [ 'exposure' ] }, 'value': { lang: [ ' '.join(exposure) ] }})

  logger.debug(f'get_iiif_metadata: manifestid={manifestid} elapsed={round(now()-start,3)}')
  return metadata