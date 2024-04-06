#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
logging.basicConfig(format='%(asctime)s : %(filename)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

import os
import base64
import json
from time import time as now
import datetime
from urllib.parse import quote
import yaml

import requests
logging.getLogger('requests').setLevel(logging.INFO)

GH_UNSCOPED_TOKEN = os.environ.get('GH_UNSCOPED_TOKEN')

def get_gh_file_by_url(url):
  start = now()
  content = sha = None
  resp = requests.get(url, headers={
    'Authorization': f'Token {GH_UNSCOPED_TOKEN}',
    'Accept': 'application/vnd.github.v3+json',
    'User-agent': 'MDPress client'
  })
  logger.debug(f'get_gh_file_by_url: url={url} resp={resp.status_code}')
  if resp.status_code == 200:
    resp = resp.json()
    content = base64.b64decode(resp['content']).decode('utf-8')
    sha = resp['sha']
  logger.debug(f'get_gh_file_by_url: url={url} elapsed={round(now()-start,3)}')
  return content, url, sha

def get_gh_file(acct, repo, ref, path):
  start = now()
  url = f'https://api.github.com/repos/{acct}/{repo}/contents/{path}?ref={ref}'
  logger.debug(f'get_gh_file: acct={acct} repo={repo} ref={ref} path={path} elapsed={round(now()-start,3)}')
  return get_gh_file_by_url(url)[0]

def get_gh_last_commit(acct, repo, ref, path=None):
  start = now()
  url = f'https://api.github.com/repos/{acct}/{repo}/commits?sha={ref}&page=1&per_page=1{"&path="+path if path else ""}'
  resp = requests.get(url, headers={
    'Authorization': f'Token {GH_UNSCOPED_TOKEN}',
    'Accept': 'application/vnd.github.v3+json',
    'User-agent': 'MDPress client'
  })
  commits = resp.json() if resp.status_code == 200 else []
  last_commit_date = datetime.datetime.strptime(commits[0]['commit']['author']['date'], '%Y-%m-%dT%H:%M:%SZ') if len(commits) > 0 else None
  logger.debug(f'get_gh_last_commit: acct={acct} repo={repo} ref={ref} path={path} resp={resp.status_code} last_commit_date={last_commit_date} elapsed={round(now()-start,3)}')
  return last_commit_date

def gh_dir_list(acct, repo, path=None, ref=None):
  url = f'https://api.github.com/repos/{acct}/{repo}/contents/{path if path else ""}'
  if ref:
    url += f'?ref={ref}'
  resp = requests.get(url, headers={
    'Authorization': f'Token {GH_UNSCOPED_TOKEN}',
    'Accept': 'application/vnd.github.v3+json',
    'User-agent': 'MDPress client'
  })
  return resp.json() if resp.status_code == 200 else []

def gh_repo_info(acct, repo):
  start = now()
  url = f'https://api.github.com/repos/{acct}/{repo}'
  resp = requests.get(url, headers={
    'Authorization': f'Token {GH_UNSCOPED_TOKEN}',
    'Accept': 'application/vnd.github.v3+json',
    'User-agent': 'MDPress client'
  })
  repo_info = resp.json() if resp.status_code == 200 else {}
  logger.debug(json.dumps(repo_info, indent=2))
  logger.debug(f'gh_repo_info: acct={acct} repo={repo} elapsed={round(now()-start,3)}')
  return repo_info

def gh_user_info(login=None, acct=None, repo=None):
  start = now()
  if not login:
    login = gh_repo_info(acct, repo)['owner']['login']
  url = f'https://api.github.com/users/{login}'
  resp = requests.get(url, headers={
    'Authorization': f'Token {GH_UNSCOPED_TOKEN}',
    'Accept': 'application/vnd.github.v3+json',
    'User-agent': 'MDPress client'
  })
  user_info = resp.json() if resp.status_code == 200 else {}
  # logger.debug(json.dumps(user_info, indent=2))
  logger.debug(f'gh_user_info: login={login} acct={acct} repo={repo} elapsed={round(now()-start,3)}')
  return user_info

def get_default_branch(acct, repo):
  start = now()
  repo_info = gh_repo_info(acct, repo)
  logger.debug(f'get_default_branch: acct={acct} repo={repo} elapsed={round(now()-start,3)}')
  return repo_info['default_branch'] if repo_info else None

licenses = {
  # Creative Commons Licenses
  'PD': {'label': 'Public Domain', 'url': ''},
  'PUBLIC DOMAIN': {'label': 'Public Domain', 'url': ''},
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

def get_entity_labels(qids, lang='en'):
  values = ' '.join([f'(<http://www.wikidata.org/entity/{qid}>)' for qid in qids])
  query = f'SELECT ?item ?label WHERE {{ VALUES (?item) {{ {values} }} ?item rdfs:label ?label . FILTER (LANG(?label) = "{lang}" || LANG(?label) = "en") .}}'
  resp = requests.get(
    f'https://query.wikidata.org/sparql?query={quote(query)}',
    headers = {
      'Content-Type': 'application/x-www-form-urlencoded', 
      'Accept': 'application/sparql-results+json',
      'User-Agent': 'MDPress Client'
    }
  )
  return dict([(rec['item']['value'].split('/')[-1],rec['label']['value']) for rec in resp.json()['results']['bindings']]) if resp.status_code == 200 else {}

def manifestid_to_url(manifestid):
  acct, repo, *path = manifestid[3:].split('/')
  return f'https://raw.githubusercontent.com/{acct}/{repo}/main/{"/".join(path)}'
  
def get_iiif_metadata(**kwargs):
  manifestid = kwargs.get('manifestid')
  start = now()
  acct, repo, *path = manifestid[3:].split('/')
  repo_info = gh_repo_info(acct, repo)
  ref = repo_info['default_branch']
  user_info = gh_user_info(repo_info['owner']['login'])

  fname = path[-1].split('.')[0]
  label = fname.split('__')[0].replace('_',' ')
  license_code = fname.split('-')[-1] if fname.split('-')[-1] in licenses else 'CC-BY-SA'
  license_url = licenses[license_code]['url']
  license_label = licenses[license_code]['label']
  author = user_info.get('name') or user_info.get('login')
  author_url = user_info['html_url']
  attribution_statement = f'Image <em>{label}</em> provided by <a href="{author_url}">{author}</a> under a <a href="{license_url}">{license_label} ({license_code.replace("CC-", "CC ")})</a> license'
  
  path[-1] = '.'.join(path[-1].split('.')[:-1]) + '.yaml'
  gh_metadata = yaml.load(get_gh_file(acct, repo, ref, '/'.join(path)) or '', Loader=yaml.FullLoader) or {}
  lang = gh_metadata.get('language', 'en')
  
  metadata = {
    'language': lang,
    'label': label,
    'metadata': [
      { 'label': { lang: [ 'title' ] }, 'value': { lang: [ label ] }},
      { 'label': { lang: [ 'author' ] }, 'value': { lang: [ f'<a href="{author_url}">{author}</a>' ] }},
      { 'label': { lang: [ 'source' ] }, 'value': { lang: [ f'https://github.com/{acct}/{repo}/blob/{ref}/{"/".join(path)}' ] } }
    ]
  }
  metadata['rights'] = license_url
  if license_code not in ('PD', 'PUBLIC DOMAIN', 'PDM'):
    metadata['requiredStatement'] = {
      'label': { lang: [ 'attribution' ] },
      'value': { lang: [ attribution_statement ] }
    }
  
  for key in gh_metadata.keys():
    if key in ('label', 'metadata', 'navDate', 'orientation', 'provider', 'rights', 'requiredStatement', 'source', 'summary'):
      metadata[key] = gh_metadata[key]
    elif key in ('depicts', 'digital__representation_of'):
      qids = gh_metadata[key] if isinstance(gh_metadata[key], list) else [gh_metadata[key]]
      labels = get_entity_labels(qids, lang)
      metadata['metadata'].append({
        'label': { lang: [ key ] }, 
        'value': { lang: [ f'<a href="https://www.wikidata.org/entity/{qid}">{labels.get(qid, qid)}</a>' for qid in qids] } 
      })
    else:
      metadata['metadata'].append({ 'label': { lang: [ key ] }, 'value': { lang: [ gh_metadata[key] if isinstance(gh_metadata[key], list) else [gh_metadata[key]] ] } })

  logger.info(f'get_iiif_metadata: manifestid={manifestid} elapsed={round(now()-start,3)}')
  return metadata