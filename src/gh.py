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

import requests
logging.getLogger('requests').setLevel(logging.INFO)

GH_ACCESS_TOKEN = os.environ.get('GH_ACCESS_TOKEN')

def get_gh_file_by_url(url):
    start = now()
    content = sha = None
    resp = requests.get(url, headers={
        'Authorization': f'Token {GH_ACCESS_TOKEN}',
        'Accept': 'application/vnd.github.v3+json',
        'User-agent': 'JSTOR Labs visual essays client'
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
    url = f'https://api.github.com/repos/{acct}/{repo}/contents{path}?ref={ref}'
    logger.debug(f'get_gh_file: acct={acct} repo={repo} ref={ref} path={path} elapsed={round(now()-start,3)}')
    return get_gh_file_by_url(url)[0]

def get_gh_last_commit(acct, repo, ref, path=None):
    start = now()
    url = f'https://api.github.com/repos/{acct}/{repo}/commits?sha={ref}&page=1&per_page=1{"&path="+path if path else ""}'
    resp = requests.get(url, headers={
        'Authorization': f'Token {GH_ACCESS_TOKEN}',
        'Accept': 'application/vnd.github.v3+json',
        'User-agent': 'JSTOR Labs visual essays client'
    })
    commits = resp.json() if resp.status_code == 200 else []
    last_commit_date = datetime.datetime.strptime(commits[0]['commit']['author']['date'], '%Y-%m-%dT%H:%M:%SZ') if len(commits) > 0 else None
    logger.debug(f'get_gh_last_commit: acct={acct} repo={repo} ref={ref} path={path} resp={resp.status_code} last_commit_date={last_commit_date} elapsed={round(now()-start,3)}')
    return last_commit_date

def gh_dir_list(acct, repo, path=None, ref=None):
    start = now()
    url = f'https://api.github.com/repos/{acct}/{repo}/contents/{path if path else ""}'
    if ref:
        url += f'?ref={ref}'
    resp = requests.get(url, headers={
        'Authorization': f'Token {GH_ACCESS_TOKEN}',
        'Accept': 'application/vnd.github.v3+json',
        'User-agent': 'JSTOR Labs visual essays client'
    })
    # logger.info(json.dumps(resp.json(),indent=2))
    logger.debug(f'gh_dir_list: acct={acct} repo={repo} path={path} elapsed={round(now()-start,3)}')
    return [item['name'] for item in resp.json()] if resp.status_code == 200 else []

def gh_repo_info(acct, repo):
    start = now()
    url = f'https://api.github.com/repos/{acct}/{repo}'
    resp = requests.get(url, headers={
        'Authorization': f'Token {GH_ACCESS_TOKEN}',
        'Accept': 'application/vnd.github.v3+json',
        'User-agent': 'JSTOR Labs visual essays client'
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
        'Authorization': f'Token {GH_ACCESS_TOKEN}',
        'Accept': 'application/vnd.github.v3+json',
        'User-agent': 'JSTOR Labs visual essays client'
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
