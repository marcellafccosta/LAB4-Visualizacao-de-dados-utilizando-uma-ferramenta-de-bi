

import requests
import csv
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

TOKENS = [
]
NUM_WORKERS = len(TOKENS) * 8

def get_headers(token):
 return {'Authorization': f'token {token}'}

def round_robin_tokens():
 while True:
     for token in TOKENS:
         yield token

token_gen = round_robin_tokens()

def safe_request(url, params=None):
 for _ in range(len(TOKENS)):
     token = next(token_gen)
     headers = get_headers(token)
     r = requests.get(url, headers=headers, params=params)
     if r.status_code == 403 and 'rate limit' in r.text.lower():
         continue
     r.raise_for_status()
     return r
 time.sleep(60)
 return safe_request(url, params)

def get_prs_stats(owner, repo):
 prs_opened = prs_merged = 0
 time_to_merge = []
 page = 1
 while True:
     url = f'https://api.github.com/repos/{owner}/{repo}/pulls'
     params = {'state': 'all', 'per_page': 100, 'page': page}
     r = safe_request(url, params)
     prs = r.json()
     if not prs or 'message' in prs:
         break
     for pr in prs:
         prs_opened += 1
         if pr.get('merged_at'):
             prs_merged += 1
             created = pr['created_at']
             merged = pr['merged_at']
             dt_created = datetime.strptime(created, '%Y-%m-%dT%H:%M:%SZ')
             dt_merged = datetime.strptime(merged, '%Y-%m-%dT%H:%M:%SZ')
             time_to_merge.append((dt_merged - dt_created).total_seconds() / 3600)
     if len(prs) < 100:
         break
     page += 1
 avg_time_to_merge = round(sum(time_to_merge)/len(time_to_merge), 2) if time_to_merge else ''
 return prs_opened, prs_merged, avg_time_to_merge

def get_commits_count(owner, repo):
 url = f'https://api.github.com/repos/{owner}/{repo}/commits?per_page=1'
 r = safe_request(url)
 if 'Link' in r.headers:
     last_link = [l for l in r.headers['Link'].split(',') if 'rel="last"' in l]
     if last_link:
         last_url = last_link[0].split(';')[0].strip()[1:-1]
         count = int(last_url.split('page=')[1].split('&')[0])
         return count
 return len(r.json())

def get_contributors_count(owner, repo):
 url = f'https://api.github.com/repos/{owner}/{repo}/contributors?per_page=1&anon=true'
 r = safe_request(url)
 if 'Link' in r.headers:
     last_link = [l for l in r.headers['Link'].split(',') if 'rel="last"' in l]
     if last_link:
         last_url = last_link[0].split(';')[0].strip()[1:-1]
         count = int(last_url.split('page=')[1].split('&')[0])
         return count
 return len(r.json())

def get_release_count(owner, repo):
 url = f'https://api.github.com/repos/{owner}/{repo}/releases?per_page=1'
 r = safe_request(url)
 if 'Link' in r.headers:
     last_link = [l for l in r.headers['Link'].split(',') if 'rel="last"' in l]
     if last_link:
         last_url = last_link[0].split(';')[0].strip()[1:-1]
         count = int(last_url.split('page=')[1].split('&')[0])
         return count
 return len(r.json())

def get_maintainers_count(owner, repo):
 url = f'https://api.github.com/repos/{owner}/{repo}/collaborators?per_page=100'
 r = safe_request(url)
 if r.status_code == 200:
     return len(r.json())
 return ''

def get_active_days(owner, repo):
 url = f'https://api.github.com/repos/{owner}/{repo}/commits?per_page=100'
 days = set()
 page = 1
 while page <= 10:
     r = safe_request(url + f"&page={page}")
     data = r.json()
     if not data or 'message' in data:
         break
     for commit in data:
         date = commit['commit']['author']['date'][:10]
         days.add(date)
     if len(data) < 100:
         break
     page += 1
 return len(days)

def get_time_to_first_response(owner, repo):
 url = f'https://api.github.com/repos/{owner}/{repo}/issues?state=all&per_page=20'
 r = safe_request(url)
 issues = r.json()
 times = []
 for issue in issues:
     if issue.get('comments', 0) > 0:
         created = datetime.strptime(issue['created_at'], '%Y-%m-%dT%H:%M:%SZ')
         comments_url = issue['comments_url']
         rc = safe_request(comments_url)
         comments = rc.json()
         if comments:
             first_comment = datetime.strptime(comments[0]['created_at'], '%Y-%m-%dT%H:%M:%SZ')
             times.append((first_comment - created).total_seconds() / 3600)
 avg_time = round(sum(times)/len(times), 2) if times else ''
 return avg_time

def process_repo_from_url(repo_url):
 try:
     parts = repo_url.rstrip('/').split('/')
     owner = parts[-2]
     name = parts[-1]
 except Exception as e:
     return None
 repo_api_url = f'https://api.github.com/repos/{owner}/{name}'
 try:
     r = safe_request(repo_api_url)
     repo = r.json()
 except Exception as e:
     return None

 description = (repo.get('description') or '').replace('\n', ' ').replace('\r', ' ')
 try:
     prs_opened, prs_merged, avg_time_to_merge = get_prs_stats(owner, name)
     commits_count = get_commits_count(owner, name)
     contributors_count = get_contributors_count(owner, name)
     release_count = get_release_count(owner, name)
     maintainers_count = get_maintainers_count(owner, name)
     active_days = get_active_days(owner, name)
     time_to_first_response = get_time_to_first_response(owner, name)
 except Exception as e:
     prs_opened = prs_merged = avg_time_to_merge = commits_count = contributors_count = release_count = maintainers_count = active_days = time_to_first_response = ''
 return [
     name, owner, repo.get('full_name', ''), repo_url, description, repo.get('created_at', ''), repo.get('updated_at', ''), repo.get('language', ''),
     ','.join(repo.get('topics', [])),
     repo.get('stargazers_count', ''), repo.get('forks_count', ''),
     prs_opened, prs_merged, commits_count, contributors_count, active_days,
     time_to_first_response, avg_time_to_merge, release_count, maintainers_count
 ]

def main():
 input_csv = 'reposFinal.csv'
 repos_urls = []
 with open(input_csv, newline='', encoding='utf-8') as f:
     reader = csv.DictReader(f)
     for row in reader:
         repo_url = row['repo_url']
         repos_urls.append(repo_url)

 with open('repos_metrics.csv', 'w', newline='', encoding='utf-8') as f:
     writer = csv.writer(f)
     writer.writerow([
         'repo_name', 'repo_owner', 'full_name', 'repo_url', 'description', 'created_at', 'updated_at', 'language_primary', 'topics',
         'stars_count', 'forks_count',
         'prs_opened_count', 'prs_merged_count', 'commits_count', 'contributors_count', 'active_days',
         'time_to_first_response', 'time_to_merge', 'release_count', 'maintainers_count'
     ])
     with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
         futures = [executor.submit(process_repo_from_url, repo_url) for repo_url in repos_urls]
         for future in as_completed(futures):
             result = future.result()
             if result:
                 writer.writerow(result)

if __name__ == '__main__':
 main()