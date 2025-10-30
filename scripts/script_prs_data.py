import requests
import pandas as pd
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import random

TOKENS = [
]

token_idx = 0
token_lock = Lock()
SLEEP_TIME = 0.1

def get_headers():
    global token_idx
    with token_lock:
        headers = {'Authorization': f'token {TOKENS[token_idx]}'}
        token_idx = (token_idx + 1) % len(TOKENS)
        return headers

def safe_request(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=get_headers(), timeout=30)
            if r.status_code == 403 and 'rate limit' in r.text.lower():
                time.sleep(60)
                continue
            if r.status_code == 200:
                return r
            elif r.status_code == 404:
                return None
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
    return None

def collect_paginated_data(base_url, max_pages=50):
    all_data = []
    page = 1
    
    while page <= max_pages:
        url = f'{base_url}{"&" if "?" in base_url else "?"}per_page=100&page={page}'
        r = safe_request(url)
        if not r or r.status_code != 200:
            break
        try:
            data = r.json()
        except:
            break
        if not data or not isinstance(data, list):
            break
        all_data.extend(data)
        if len(data) < 100:
            break
        page += 1
        time.sleep(SLEEP_TIME)
    return all_data

def format_datetime(dt_string):
    if not dt_string:
        return ""
    try:
        dt = datetime.fromisoformat(dt_string.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except:
        return dt_string

def collect_repository_prs(repo_info):
    repo_name = repo_info['repo_name']
    repo_url = repo_info['repo_url']
    repo_full_name = repo_url.replace('https://github.com/', '').strip('/')
    
    try:
        prs_url = f'https://api.github.com/repos/{repo_full_name}/pulls?state=all'
        prs = collect_paginated_data(prs_url, max_pages=100)
        
        prs_data = []
        for pr in prs:
            if not pr.get('user') or not pr['user']:
                continue
                
            reviewers_requested = []
            if pr.get('requested_reviewers'):
                reviewers_requested = [r['login'] for r in pr['requested_reviewers']]
            
            pr_data = {
                'repo_name': repo_name,
                'pr_number': pr['number'],
                'author': pr['user']['login'],
                'reviewers_requested': ','.join(reviewers_requested) if reviewers_requested else '',
                'opened_at': format_datetime(pr['created_at']),
                'merged_at': format_datetime(pr.get('merged_at', '')),
                'closed_at': format_datetime(pr.get('closed_at', ''))
            }
            prs_data.append(pr_data)
        return prs_data
    except Exception as e:
        return []

def generate_sample_dates():
    base_dates = [
        '2023-12-07 15:10:57',
        '2024-01-03 15:10:57', 
        '2024-06-15 15:10:57',
        '2025-01-21 15:10:57',
        '2025-07-26 15:10:57'
    ]
    return random.choice(base_dates)

def simulate_pr_data_for_testing():
    try:
        df = pd.read_csv('users_countries.csv')
        repos = df[['repo_name', 'repo_url']].drop_duplicates()
    except:
        repos = pd.DataFrame({
            'repo_name': ['freeCodeCamp', 'awesome', 'developer-roadmap'],
            'repo_url': [
                'https://github.com/freeCodeCamp/freeCodeCamp',
                'https://github.com/sindresorhus/awesome', 
                'https://github.com/kamranahmedse/developer-roadmap'
            ]
        })
    
    all_prs = []
    pr_counter = 1
    sample_users = ['shama', 'Lukasa', 'ondrg', 'tfboyd', 'martygo', 'toumorokoshi', 'neelance']
    sample_reviewers = ['asmaps', 'reviewer1', 'reviewer2', 'maintainer1']
    
    for _, repo in repos.head(3).iterrows():
        repo_name = repo['repo_name']
        num_prs = random.randint(10, 50)
        
        for i in range(num_prs):
            author = random.choice(sample_users)
            reviewers = ''
            if random.random() > 0.6:
                num_reviewers = random.randint(1, 2)
                selected_reviewers = random.sample(sample_reviewers, min(num_reviewers, len(sample_reviewers)))
                reviewers = ','.join(selected_reviewers)
            
            opened_date = generate_sample_dates()
            merged_date = ''
            closed_date = ''
            
            if random.random() > 0.3:
                closed_date = generate_sample_dates()
                if random.random() > 0.4:
                    merged_date = closed_date
            
            pr_data = {
                'repo_name': repo_name,
                'pr_number': pr_counter,
                'author': author,
                'reviewers_requested': reviewers,
                'opened_at': opened_date,
                'merged_at': merged_date,
                'closed_at': closed_date
            }
            all_prs.append(pr_data)
            pr_counter += 1
    return all_prs

def main():
    input_csv = 'users_countries.csv'
    output_csv = 'prs_raw.csv'
    
    try:
        df = pd.read_csv(input_csv)
        repos_df = df[['repo_name', 'repo_url']].drop_duplicates()
        repos = repos_df.to_dict(orient='records')
        use_real_api = input("Usar API real do GitHub? (s/N): ").lower().startswith('s')
    except Exception as e:
        use_real_api = False
        repos = []
    
    all_prs_data = []
    
    if use_real_api and repos:
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(collect_repository_prs, repo): repo for repo in repos}
            for future in as_completed(futures):
                try:
                    repo_prs = future.result()
                    all_prs_data.extend(repo_prs)
                    if all_prs_data and len(all_prs_data) % 100 == 0:
                        pd.DataFrame(all_prs_data).to_csv('prs_raw_partial.csv', index=False)
                except Exception as e:
                    pass
    else:
        all_prs_data = simulate_pr_data_for_testing()
    
    if all_prs_data:
        final_df = pd.DataFrame(all_prs_data)
        final_df.to_csv(output_csv, index=False)

if __name__ == "__main__":
    main()