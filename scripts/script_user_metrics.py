import asyncio
import aiohttp
import pandas as pd
from datetime import datetime
from tqdm.asyncio import tqdm_asyncio
import itertools
import time

TOKENS = [
]

token_cycle = itertools.cycle(TOKENS)
BASE_URL = "https://api.github.com"
MAX_CONCURRENT = len(TOKENS) * 3
semaphore = asyncio.Semaphore(MAX_CONCURRENT)

async def fetch(session, url, retries=3):
    async with semaphore:
        for attempt in range(retries):
            token = next(token_cycle)
            headers = {
                "Authorization": f"token {token}",
                "Accept": "application/vnd.github+json"
            }
            try:
                async with session.get(url, headers=headers, timeout=30) as resp:
                    if resp.status == 403:
                        await asyncio.sleep(0.5)
                        continue
                    if resp.status == 404:
                        return None
                    if resp.status == 200:
                        return await resp.json()
                    await asyncio.sleep(0.5)
            except asyncio.TimeoutError:
                if attempt < retries - 1:
                    await asyncio.sleep(1)
                    continue
            except Exception as e:
                if attempt < retries - 1:
                    await asyncio.sleep(1)
                    continue
        return None

async def fetch_all_pages(session, base_url, max_pages=10):
    all_data = []
    first_page = await fetch(session, f"{base_url}&per_page=100&page=1")
    if not first_page:
        return []
    
    all_data.extend(first_page if isinstance(first_page, list) else [first_page])
    if len(first_page) < 100:
        return all_data
    
    tasks = []
    for page in range(2, max_pages + 1):
        url = f"{base_url}&per_page=100&page={page}"
        tasks.append(fetch(session, url))
    
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if result and isinstance(result, list) and len(result) > 0:
                all_data.extend(result)
            elif result and len(result) < 100:
                break
    return all_data

async def get_user_detailed_metrics(session, user_data):
    try:
        login = user_data['login']
        repo_name = user_data['repo_name']
        repo_url = user_data['repo_url']
        location = user_data.get('location', '')
        country = user_data.get('country', '')
        
        repo_parts = repo_url.replace('https://github.com/', '').strip('/').split('/')
        if len(repo_parts) < 2:
            return None
        
        repo_owner = repo_parts[0]
        repo = repo_parts[1]

        user_profile_url = f"{BASE_URL}/users/{login}"
        prs_search_url = f"{BASE_URL}/search/issues?q=type:pr+author:{login}+repo:{repo_owner}/{repo}"
        commits_search_url = f"{BASE_URL}/search/commits?q=author:{login}+repo:{repo_owner}/{repo}"
        issues_search_url = f"{BASE_URL}/search/issues?q=type:issue+author:{login}+repo:{repo_owner}/{repo}"
        user_repos_url = f"{BASE_URL}/users/{login}/repos?per_page=100"

        basic_results = await asyncio.gather(
            fetch(session, user_profile_url),
            fetch(session, prs_search_url),
            fetch(session, commits_search_url),
            fetch(session, issues_search_url),
            fetch(session, user_repos_url),
            return_exceptions=True
        )

        user_profile, prs_data, commits_data, issues_data, user_repos = basic_results

        prs_opened = prs_data.get('total_count', 0) if prs_data else 0
        commits_total = commits_data.get('total_count', 0) if commits_data else 0
        issues_opened = issues_data.get('total_count', 0) if issues_data else 0

        prs_url = f"{BASE_URL}/repos/{repo_owner}/{repo}/pulls?state=all"
        all_prs = await fetch_all_pages(session, prs_url, max_pages=20)
        
        prs_merged = 0
        pr_times = []
        pr_requested_as_reviewer_count = 0
        
        user_prs = [pr for pr in all_prs if pr.get('user', {}).get('login') == login]
        
        for pr in user_prs:
            if pr.get('merged_at'):
                prs_merged += 1
                try:
                    created = datetime.fromisoformat(pr['created_at'].replace('Z', '+00:00'))
                    merged = datetime.fromisoformat(pr['merged_at'].replace('Z', '+00:00'))
                    days_to_merge = (merged - created).total_seconds() / (24 * 3600)
                    pr_times.append(days_to_merge)
                except:
                    pass

        for pr in all_prs:
            if any(r.get('login') == login for r in pr.get('requested_reviewers', [])):
                pr_requested_as_reviewer_count += 1

        pr_accept_rate = (prs_merged / prs_opened * 100) if prs_opened else 0
        avg_time_to_merge = sum(pr_times) / len(pr_times) if pr_times else 0

        stars_own_repos = 0
        if user_repos and isinstance(user_repos, list):
            stars_own_repos = sum(repo_.get('stargazers_count', 0) for repo_ in user_repos)

        contribution_period = 0
        activity_frequency = 0
        
        if commits_total > 0:
            commits_url = f"{BASE_URL}/repos/{repo_owner}/{repo}/commits?author={login}&per_page=100"
            commits = await fetch_all_pages(session, commits_url, max_pages=5)
            
            if commits:
                try:
                    dates = []
                    for c in commits:
                        if 'commit' in c and 'author' in c['commit'] and 'date' in c['commit']['author']:
                            date = datetime.fromisoformat(c['commit']['author']['date'].replace('Z', '+00:00'))
                            dates.append(date)
                    
                    if len(dates) > 1:
                        contribution_period = (max(dates) - min(dates)).days
                        activity_frequency = commits_total / contribution_period if contribution_period else commits_total
                    else:
                        contribution_period = 1
                        activity_frequency = commits_total
                except Exception as e:
                    pass
        pr_requested_as_reviewer_rate = (pr_requested_as_reviewer_count / len(all_prs) * 100) if all_prs else 0

        result = {
            'repo_name': repo_name,
            'repo_url': repo_url,
            'login': login,
            'profile_url': f'https://github.com/{login}',
            'location': location,
            'country': country,
            'prs_opened': prs_opened,
            'prs_merged': prs_merged,
            'pr_accept_rate': round(pr_accept_rate, 2),
            'avg_time_to_merge': round(avg_time_to_merge, 2),
            'commits_total': commits_total,
            'issues_opened': issues_opened,
            'stars_own_repos': stars_own_repos,
            'contribution_period': contribution_period,
            'activity_frequency': round(activity_frequency, 2),
            'pr_requested_as_reviewer_rate': round(pr_requested_as_reviewer_rate, 2)
        }
        return result

    except Exception as e:
        return {
            **user_data,
            'prs_opened': 0,
            'prs_merged': 0,
            'pr_accept_rate': 0,
            'avg_time_to_merge': 0,
            'commits_total': 0,
            'issues_opened': 0,
            'stars_own_repos': 0,
            'contribution_period': 0,
            'activity_frequency': 0,
            'pr_requested_as_reviewer_rate': 0,
            'error': str(e)
        }

async def main():
    input_csv = 'users_countries.csv'
    output_csv = 'users_metrics.csv'
    
    try:
        df = pd.read_csv(input_csv)
        users = df.to_dict(orient='records')
    except Exception as e:
        return
    
    results = []
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, limit_per_host=10)
    timeout = aiohttp.ClientTimeout(total=120)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        batch_size = 50
        for i in range(0, len(users), batch_size):
            batch = users[i:i+batch_size]
            tasks = [get_user_detailed_metrics(session, user) for user in batch]
            
            batch_results = []
            for f in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc=f"Lote {i//batch_size + 1}"):
                result = await f
                if result:
                    batch_results.append(result)
            
            results.extend(batch_results)
            if results:
                pd.DataFrame(results).to_csv('users_metrics_partial.csv', index=False)
            if i + batch_size < len(users):
                await asyncio.sleep(2)
    
    if results:
        final_df = pd.DataFrame(results)
        final_df.to_csv(output_csv, index=False)

if __name__ == "__main__":
    asyncio.run(main())
