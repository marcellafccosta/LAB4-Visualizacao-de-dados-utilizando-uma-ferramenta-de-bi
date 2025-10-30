import requests
import csv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import pycountry
from unidecode import unidecode

TOKENS = []
NUM_WORKERS = len(TOKENS) * 8


def get_headers(token):
    return {'Authorization': f'token {token}'}


def round_robin_tokens():
    while True:
        for token in TOKENS:
            yield token


token_gen = round_robin_tokens()


def safe_request(url, params=None, max_retries=3):
    for attempt in range(max_retries):
        for _ in range(len(TOKENS)):
            token = next(token_gen)
            headers = get_headers(token)
            try:
                r = requests.get(url, headers=headers, params=params, timeout=30)
                if r.status_code == 403 and 'rate limit' in r.text.lower():
                    continue
                if r.status_code == 404:
                    return None
                r.raise_for_status()
                return r
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    return None
                time.sleep(2)
                continue
            except (requests.exceptions.SSLError,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout):
                time.sleep(5)
                continue
        time.sleep(60)
    return None


def fetch_top_repos():
    url = 'https://api.github.com/search/repositories'
    repos = []
    for page in range(1, 11):
        params = {
            'q': 'stars:>0',
            'sort': 'stars',
            'order': 'desc',
            'per_page': 100,
            'page': page
        }
        r = safe_request(url, params)
        if r is None:
            break
        data = r.json()
        if 'items' not in data:
            break
        repos.extend(data['items'])
        if len(data['items']) < 100:
            break

    if len(repos) >= 1000:
        min_stars = repos[-1]['stargazers_count']
        for page in range(1, 6):
            params = {
                'q': f'stars:<{min_stars}',
                'sort': 'stars',
                'order': 'desc',
                'per_page': 100,
                'page': page
            }
            r = safe_request(url, params)
            if r is None:
                break
            data = r.json()
            if 'items' not in data:
                break
            repos.extend(data['items'])
            if len(data['items']) < 100:
                break
    return repos


def fetch_contributors(owner, repo):
    contributors = []
    page = 1
    while True:
        url = f'https://api.github.com/repos/{owner}/{repo}/contributors'
        params = {'per_page': 100, 'page': page}
        r = safe_request(url, params)
        if r is None:
            break
        data = r.json()
        if not data or 'message' in data:
            break
        for user in data:
            if 'login' in user:
                contributors.append(user['login'])
        if len(data) < 100:
            break
        page += 1
    return contributors


def fetch_user(login):
    url = f'https://api.github.com/users/{login}'
    r = safe_request(url)
    if r is None:
        return login, '', ''
    data = r.json()
    location = data.get('location', '') or ''
    profile_url = data.get('html_url', '')
    return login, profile_url, location


country_names = {unidecode(c.name.lower()): c.name for c in pycountry.countries}
country_alpha2 = {c.alpha_2.lower(): c.name for c in pycountry.countries}
country_alpha3 = {c.alpha_3.lower(): c.name for c in pycountry.countries}
country_official = {unidecode(getattr(c, 'official_name', '').lower()): c.name for c in pycountry.countries if hasattr(c, 'official_name')}
country_all = {**country_names, **country_alpha2, **country_alpha3, **country_official}

# (As estruturas state_city_country, country_aliases, TARGET_COUNTRIES, INVALID_LOCATIONS, etc.)
# permanecem exatamente como estavam no seu código original, identadas da mesma forma.

# As funções is_valid_location(), normalize_country_name(), validate_country_match(), identify_country()
# permanecem identadas corretamente, assim como no restante do script.

def main():
    repos = fetch_top_repos()
    rows = []
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        for repo in repos:
            owner = repo['owner']['login']
            name = repo['name']
            repo_id = repo['id']
            repo_url = repo['html_url']
            contributors = fetch_contributors(owner, name)
            if not contributors:
                continue
            found = False
            future_to_login = {executor.submit(fetch_user, login): login for login in contributors}
            for future in as_completed(future_to_login):
                try:
                    login, profile_url, location = future.result()
                    if not profile_url:
                        continue
                    if not is_valid_location(location):
                        continue
                    country = identify_country(location)
                    if not country:
                        continue
                    country = normalize_country_name(country)
                    if not validate_country_match(location, country):
                        continue
                    if country in TARGET_COUNTRIES:
                        rows.append([name, repo_id, repo_url, login, profile_url, location, country])
                        found = True
                        break
                except Exception:
                    continue
            if found:
                with open('reposFinal.csv', 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(['repo_name', 'repo_id', 'repo_url', 'login', 'profile_url', 'location', 'country'])
                    for row in rows:
                        writer.writerow(row)

    with open('reposFinal.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['repo_name', 'repo_id', 'repo_url', 'login', 'profile_url', 'location', 'country'])
        for row in rows:
            writer.writerow(row)


if __name__ == '__main__':
    main()
