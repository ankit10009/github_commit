import requests
import pandas as pd
import cx_Oracle
import time

# GitHub API Config
GITHUB_BASE_URL = "https://your-github-enterprise-instance/api/v3"
ACCESS_TOKEN = "your_personal_access_token"

# Oracle Database Config
ORACLE_DSN = cx_Oracle.makedsn("your_host", "your_port", service_name="your_service")
ORACLE_USER = "your_db_user"
ORACLE_PASSWORD = "your_db_password"

# API Headers
HEADERS = {
    "Authorization": f"token {ACCESS_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# Date Range for January 2025
SINCE_DATE = "2025-01-01T00:00:00Z"
UNTIL_DATE = "2025-01-31T23:59:59Z"

# Global Counter for API Calls
API_CALL_COUNT = 0
API_LIMIT = 5000  # Default limit (will be updated at runtime)
API_REMAINING = 5000
API_RESET_TIME = 0  # Epoch time when the rate limit resets

def get_authors_from_db():
    """Fetch authors from Oracle database."""
    connection = cx_Oracle.connect(ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN)
    cursor = connection.cursor()

    cursor.execute("SELECT github_username FROM authors_table")  # Update table name
    authors = [row[0] for row in cursor.fetchall()]

    cursor.close()
    connection.close()
    return authors

def update_rate_limit():
    """Fetch rate limit only when needed and update global counters."""
    global API_LIMIT, API_REMAINING, API_RESET_TIME, API_CALL_COUNT
    
    url = f"{GITHUB_BASE_URL}/rate_limit"
    response = requests.get(url, headers=HEADERS)
    API_CALL_COUNT += 1  # Track API calls
    
    if response.status_code == 200:
        rate_data = response.json()
        API_LIMIT = rate_data['rate']['limit']
        API_REMAINING = rate_data['rate']['remaining']
        API_RESET_TIME = rate_data['rate']['reset']
        print(f"Rate limit: {API_REMAINING}/{API_LIMIT}, resets at {API_RESET_TIME}")
    else:
        print("Failed to fetch rate limit.")

def check_rate_limit():
    """Check rate limit before making an API call. If near limit, wait until reset."""
    global API_REMAINING, API_RESET_TIME, API_CALL_COUNT

    if API_REMAINING <= 100:  # Only check if we're near the limit
        update_rate_limit()
    
    if API_REMAINING == 0:
        wait_time = API_RESET_TIME - int(time.time())  # Time until reset
        if wait_time > 0:
            print(f"Rate limit reached. Sleeping for {wait_time} seconds...")
            time.sleep(wait_time)  # Pause execution until reset
            update_rate_limit()  # Refresh limit after waiting

def get_repos(org):
    """Fetch repositories from an organization."""
    global API_CALL_COUNT
    check_rate_limit()
    
    url = f"{GITHUB_BASE_URL}/orgs/{org}/repos"
    response = requests.get(url, headers=HEADERS)
    API_CALL_COUNT += 1  # Track API call

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching repos: {response.status_code}")
        return []

def get_commits(owner, repo, author):
    """Fetch commits for a repository, filtering by author and date range."""
    global API_CALL_COUNT
    url = f"{GITHUB_BASE_URL}/repos/{owner}/{repo}/commits?author={author}&since={SINCE_DATE}&until={UNTIL_DATE}&per_page=100"
    
    commits = []
    page = 1

    while True:
        check_rate_limit()  # Handle rate limit
        
        paginated_url = f"{url}&page={page}"
        response = requests.get(paginated_url, headers=HEADERS)
        API_CALL_COUNT += 1  # Track API call

        if response.status_code == 200:
            batch = response.json()
            if not batch:  # No more commits
                break
            commits.extend(batch)
            page += 1
        else:
            print(f"Error fetching commits for {repo}: {response.status_code}")
            break

    return commits

# Fetch authors from the database
authors = get_authors_from_db()
ORG_NAME = "your_org"

# Initialize Rate Limit at Start
update_rate_limit()

# DataFrame to store results
commit_data = []

# Fetch repositories
repos = get_repos(ORG_NAME)

if repos:
    for repo in repos:
        repo_name = repo["name"]
        owner = repo["owner"]["login"]

        for author in authors:
            print(f"\nFetching commits for author: {author} in repo: {repo_name}")

            commits = get_commits(owner, repo_name, author)
            for commit in commits:
                commit_data.append({
                    "Repository": repo_name,
                    "Author": commit['commit']['author']['name'],
                    "Commit SHA": commit['sha'],
                    "Commit Message": commit['commit']['message'],
                    "Date": commit['commit']['author']['date']
                })

# Convert results to a DataFrame
df = pd.DataFrame(commit_data)

# Save to CSV
df.to_csv("github_commits_january_2025.csv", index=False)

print(f"\nData collection complete! {len(df)} commits saved to 'github_commits_january_2025.csv'.")
print(f"Total API Calls Used: {API_CALL_COUNT}/{API_LIMIT}")
