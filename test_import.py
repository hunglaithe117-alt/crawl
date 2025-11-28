import sys
import os
print("Start")
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
try:
    from github_api_client import GitHubAPIClient
    print("Imported GitHubAPIClient")
except Exception as e:
    print(f"Failed to import: {e}")
print("End")