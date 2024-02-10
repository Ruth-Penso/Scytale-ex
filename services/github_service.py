import json
import time
from typing import List

from github import Github
from github.Repository import Repository

from configuration.config import BASE_URL
from configuration.consts import ORG_NAME, REPO_INFO, PULL_REQUESTS
from services.service import Service


class GithubService(Service):
    def __init__(self):
        self.connector = Github(base_url=BASE_URL, verify=False)

    def extract(self, sc):
        try:
            org = self.connector.get_organization(ORG_NAME)
            return org.get_repos()
        except Exception as e:
            time.sleep(60)
            return self.extract(sc)

    def write(self, data, file_format=None, path=None) -> None:
        repos_with_prs = {}
        for repo in data:
            repos_with_prs[repo.name] = {
                REPO_INFO: repo.raw_data,
                PULL_REQUESTS: self._get_pull_requests(repo)
            }
            self._save_to_json(repos_with_prs[repo.name], f"{repo.name}.json")

    @classmethod
    def _get_pull_requests(cls, repo: Repository) -> List:
        pull_requests = repo.get_pulls(state='all')
        return [pr.raw_data for pr in pull_requests]

    @classmethod
    def _save_to_json(cls, data: dict, filename: str) -> None:
        with open(filename, 'w') as json_file:
            json.dump(data, json_file)
            json_file.write('\n')
