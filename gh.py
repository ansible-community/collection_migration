"""GitHub App auth and helpers."""

import asyncio
from dataclasses import dataclass
import contextlib
import pathlib
from typing import Union

import gidgethub
from logzero import logger

from octomachinery.github.api.app_client import GitHubApp
from octomachinery.github.config.app import GitHubAppIntegrationConfig


@dataclass(frozen=True)
class GitHubOrgClient:
    """Wrapper for GH repos creation."""

    github_app_id: int
    github_app_private_key_path: Union[pathlib.Path, str]
    github_org_name: str

    def _get_github_app(self):
        """Initialize a GitHub App instance with creds."""
        github_app_config = GitHubAppIntegrationConfig(
            app_id=self.github_app_id,
            private_key=pathlib.Path(
                self.github_app_private_key_path
            ).expanduser().resolve().read_text(),

            app_name='Ansible Collection Migrator',
            app_version='1.0',
            app_url='https://github.com/ansible/collection_migration',
        )
        return GitHubApp(github_app_config)

    async def _get_github_client(self):
        """Return a GitHub API client for the target org."""
        github_app = self._get_github_app()
        try:
            github_app_installations = await github_app.get_installations()
        except gidgethub.BadRequest:
            error_msg = 'Invalid GitHub App credentials'
            logger.error(error_msg)
            raise LookupError(error_msg)
        target_github_app_installation = next(  # find the one
            (
                i for n, i in github_app_installations.items()
                if i._metadata.account['login'] == self.github_org_name
            ),
            None,
        )
        return target_github_app_installation.get_github_api_client()

    async def create_repo_if_not_exists(self, repo_name):
        """Ensure that the repo exists under the org."""
        github_api = await self._get_github_client()
        with contextlib.suppress(gidgethub.InvalidField):
            await github_api.post(
                f'/orgs/{self.github_org_name}/repos',
                data={'name': repo_name},
            )
            logger.info(
                'Repo %s has been created',
                f'https://github.com'
                f'/{self.github_org_name}'
                f'/{repo_name}.git'
            )

    async def get_org_repo_token(self, repo_name):
        """Return an access token once the repo exists."""
        await self.create_repo_if_not_exists(repo_name)
        return str((await self._get_github_client())._token)

    async def get_git_repo_token(self, repo_name):
        """Generate a Git repo URL with creds after ensuring repo existence."""
        gh_token = await self.get_org_repo_token(repo_name)
        return (
            f'https://x-access-token:{gh_token}@github.com'
            f'/{self.github_org_name}/{repo_name}.git'
        )

    def get_git_repo_write_uri(self, repo_name):
        """Get a Git repo URL with embedded creds synchronously."""
        return asyncio.run(self.get_git_repo_token(repo_name))
