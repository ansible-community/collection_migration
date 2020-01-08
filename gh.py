"""GitHub App auth and helpers."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import contextlib
import os
import pathlib
from typing import Union

from aiohttp.client import ClientSession
import gidgethub
from logzero import logger

from octomachinery.github.api.app_client import GitHubApp
from octomachinery.github.config.app import GitHubAppIntegrationConfig


def provision_http_session(async_method):
    """Inject aiohttp client session into method keyword args."""
    async def async_method_wrapper(self, *args, **kwargs):
        async with ClientSession() as http_session:
            kwargs['http_session'] = http_session
            return await async_method(self, *args, **kwargs)
    return async_method_wrapper


@dataclass(frozen=True)
class GitHubOrgClient:
    """Wrapper for GH repos creation."""

    github_app_id: int
    github_app_private_key_path: Union[pathlib.Path, str]
    github_org_name: str

    deployment_rsa_pub_key: str

    def _read_app_id(self):
        if self.github_app_id is None:
            return int(os.environ['GITHUB_APP_IDENTIFIER'])
        return self.github_app_id

    def _read_private_key(self):
        if self.github_app_private_key_path is None:
            return os.environ['GITHUB_PRIVATE_KEY']
        return pathlib.Path(
            self.github_app_private_key_path,
        ).expanduser().resolve().read_text()

    def _get_github_app(self, http_session: ClientSession):
        """Initialize a GitHub App instance with creds."""
        github_app_config = GitHubAppIntegrationConfig(
            app_id=self._read_app_id(),
            private_key=self._read_private_key(),

            app_name='Ansible Collection Migrator',
            app_version='1.0',
            app_url='https://github.com/ansible/collection_migration',
        )
        return GitHubApp(github_app_config, http_session)

    async def _get_github_client(self, http_session: ClientSession):
        """Return a GitHub API client for the target org."""
        github_app = self._get_github_app(http_session)
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
        return target_github_app_installation.api_client

    @provision_http_session
    async def create_repo_if_not_exists(
            self, repo_name: str,
            *,
            http_session: ClientSession,
    ):
        """Ensure that the repo exists under the org."""
        github_api = await self._get_github_client(http_session)
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

    @provision_http_session
    async def get_org_repo_token(
            self, repo_name: str,
            *,
            http_session: ClientSession,
    ) -> str:
        """Return an access token once the repo exists."""
        await self.create_repo_if_not_exists(repo_name)
        return str((await self._get_github_client(http_session))._token)

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

    def sync_provision_deploy_key_to(self, repo_name: str) -> int:
        return asyncio.run(self.provision_deploy_key_to(repo_name))

    @provision_http_session
    async def provision_deploy_key_to(
            self, repo_name: str,
            *,
            http_session: ClientSession,
    ) -> int:
        """Add deploy key to the repo."""
        await self.create_repo_if_not_exists(repo_name)

        dpl_key = self.deployment_rsa_pub_key
        dpl_key_repr = dpl_key.split(' ')[1]
        dpl_key_repr = '...'.join((dpl_key_repr[:16], dpl_key_repr[-16:]))
        github_api = await self._get_github_client(http_session)
        api_resp = await github_api.post(
            '/repos/{owner}/{repo}/keys',
            url_vars={
                'owner': self.github_org_name,
                'repo': repo_name,
            },
            data={
                'title': (
                    '[SHOULD BE AUTO-REMOVED MINUTES AFTER CREATION!] '
                    f'Temporary key ({dpl_key_repr}) added '
                    'by Ansible Collection Migrator'
                ),
                'key': dpl_key,
                'read_only': False,
            },
        )
        return api_resp['id']

    def sync_drop_deploy_key_from(self, repo_name: str, key_id: int):
        return asyncio.run(self.drop_deploy_key_from(repo_name, key_id))

    @provision_http_session
    async def drop_deploy_key_from(
            self, repo_name: str, key_id: int,
            *,
            http_session: ClientSession,
    ) -> None:
        """Add deploy key to the repo."""
        github_api = await self._get_github_client(http_session)
        await github_api.delete(
            '/repos/{owner}/{repo}/keys/{key_id}',
            url_vars={
                'owner': self.github_org_name,
                'repo': repo_name,
                'key_id': key_id,
            },
        )

    def tmp_deployment_key_for(self, repo_name: str):
        """Make a CM that adds and removes deployment keys."""
        return _tmp_repo_deploy_key(self, repo_name)


@contextlib.contextmanager
def _tmp_repo_deploy_key(gh_api, repo_name):
    _key_id = gh_api.sync_provision_deploy_key_to(repo_name)
    try:
        yield
    finally:
        gh_api.sync_drop_deploy_key_from(repo_name, _key_id)
