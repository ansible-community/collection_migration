"""In-memory RSA key generation and management utils."""
from __future__ import annotations

import contextlib
import functools
import os
import subprocess

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric.rsa import generate_private_key
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PublicFormat,
    PrivateFormat,
)


class RSAKey:
    """In-memory RSA key wrapper."""

    def __init__(self):
        _rsa_key_obj = generate_private_key(
            public_exponent=65537,
            key_size=4096,
            backend=default_backend(),
        )

        _rsa_pub_key_obj = _rsa_key_obj.public_key()
        self._public_repr = _rsa_pub_key_obj.public_bytes(
            encoding=Encoding.PEM,
            format=PublicFormat.PKCS1,
        ).decode()
        self._public_repr_openssh = _rsa_pub_key_obj.public_bytes(
            encoding=Encoding.OpenSSH,
            format=PublicFormat.OpenSSH,
        ).decode()

        _private_rsa_key_repr = _rsa_key_obj.private_bytes(
            encoding=Encoding.PEM,
            format=PrivateFormat.TraditionalOpenSSL,  # A.K.A. PKCS#1
            encryption_algorithm=NoEncryption(),
        )
        self._ssh_agent_cm = SSHAgent(_private_rsa_key_repr)

    @property
    def ssh_agent(self) -> SSHAgent:
        """SSH agent CM."""
        return self._ssh_agent_cm

    @property
    def public(self) -> str:
        """String PKCS#1-formatted representation of the public key."""
        return self._public_repr

    @property
    def public_openssh(self) -> str:
        """String OpenSSH-formatted representation of the public key."""
        return self._public_repr_openssh


class SSHAgent:
    """SSH agent lifetime manager.

    Only usable as a CM. Only holds one RSA key in memory.
    """

    def __init__(self, ssh_key: bytes):
        self._ssh_key = ssh_key
        self._ssh_agent_proc = None
        self._ssh_agent_socket = None

    def __enter__(self) -> _SubprocessSSHAgentProxy:
        ssh_agent_cmd = (
            'ssh-agent',  # man 1 ssh-agent
            '-s',  # generate Bourne shell commands on stdout
            '-D',  # foreground mode
        )
        ssh_add_cmd = 'ssh-add', '-'

        self._ssh_agent_proc = subprocess.Popen(
            ssh_agent_cmd,
            stdout=subprocess.PIPE,  # we need to parse the socket path
            text=True,  # auto-decode the text from bytes
        )
        self._ssh_agent_socket = (
            self._ssh_agent_proc.
            stdout.readline().
            partition('; ')[0].
            partition('=')[-1]
        )

        subprocess_proxy = _SubprocessSSHAgentProxy(self._ssh_agent_socket)
        subprocess_proxy.check_output(
            ssh_add_cmd,
            input=self._ssh_key,
            stderr=subprocess.DEVNULL,
        )

        return subprocess_proxy

    def __exit__(self, exc_type, exc_val, exc_tb):
        ssh_agent_proc = self._ssh_agent_proc

        self._ssh_agent_socket = None
        self._ssh_agent_proc = None

        with contextlib.suppress(IOError, OSError):
            ssh_agent_proc.terminate()

        return False


def pre_populate_env_kwarg(meth):
    """Pre-populated env arg in decorated methods."""
    @functools.wraps(meth)
    def method_wrapper(self, *args, **kwargs):
        if 'env' not in kwargs:
            kwargs['env'] = os.environ.copy()
        kwargs['env']['GIT_SSH_COMMAND'] = (
            'ssh -2 '
            '-F /dev/null '
            '-o PreferredAuthentications=publickey '
            '-o IdentityFile=/dev/null '
            f'-o IdentityAgent="{self._sock}"'
        )
        kwargs['env']['SSH_AUTH_SOCK'] = self._sock
        return meth(self, *args, **kwargs)
    return method_wrapper


# pylint: disable=too-few-public-methods
class _SubprocessSSHAgentProxy:
    """Proxy object for calls to subprocess functions."""

    def __init__(self, sock):
        self._sock = sock

    @pre_populate_env_kwarg
    def check_call(self, *args, **kwargs):
        """Populate the SSH agent sock into the check_call env."""
        return subprocess.check_call(*args, **kwargs)

    @pre_populate_env_kwarg
    def check_output(self, *args, **kwargs):
        """Populate the SSH agent sock into the check_output env."""
        return subprocess.check_output(*args, **kwargs)

    @pre_populate_env_kwarg
    def run(self, *args, **kwargs):
        """Populate the SSH agent sock into the run env."""
        return subprocess.run(*args, **kwargs)
