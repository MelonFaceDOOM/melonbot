"""SSH tunnel connector for reaching the Postgres box from a remote machine.

Postgres listens on the server's loopback only. Two ways to reach it:

  * Bot process runs ON the DB server — no tunnel; connect to 127.0.0.1:5432.
  * Bot runs anywhere else (laptop, even on LAN) — SSH to the server and forward
    a local port to that server's 127.0.0.1:5432. SSH_HOST may be a LAN IP or
    public hostname; REMOTE_DB_HOST stays 127.0.0.1 (address of Postgres on the
    server side of the tunnel).

Usage (see config.py):

    import db_tunnel
    db_tunnel.open_tunnel("127.0.0.1", 5432)
    host, port = "127.0.0.1", db_tunnel.local_bind_port()

The tunnel is a process-wide singleton and is torn down automatically at exit.
"""

from __future__ import annotations

import atexit
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

# SSHTunnelForwarder instance (process-wide singleton).
_TUNNEL = None


def _pkey_path() -> Optional[str]:
    raw = os.environ.get("SSH_PKEY")
    return os.path.expanduser(raw) if raw else None


def open_tunnel(remote_host: str, remote_port: int):
    """Open (once) an SSH tunnel that forwards a local port to
    ``remote_host:remote_port`` as seen from the SSH server.

    ``remote_host`` is resolved on the SSH server, so for Postgres running on the
    server's loopback this is typically ``127.0.0.1``. Returns the live
    ``SSHTunnelForwarder``; repeated calls reuse the existing tunnel.
    """
    global _TUNNEL
    if _TUNNEL is not None and _TUNNEL.is_active:
        return _TUNNEL

    # Imported lazily so the server (which never tunnels) doesn't need sshtunnel.
    from sshtunnel import SSHTunnelForwarder

    ssh_host = os.environ["SSH_HOST"]
    ssh_port = int(os.environ.get("SSH_PORT", "22"))
    ssh_user = os.environ["SSH_USER"]
    pkey = _pkey_path()
    password = os.environ.get("SSH_PASSWORD")

    kwargs = dict(
        ssh_username=ssh_user,
        remote_bind_address=(remote_host, remote_port),
        # local port 0 -> let the OS pick a free port, avoids collisions.
        local_bind_address=("127.0.0.1", 0),
        set_keepalive=30.0,
    )

    if pkey:
        if not os.path.isfile(pkey):
            raise FileNotFoundError(
                f"SSH_PKEY file not found: {pkey!r}. "
                "Check the path in .env (Windows: use the full path to the private key)."
            )
        kwargs["ssh_pkey"] = pkey
        key_pw = os.environ.get("SSH_PKEY_PASSWORD")
        if key_pw:
            kwargs["ssh_private_key_password"] = key_pw
    elif password:
        kwargs["ssh_password"] = password
    else:
        raise RuntimeError(
            "No SSH auth configured. Set SSH_PKEY (path to a private key) or "
            "SSH_PASSWORD in the environment / .env file."
        )

    logger.info(
        "Opening SSH tunnel %s@%s:%s -> %s:%s",
        ssh_user, ssh_host, ssh_port, remote_host, remote_port,
    )
    _TUNNEL = SSHTunnelForwarder((ssh_host, ssh_port), **kwargs)
    _TUNNEL.start()
    logger.info("SSH tunnel established on 127.0.0.1:%s", _TUNNEL.local_bind_port)
    return _TUNNEL


def local_bind_port() -> int:
    """Local port the tunnel is listening on. Requires an open tunnel."""
    assert _TUNNEL is not None, "Tunnel not open; call open_tunnel() first."
    return _TUNNEL.local_bind_port


def close_tunnel() -> None:
    global _TUNNEL
    if _TUNNEL is not None:
        logger.info("Closing SSH tunnel.")
        try:
            _TUNNEL.stop()
        finally:
            _TUNNEL = None


@atexit.register
def _cleanup() -> None:
    close_tunnel()
