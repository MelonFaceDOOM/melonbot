"""
CLI for Twitch OAuth:
- Prints the /authorize URL (so you can get a "code")
- Exchanges the code for {access_token, refresh_token}
- Refreshes using twitch_refresh_token

Reads from config.py:
  twitch_client_id: str
  twitch_client_secret: str
Optional (recommended):
  twitch_redirect_uri: str   (must match what you registered in Twitch dev console)
  twitch_refresh_token: str  (set after your first exchange)

Steps:
1) python twitch_oauth_cli.py auth-url
 - this will open browser url. click authorize. copy value in redirect url
2) python twitch_oauth_cli.py exchange --code "CODE FROM URL"
 - this will return the access token (temporary, don't need to save), and the refresh token
 - save refresh token somewhere
3) optional - update access token with: python twitch_oauth_cli.py refresh

Notes:
- This prints tokens to stdout. Don’t paste output into logs or commit it.
- Access tokens expire. Refresh tokens are the long-lived value you persist.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional


AUTHORIZE_URL = "https://id.twitch.tv/oauth2/authorize"
TOKEN_URL = "https://id.twitch.tv/oauth2/token"


def _load_config_value(name: str) -> Optional[str]:
    """
    Reads value from config.py if present; otherwise falls back to env var.
    Env var names:
      TWITCH_CLIENT_ID
      TWITCH_CLIENT_SECRET
      TWITCH_REDIRECT_URI
      TWITCH_REFRESH_TOKEN
    """
    env_map = {
        "twitch_client_id": "TWITCH_CLIENT_ID",
        "twitch_client_secret": "TWITCH_CLIENT_SECRET",
        "twitch_redirect_uri": "TWITCH_REDIRECT_URI",
        "twitch_refresh_token": "TWITCH_REFRESH_TOKEN",
    }

    # Try config.py first
    try:
        import config  # type: ignore

        if hasattr(config, name):
            v = getattr(config, name)
            return str(v) if v is not None else None
    except Exception:
        # If config import fails, still allow env var usage.
        pass

    # Fall back to env var
    env = env_map.get(name)
    if env:
        v = os.environ.get(env)
        return v if v else None
    return None


def _require(name: str) -> str:
    v = _load_config_value(name)
    if not v:
        raise RuntimeError(f"Missing required config value: {name}")
    return v


def _http_post_form(url: str, data: Dict[str, str]) -> Dict[str, Any]:
    body = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            # Twitch returns JSON on success (and usually on error).
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        try:
            j = json.loads(raw)
        except Exception:
            j = {"error": "http_error", "status": e.code, "body": raw[:500]}
        j["_http_status"] = e.code
        raise RuntimeError(f"HTTP {e.code}: {j}") from None
    except Exception as e:
        raise RuntimeError(f"Request failed: {e}") from None


def build_authorize_url(
    client_id: str,
    redirect_uri: str,
    scopes: str,
    state: str,
    force_verify: bool,
) -> str:
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": scopes.strip(),
        "state": state,
    }
    if force_verify:
        params["force_verify"] = "true"

    return AUTHORIZE_URL + "?" + urllib.parse.urlencode(params)


def exchange_code_for_tokens(code: str) -> Dict[str, Any]:
    client_id = _require("twitch_client_id")
    client_secret = _require("twitch_client_secret")
    redirect_uri = _load_config_value("twitch_redirect_uri") or "http://localhost:3000"

    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": redirect_uri,
    }
    return _http_post_form(TOKEN_URL, payload)


def refresh_tokens(refresh_token: str) -> Dict[str, Any]:
    client_id = _require("twitch_client_id")
    client_secret = _require("twitch_client_secret")

    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    return _http_post_form(TOKEN_URL, payload)


def _print_token_result(result: Dict[str, Any]) -> None:
    # Normalize common fields
    access_token = result.get("access_token")
    refresh_token = result.get("refresh_token")
    expires_in = result.get("expires_in")
    scope = result.get("scope")
    token_type = result.get("token_type")

    now = int(time.time())
    exp_at = now + int(expires_in) if isinstance(expires_in, int) else None

    print(json.dumps(result, indent=2, sort_keys=True))
    print()

    if access_token:
        print("Access token:")
        print(access_token)
        print()

    if refresh_token:
        print("Refresh token (save this in config.py as twitch_refresh_token):")
        print(refresh_token)
        print()

    if expires_in is not None:
        print(f"expires_in: {expires_in} seconds")
        if exp_at:
            print(f"approx expires_at (unix): {exp_at}")
        print()

    if scope is not None:
        print(f"scope: {scope}")
    if token_type is not None:
        print(f"token_type: {token_type}")


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    p_auth = sub.add_parser("auth-url", help="Print the Twitch /authorize URL")
    p_auth.add_argument("--scopes", default="", help='Space-separated scopes, e.g. "user:read:email"')
    p_auth.add_argument("--state", default="stream_tracker", help="Opaque state string (CSRF-ish)")
    p_auth.add_argument("--redirect-uri", default=None, help="Override redirect URI (must match registered)")
    p_auth.add_argument("--force-verify", action="store_true", help="Force the consent screen each time")

    p_ex = sub.add_parser("exchange", help="Exchange an authorization code for tokens")
    p_ex.add_argument("--code", required=True, help="The ?code=... returned to your redirect URI")

    sub.add_parser("refresh", help="Refresh using twitch_refresh_token from config.py")

    args = p.parse_args(argv)

    try:
        if args.cmd == "auth-url":
            client_id = _require("twitch_client_id")
            redirect_uri = (
                args.redirect_uri
                or _load_config_value("twitch_redirect_uri")
                or "http://localhost:3000"
            )
            url = build_authorize_url(
                client_id=client_id,
                redirect_uri=redirect_uri,
                scopes=args.scopes or "",
                state=args.state,
                force_verify=bool(args.force_verify),
            )
            print("Open this URL in a browser, approve, then copy the `code` from the redirect URL:\n")
            print(url)
            return 0

        if args.cmd == "exchange":
            result = exchange_code_for_tokens(args.code)
            _print_token_result(result)
            return 0

        if args.cmd == "refresh":
            rt = _load_config_value("twitch_refresh_token")
            if not rt:
                raise RuntimeError(
                    "Missing twitch_refresh_token in config.py. "
                    "Run `auth-url` + `exchange` first and save the refresh token."
                )
            result = refresh_tokens(rt)
            _print_token_result(result)
            return 0

        raise RuntimeError(f"Unknown command: {args.cmd}")

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
