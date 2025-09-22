import os
import re
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
import logging
import requests
from urllib.parse import quote
from pyngrok import conf, ngrok

from constants import DEFAULT_CLOUDFLARED_PATH

logger = logging.getLogger(__name__)


def with_basic_auth(url: str, user: str, pwd: str) -> str:
    scheme_sep = "://"
    if scheme_sep not in url:
        return url
    scheme, rest = url.split(scheme_sep, 1)
    return f"{scheme}{scheme_sep}{quote(user)}:{quote(pwd)}@{rest}"


def looks_like_pyngrok_shim(path_str: str) -> bool:
    if os.name != "nt":
        return False
    p = Path(path_str)
    try:
        scripts = p.parent
        return scripts.name.lower() == "scripts" and (
            (scripts / "python.exe").exists() or (scripts.parent / "python.exe").exists()
        )
    except Exception:
        return False


class TunnelManager:
    def __init__(self, config):
        self.tunnel_provider = (
            config.get("tunnel", "provider", fallback=os.getenv("TUNNEL_PROVIDER", "cloudflare"))
            .strip()
            .lower()
        )
        explicit_cf = config.get(
            "tunnel",
            "cloudflared_path",
            fallback=os.getenv("CLOUDFLARED_PATH", DEFAULT_CLOUDFLARED_PATH),
        ).strip()
        self.cloudflared_path: Optional[str] = explicit_cf if explicit_cf else None
        if self.cloudflared_path and not Path(self.cloudflared_path).exists():
            found = shutil.which("cloudflared")
            if found:
                self.cloudflared_path = found
            else:
                self.cloudflared_path = None
        self.active_tunnels: Dict[int, Dict[str, Any]] = {}

        if self.tunnel_provider == "ngrok":
            self._configure_ngrok(config)
            self._ngrok_preflight()
        elif self.tunnel_provider == "cloudflare" and not self._cloudflared_available():
            logger.warning("cloudflared not available; falling back to ngrok.")
            self.tunnel_provider = "ngrok"
            self._configure_ngrok(config)
            self._ngrok_preflight()
        elif self.tunnel_provider == "cloudflare":
            logger.info(f"✅ Using cloudflared at: {self.cloudflared_path}")

    # Readiness probe (used externally as well)
    @staticmethod
    def wait_for_jupyter(host_port: int, timeout: int = 240) -> bool:
        url = f"http://127.0.0.1:{host_port}/"
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                r = requests.get(url, timeout=2)
                if r.status_code < 500:
                    return True
            except requests.RequestException:
                pass
            time.sleep(1)
        return False

    # cloudflared
    def _cloudflared_available(self) -> bool:
        return bool(
            (self.cloudflared_path and Path(self.cloudflared_path).exists()) or shutil.which("cloudflared")
        )

    def start_cloudflared(self, host_port: int) -> Tuple[str, subprocess.Popen]:
        bin_path = (
            self.cloudflared_path
            if (self.cloudflared_path and Path(self.cloudflared_path).exists())
            else "cloudflared"
        )
        cmd = [
            bin_path,
            "tunnel",
            "--no-autoupdate",
            "--url",
            f"http://127.0.0.1:{host_port}",
            "--loglevel",
            "info",
        ]
        logger.info(f"Starting cloudflared: {cmd[0]} tunnel --url http://127.0.0.1:{host_port}")
        creationflags = (
            subprocess.CREATE_NO_WINDOW
            if os.name == "nt" and hasattr(subprocess, "CREATE_NO_WINDOW")
            else 0
        )
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            creationflags=creationflags,
        )
        deadline = time.time() + 30
        url = None
        pattern = re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com", re.I)
        if proc.stdout:
            while time.time() < deadline and proc.poll() is None:
                line = proc.stdout.readline()
                if not line:
                    time.sleep(0.05)
                    continue
                m = pattern.search(line)
                if m:
                    url = m.group(0)
                    break
        if not url:
            try:
                proc.terminate()
            except Exception:
                pass
            raise RuntimeError("cloudflared did not output a public URL in time")
        return url, proc

    # ngrok
    def _configure_ngrok(self, config):
        auth_token = config.get("ngrok", "auth_token", fallback=os.getenv("NGROK_AUTHTOKEN", ""))
        if not auth_token:
            logger.error("❌ 'auth_token' missing in [ngrok] (or NGROK_AUTHTOKEN).")
            raise SystemExit(1)
        conf.get_default().auth_token = auth_token
        if os.name == "nt":
            base = Path(os.getenv("LOCALAPPDATA") or Path.home())
            ngrok_dir = base / "UnifiedCompute" / "ngrok"
            bin_name = "ngrok.exe"
        else:
            ngrok_dir = Path.home() / ".unified_compute" / "ngrok"
            bin_name = "ngrok"
        ngrok_dir.mkdir(parents=True, exist_ok=True)
        per_user_bin = ngrok_dir / bin_name
        conf.get_default().config_path = str(ngrok_dir / "ngrok.yml")
        explicit_path = config.get("ngrok", "path", fallback=os.getenv("NGROK_PATH", ""))
        if explicit_path and Path(explicit_path).exists():
            conf.get_default().ngrok_path = str(Path(explicit_path))
            logger.info(f"✅ Using preinstalled ngrok at: {explicit_path}")
            return
        found = shutil.which("ngrok")
        if found and not looks_like_pyngrok_shim(found):
            conf.get_default().ngrok_path = found
            logger.info(f"✅ Found ngrok on PATH: {found}")
            return
        elif found:
            logger.info(f"ℹ️ Found pyngrok shim on PATH, ignoring: {found}")
        if os.name == "nt" and shutil.which("winget"):
            try:
                logger.info("📦 Installing ngrok via WinGet (silent)...")
                subprocess.run(
                    [
                        "winget",
                        "install",
                        "--id",
                        "Ngrok.Ngrok",
                        "--silent",
                        "--accept-package-agreements",
                        "--accept-source-agreements",
                    ],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                post = shutil.which("ngrok")
                if post and not looks_like_pyngrok_shim(post):
                    conf.get_default().ngrok_path = post
                    logger.info(f"✅ WinGet installed ngrok: {post}")
                    return
                else:
                    logger.info("ℹ️ WinGet installed ngrok but PATH still points to shim; continuing...")
            except Exception as e:
                logger.warning(f"WinGet install failed or unavailable: {e}")
        conf.get_default().ngrok_path = str(per_user_bin)
        os.environ["TMP"] = str(ngrok_dir)
        os.environ["TEMP"] = str(ngrok_dir)
        os.environ["TMPDIR"] = str(ngrok_dir)
        logger.info(f"ℹ️ Falling back to pyngrok auto-download into: {per_user_bin}")

    def _ngrok_preflight(self) -> None:
        ngrok_bin = Path(conf.get_default().ngrok_path or "")
        try:
            if ngrok_bin and ngrok_bin.exists() and not looks_like_pyngrok_shim(str(ngrok_bin)):
                proc = subprocess.run(
                    [str(ngrok_bin), "version"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    shell=False,
                )
                if proc.returncode == 0:
                    logger.info(f"✅ ngrok preflight OK: {proc.stdout.strip() or 'version OK'}")
                    return
                else:
                    logger.warning(
                        f"ngrok exists but 'version' failed (rc={proc.returncode}). stderr: {proc.stderr.strip()}"
                    )
            from pyngrok import installer
            installer.install_ngrok(conf.get_default().ngrok_path)
            logger.info("✅ ngrok installed via pyngrok.")
            proc = subprocess.run(
                [str(conf.get_default().ngrok_path), "version"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                shell=False,
            )
            if proc.returncode == 0:
                logger.info(f"✅ ngrok version: {proc.stdout.strip() or 'OK'}")
                return
            raise RuntimeError(f"ngrok installed but unusable: {proc.stderr.strip()}")
        except Exception as e:
            logger.error(f"❌ ngrok preflight failed: {e}", exc_info=True)
            logger.error(
                "If this is a locked-down network, set [ngrok] path to an existing ngrok binary."
            )
            raise SystemExit(1)
