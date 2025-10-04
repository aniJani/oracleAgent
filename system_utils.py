# system_utils.py

from pathlib import Path
import subprocess
import logging
import os
import shutil
import sys
from typing import List, Tuple


log = logging.getLogger(__name__)

# --- Helper to hide console window on Windows ---
def _get_creation_flags():
    return subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0

def is_docker_running() -> bool:
    """
    Checks if the Docker daemon is responsive using 'docker info'.
    Returns True if it is, False otherwise.
    """
    try:
        subprocess.check_output(
            ["docker", "info"], 
            stderr=subprocess.STDOUT,
            creationflags=_get_creation_flags()
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False

# --- NEW FUNCTION: Check for NVIDIA driver ---
def check_nvidia_smi() -> Tuple[bool, str]:
    """
    Checks if 'nvidia-smi' is accessible and working.
    Returns a tuple: (is_ok: bool, message: str).
    """
    if not shutil.which("nvidia-smi"):
        message = (
            "NVIDIA GPU drivers are not installed or 'nvidia-smi' is not in the system's PATH.\n\n"
            "This is a critical requirement. Please download and install the official drivers for your GPU."
        )
        return False, message
    
    try:
        subprocess.check_output(
            ["nvidia-smi"], 
            stderr=subprocess.STDOUT,
            creationflags=_get_creation_flags()
        )
        return True, "'nvidia-smi' is accessible and working."
    except (subprocess.CalledProcessError, FileNotFoundError):
        message = (
            "Found 'nvidia-smi', but the command failed to execute. "
            "This may indicate a problem with your NVIDIA driver installation."
        )
        return False, message

# --- NEW FUNCTION: Check for Cloudflared ---
def check_cloudflared() -> Tuple[bool, str]:
    """
    Checks if 'cloudflared' is accessible on the system PATH.
    Returns a tuple: (is_ok: bool, message: str).
    """
    if shutil.which("cloudflared"):
        return True, "'cloudflared' is installed and accessible."
    
    # Provide OS-specific instructions
    if os.name == 'nt': # Windows
        install_url = "https://developers.cloudflare.com/cloudflare-one/connections/connect-apps/install-and-setup/installation/#windows"
        message = (
            "'cloudflared' was not found. This is needed for creating secure tunnels.\n\n"
            f"Please download and install it from:\n{install_url}"
        )
    else: # macOS / Linux
        install_url = "https://developers.cloudflare.com/cloudflare-one/connections/connect-apps/install-and-setup/installation/#macos"
        message = (
            "'cloudflared' was not found. This is needed for creating secure tunnels.\n\n"
            f"The easiest way to install is with Homebrew:\n'brew install cloudflare/cloudflare/cloudflared'\n\n"
            f"For other methods, see:\n{install_url}"
        )
    return False, message

# --- NEW FUNCTION: Main pre-flight check runner ---
def run_preflight_checks() -> List[str]:
    """
    Runs all prerequisite checks and returns a list of error/warning messages.
    An empty list means all checks passed.
    """
    errors = []
    
    log.info("Running pre-flight checks...")

    # Check 1: NVIDIA Driver
    nvidia_ok, nvidia_msg = check_nvidia_smi()
    if not nvidia_ok:
        errors.append(f"❌ NVIDIA Driver Error:\n{nvidia_msg}")
    log.info(f"NVIDIA Check: {'OK' if nvidia_ok else 'FAIL'}")

    # Check 2: Docker
    if not is_docker_running():
        errors.append("❌ Docker Is Not Running:\nPlease start Docker Desktop and wait for it to initialize.")
    log.info(f"Docker Check: {'OK' if not any('Docker' in e for e in errors) else 'FAIL'}")

    # Check 3: Cloudflared (as a warning, since ngrok could be a fallback)
    cloudflared_ok, cloudflared_msg = check_cloudflared()
    if not cloudflared_ok:
        # We'll treat this as a warning for now, not a hard failure.
        errors.append(f"⚠️ Cloudflare Tunnel Warning:\n{cloudflared_msg}")
    log.info(f"Cloudflared Check: {'OK' if cloudflared_ok else 'WARN'}")
    
    log.info("Pre-flight checks complete.")
    return errors

def attempt_install_cloudflared() -> Tuple[bool, str]:
    """
    Attempts to install 'cloudflared' using a common package manager.
    Returns (success: bool, message: str).
    """
    log.info("Attempting to automatically install 'cloudflared'...")
    
    if sys.platform == "darwin": # macOS
        if not shutil.which("brew"):
            return False, "Homebrew not found. Please install it from https://brew.sh/ to automate installation."
        try:
            # The user might see a password prompt in the terminal if sudo is needed.
            subprocess.run(
                ["brew", "install", "cloudflare/cloudflare/cloudflared"],
                check=True, capture_output=True, text=True
            )
            if shutil.which("cloudflared"):
                return True, "'cloudflared' was successfully installed via Homebrew."
            else:
                return False, "Installation via Homebrew appeared to succeed, but 'cloudflared' is still not on the PATH."
        except subprocess.CalledProcessError as e:
            return False, f"Homebrew installation failed:\n{e.stderr}"
            
    elif sys.platform == "win32": # Windows
        if not shutil.which("winget"):
            return False, "Windows Package Manager (winget) not found. Please update Windows to automate installation."
        try:
            subprocess.run(
                [
                    "winget", "install", "--id", "Cloudflare.cloudflared",
                    "--silent", "--accept-package-agreements", "--accept-source-agreements"
                ],
                check=True, capture_output=True, text=True
            )
            # WinGet might not update the PATH in the current session, but we can check the default install location
            if shutil.which("cloudflared") or (Path(os.environ["ProgramFiles"]) / "cloudflared" / "cloudflared.exe").exists():
                 return True, "'cloudflared' was successfully installed via WinGet. A restart of the app may be required for it to be found."
            else:
                return False, "Installation via WinGet appeared to succeed, but 'cloudflared' could not be found."
        except subprocess.CalledProcessError as e:
            return False, f"WinGet installation failed:\n{e.stderr}"
            
    return False, "Automated installation is not supported on this operating system."