# Unified Compute Host Agent

This is part of a 4 repo project:


SmartContracts: https://github.com/Nishan30/AxessProtocol


Frontend: https://github.com/Nishan30/AxessProtocolFrontend


Backend: https://github.com/aniJani/AxessProtocolBackend


Oracle Agent: https://github.com/aniJani/oracleAgent

This repo runs a GPU host agent that exposes a Jupyter Notebook through a secure tunnel and claims payments on-chain.

## Prerequisites (Windows)
- Python 3.10+
- Docker Desktop (running). Cloudflared Installed with the default path, coders can edit the path. For GPU, install NVIDIA driver + NVIDIA Container Toolkit
- Recommended: `git`, `PowerShell` v5+

## Setup

1. Create a virtual environment and activate it:

```powershell
python -m venv .venv
.venv\Scripts\activate
```

2. Install dependencies:

```powershell
pip install -r requirements.txt
```

3. Create `config.ini` in the project root:

```ini
[aptos]
private_key = <YOUR_PRIVATE_KEY>
contract_address = <MARKETPLACE_CONTRACT_ADDRESS>
node_url = https://fullnode.mainnet.aptoslabs.com/v1
backend_ws_url = ws://127.0.0.1:8000/ws

[host]
price_per_second = 1

[tunnel]
provider = cloudflare  ; or ngrok

[ngrok]
auth_token = <YOUR_NGROK_AUTHTOKEN>
```

> If using Cloudflare, install cloudflared or configure its path in your config. If using ngrok, ensure `auth_token` is set.

## Run the agent

```powershell
python main.py
```

The agent will:
- Detect hardware and register on-chain (if needed)
- Prepare/pull the PyTorch image
- Wait for rentals and start Jupyter in a container
- Create a tunnel (Cloudflare preferred, ngrok fallback)

## Optional: Desktop GUI

A simple PySide6 GUI is included to start/stop the agent and view sessions.

```powershell
pip install PySide6
python gui_app.py
```

The GUI reads the same state file used by the agent to list active sessions and open/stop them.

## Packaging (optional)

Create a single EXE for Windows:

```powershell
pip install pyinstaller
pyinstaller --noconfirm --onefile --name UnifiedComputeHost gui_app.py
```

## Troubleshooting
- Ensure Docker Desktop is running.
- GPU required? Verify `nvidia-smi` works and NVIDIA Container Toolkit is installed.
- ngrok errors? Set `auth_token` in `config.ini`.
- If Cloudflare used, ensure `cloudflared` is installed and accessible.
