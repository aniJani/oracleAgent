import asyncio
import contextlib
import io
import logging
import os
import sys
import json
import subprocess
import threading
import time
from pathlib import Path
import configparser
import webbrowser

import requests  # only used for stats if you keep a separate backend; safe otherwise
from PySide6 import QtCore, QtWidgets, QtGui
from agent import HostAgent
from main import _register_once, load_config, run_agent_main
from typing import Dict
from config import save_user_settings, load_user_settings
from system_utils import attempt_install_cloudflared, is_docker_running
from system_utils import run_preflight_checks

# If you already have constants.py with STATE_FILE, import it; else define a temp path
try:
    from constants import STATE_FILE
except ImportError:
    STATE_FILE = Path(os.getenv("TEMP", "/tmp")) / "gpu_rental_state.json"

if getattr(sys, 'frozen', False):
    APP_ROOT = Path(sys.executable).parent.resolve()
else:
    APP_ROOT = Path(__file__).resolve().parent

# If you have a separate backend for listings/sessions, you can point to it here.
# The GUI no longer needs a local uvicorn control API.
BACKEND_BASE = os.getenv(
    "UC_BACKEND_BASE", "https://axessprotocolbackend-production.up.railway.app"
).strip()  # e.g.,  (optional)

MODERN_STYLESHEET = """
QWidget { background-color: #1a1b26; color: #c0caf5; font-family: 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif; font-size: 14px; }
QMainWindow { background-color: #1a1b26; }
QFrame#controlPanel, QFrame#logPanel { background-color: #24283b; border-radius: 10px; }
QGroupBox { border: 1px solid #414868; border-radius: 8px; margin-top: 10px; padding-top: 12px; color: #c0caf5; font-weight: 600; }
QGroupBox::title { subcontrol-origin: margin; subcontrol-position: top left; padding: 0 6px; margin-left: 10px; }
QLabel#statusIndicator { min-width: 18px; min-height: 18px; max-width: 18px; max-height: 18px; border-radius: 9px; background-color: #ff5555; margin-right: 8px; }
QLabel#statusLabel { font-size: 18px; font-weight: 700; letter-spacing: 0.5px; }
QLabel#sessionsLabel { font-size: 16px; color: #a9b1d6; padding: 5px; }
QPushButton { background-color: #414868; color: #c0caf5; border: none; padding: 12px; border-radius: 8px; font-weight: 600; }
QPushButton:hover { background-color: #51587a; }
QPushButton:disabled { background-color: #2c2f40; color: #777e99; }
QPlainTextEdit { background-color: #1f2335; border: 1px solid #414868; border-radius: 8px; color: #c0caf5; padding: 8px; font-family: Consolas, 'Courier New', monospace; font-size: 13px; }
QListWidget { background-color: #1f2335; border: 1px solid #414868; border-radius: 8px; color: #c0caf5; padding: 8px; font-size: 13px; }
QCheckBox { color: #c0caf5; }
QCheckBox::indicator { width: 16px; height: 16px; }
QCheckBox::indicator:unchecked { background-color: #1f2335; border: 1px solid #414868; border-radius: 3px; }
QCheckBox::indicator:checked { background-color: #7aa2f7; border: 1px solid #7aa2f7; border-radius: 3px; }
QScrollBar:vertical { border: none; background: #24283b; width: 10px; margin: 0; }
QScrollBar::handle:vertical { background: #414868; min-height: 20px; border-radius: 5px; }
QScrollBar::handle:vertical:hover { background: #51587a; }
QLabel.hint { color: #9aa5ce; font-size: 12px; }
"""


def read_state():
    p = Path(STATE_FILE)
    if not p.exists():
        return {"sessions": {}, "host": {}}
    try:
        return json.loads(p.read_text(encoding="utf-8")) or {"sessions": {}, "host": {}}
    except Exception:
        return {"sessions": {}, "host": {}}


class AgentProcess(QtCore.QObject):
    output = QtCore.Signal(str)
    exited = QtCore.Signal(int)

    def __init__(self):
        super().__init__()
        self.proc = None
        self._reader_thread = None
        self._stop_reader = threading.Event()

    def start(self):
        if self.proc and self.proc.poll() is None:
            return
        
        self._stop_reader.clear()

        # --- THIS IS THE KEY FIX ---
        # When running as an .exe, we call the .exe itself with a special flag.
        # When running as a script, we call 'python main.py'.
        if getattr(sys, 'frozen', False):
            # We are in the bundled .exe. Call ourself with the --run-agent flag.
            command = [sys.executable, "--run-agent"]
        else:
            # We are in development. Call the main.py script.
            command = [sys.executable, "-u", "main.py"]
        # ---------------------------
        
        self.proc = subprocess.Popen(
            command,
            cwd=str(APP_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
            # Hide the console window on Windows
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
        )
        self._reader_thread = threading.Thread(target=self._read_stdout, daemon=True)
        self._reader_thread.start()
        threading.Thread(target=self._waiter, daemon=True).start()

    def stop(self):
        if not self.proc:
            return
        try:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        finally:
            self.proc = None
            self._stop_reader.set()

    def _read_stdout(self):
        if not self.proc or not self.proc.stdout:
            return
        for line in self.proc.stdout:
            if self._stop_reader.is_set():
                break
            self.output.emit(line.rstrip("\n"))

    def _waiter(self):
        if not self.proc:
            return
        rc = self.proc.wait()
        self.exited.emit(rc)

class QtStream(io.StringIO):
    """
    A custom stream object that redirects stdout/stderr to a Qt signal.
    """
    def __init__(self, signal_emitter):
        super().__init__()
        self.signal_emitter = signal_emitter

    def write(self, text):
        # Emit the signal with the new text
        self.signal_emitter.emit(text)

# --- NEW CLASS: A thread to run the agent's asyncio loop ---
class QtLogHandler(logging.Handler):
    def __init__(self, signal_emitter):
        super().__init__()
        self.signal_emitter = signal_emitter

    def emit(self, record):
        msg = self.format(record)
        self.signal_emitter.emit(msg + '\n')

# --- REPLACE the AgentThread class ---
class AgentThread(QtCore.QObject):
    """
    Runs the agent's main asyncio loop in a separate thread.
    Captures all logging output and communicates back to the GUI using Qt signals.
    """
    log_signal = QtCore.Signal(str)
    finished_signal = QtCore.Signal(int)
    
    def __init__(self):
        super().__init__()
        self._thread = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self.run, daemon=True)
        self._thread.start()

    def run(self):
        """
        The main entry point for the background thread.
        Sets up the logging system to use a Qt signal handler.
        """
        # --- THIS IS THE NEW, ROBUST LOGGING SETUP ---
        # Get the root logger
        root_logger = logging.getLogger()
        # Clear any existing handlers
        root_logger.handlers.clear()
        # Set the level to capture everything
        root_logger.setLevel(logging.INFO)
        # Create our custom handler that emits a Qt signal
        qt_handler = QtLogHandler(signal_emitter=self.log_signal)
        # Add a formatter
        formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
        qt_handler.setFormatter(formatter)
        # Add our handler to the root logger
        root_logger.addHandler(qt_handler)
        # -----------------------------------------------
        
        return_code = 0
        try:
            # Now, any logging call from any module (agent, main, etc.)
            # will be processed by our QtLogHandler.
            run_agent_main()
        except Exception:
            import traceback
            # Use the logger to report the final crash
            logging.critical(f"CRITICAL AGENT THREAD ERROR:\n{traceback.format_exc()}")
            return_code = 1
        finally:
            self.finished_signal.emit(return_code)


class SimpleMainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Axess Protocol Agent")
        self.resize(1200, 800)

        self.agent = AgentThread()
        self.agent.log_signal.connect(self.on_agent_output)
        self.agent.finished_signal.connect(self.on_agent_exit)

        # Layout
        central_widget = QtWidgets.QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QtWidgets.QHBoxLayout(central_widget)
        main_layout.setContentsMargins(20, 20, 20, 20)
        main_layout.setSpacing(20)

        # Left panel
        control_panel = QtWidgets.QFrame()
        control_panel.setObjectName("controlPanel")
        control_panel.setMaximumWidth(420)
        control_layout = QtWidgets.QVBoxLayout(control_panel)
        control_layout.setContentsMargins(20, 20, 20, 20)
        control_layout.setSpacing(15)

        self.status_indicator = QtWidgets.QLabel()
        self.status_indicator.setObjectName("statusIndicator")
        self.status_indicator.setAlignment(QtCore.Qt.AlignCenter)

        self.status_label = QtWidgets.QLabel("Offline")
        self.status_label.setObjectName("statusLabel")

        status_row = QtWidgets.QHBoxLayout()
        status_row.addWidget(self.status_indicator)
        status_row.addWidget(self.status_label)
        status_row.addStretch(1)
        control_layout.addLayout(status_row)

        how_group = QtWidgets.QGroupBox("How It Works")
        how_layout = QtWidgets.QVBoxLayout(how_group)
        how_label = QtWidgets.QLabel(
            "1) Click <b>Register Device</b> once to publish this machine on-chain.<br>"
            "2) Click <b>Start Agent</b> whenever you want to go online.<br>"
            "3) Your GPU becomes available for rent; earnings accrue while active.<br>"
            "4) Click <b>Stop Agent</b> to go offline."
        )
        how_label.setWordWrap(True)
        how_layout.addWidget(how_label)
        control_layout.addWidget(how_group)

        self.btn_register = QtWidgets.QPushButton("Register Device")
        self.btn_register.setToolTip("Register this machine on-chain (one-time)")

        self.btn_start = QtWidgets.QPushButton("Start Agent")
        self.btn_start.setIcon(self.style().standardIcon(QtWidgets.QStyle.SP_MediaPlay))
        self.btn_start.setIconSize(QtCore.QSize(24, 24))

        self.btn_stop = QtWidgets.QPushButton("Stop Agent")
        self.btn_stop.setIcon(self.style().standardIcon(QtWidgets.QStyle.SP_MediaStop))
        self.btn_stop.setIconSize(QtCore.QSize(24, 24))

        self.btn_settings = QtWidgets.QPushButton("Settings")
        self.btn_settings.setToolTip("Configure private key and pricing")

        btn_row = QtWidgets.QHBoxLayout()
        btn_row.addWidget(self.btn_register)
        btn_row.addWidget(self.btn_start)
        btn_row.addWidget(self.btn_stop)
        control_layout.addLayout(btn_row)

        # Settings button on a separate row
        settings_row = QtWidgets.QHBoxLayout()
        settings_row.addWidget(self.btn_settings)
        settings_row.addStretch(1)
        control_layout.addLayout(settings_row)

        # Optional: show sessions if your separate backend exposes it
        sess_group = QtWidgets.QGroupBox("Status")
        sess_layout = QtWidgets.QVBoxLayout(sess_group)
        self.sessions_label = QtWidgets.QLabel("Active Sessions: 0")
        self.sessions_label.setObjectName("sessionsLabel")
        sess_layout.addWidget(self.sessions_label)
        control_layout.addWidget(sess_group)
        control_layout.addStretch(1)

        # Right panel: logs
        log_panel = QtWidgets.QFrame()
        log_panel.setObjectName("logPanel")
        log_layout = QtWidgets.QVBoxLayout(log_panel)
        log_layout.setContentsMargins(20, 20, 20, 20)
        log_layout.setSpacing(15)

        log_group = QtWidgets.QGroupBox("Agent Output")
        log_group_layout = QtWidgets.QVBoxLayout(log_group)
        self.log = QtWidgets.QPlainTextEdit()
        self.log.setReadOnly(True)
        log_group_layout.addWidget(self.log)
        log_layout.addWidget(log_group)

        main_layout.addWidget(control_panel)
        main_layout.addWidget(log_panel)

        # Wire actions
        self.btn_register.clicked.connect(self.on_register)
        self.btn_start.clicked.connect(self.on_start)
        self.btn_stop.clicked.connect(self.on_stop)
        self.btn_settings.clicked.connect(self.on_settings)

        # Timer (optional) to poll your separate backend for sessions
        self.timer = QtCore.QTimer(self)
        self.timer.setInterval(2000)
        self.timer.timeout.connect(self.refresh_stats)

        # Initial UI state
        self.update_ui_for_stop()
        QtCore.QTimer.singleShot(500, self.perform_startup_checks)
    
    def perform_startup_checks(self):
        """
        Runs pre-flight checks and displays a custom message box with
        install options if there are any issues.
        """
        errors = run_preflight_checks()
        
        if not errors:
            self.log.appendPlainText("✅ All prerequisite checks passed.")
            return
        
        install_cf_button = None
        open_nvidia_button = None # <-- ADD
        
        if any("Cloudflare" in e for e in errors):
            install_cf_button = msg_box.addButton("Install Cloudflared", QtWidgets.QMessageBox.ActionRole)
        
        if any("NVIDIA" in e for e in errors): # <-- ADD THIS BLOCK
            open_nvidia_button = msg_box.addButton("Go to NVIDIA Drivers", QtWidgets.QMessageBox.ActionRole)

        # Execute the dialog
        msg_box.exec()

        # Handle custom button clicks
        clicked_button = msg_box.clickedButton()
        if clicked_button == install_cf_button:
            self.run_cloudflared_installation()
        elif clicked_button == open_nvidia_button: # <-- ADD THIS BLOCK
            self.log.appendPlainText("Opening NVIDIA driver download page in browser...")
            webbrowser.open("https://www.nvidia.com/Download/index.aspx")

        # Disable buttons until resolved
        self.btn_start.setEnabled(False)
        self.btn_register.setEnabled(False)
        self.btn_start.setToolTip("Prerequisites missing. See message.")
        self.btn_register.setToolTip("Prerequisites missing. See message.")

        # --- NEW CUSTOM MESSAGE BOX LOGIC ---
        msg_box = QtWidgets.QMessageBox(self)
        msg_box.setIcon(QtWidgets.QMessageBox.Critical)
        msg_box.setWindowTitle("Prerequisites Check Failed")
        msg_box.setText("Some required software is missing or not configured correctly.")
        
        error_summary = "\n\n".join(errors)
        msg_box.setInformativeText(error_summary)

        # Add standard buttons
        msg_box.setStandardButtons(QtWidgets.QMessageBox.Close)
        
        # Add custom buttons for actionable errors
        install_cf_button = None
        if any("Cloudflare" in e for e in errors):
            install_cf_button = msg_box.addButton("Install Cloudflared", QtWidgets.QMessageBox.ActionRole)

        # Execute the dialog
        msg_box.exec()

        # Handle custom button clicks
        clicked_button = msg_box.clickedButton()
        if clicked_button == install_cf_button:
            self.run_cloudflared_installation()
        # ------------------------------------

        self.log.appendPlainText("--- PREREQUISITE CHECKS FAILED ---")
        for error in errors: self.log.appendPlainText(error)
        self.log.appendPlainText("------------------------------------")



    # --- ADD THIS NEW METHOD to handle the installation ---
    def run_cloudflared_installation(self):
        self.log.appendPlainText("User initiated 'cloudflared' installation...")
        # Show a "please wait" message as this can take time
        progress_dialog = QtWidgets.QProgressDialog("Installing cloudflared...", "Cancel", 0, 0, self)
        progress_dialog.setWindowModality(QtCore.Qt.WindowModal)
        progress_dialog.setWindowTitle("Installation in Progress")
        progress_dialog.show()
        
        # We must run the installation in a thread to avoid freezing the GUI
        def install_task():
            success, message = attempt_install_cloudflared()
            # Safely update the GUI from the thread
            QtCore.QMetaObject.invokeMethod(self, "_on_installation_complete", QtCore.Qt.QueuedConnection, 
                                            QtCore.Q_ARG(bool, success), QtCore.Q_ARG(str, message))

        # Start the installation thread
        thread = threading.Thread(target=install_task, daemon=True)
        thread.start()

    # --- ADD THIS NEW SLOT to handle completion ---
    @QtCore.Slot(bool, str)
    def _on_installation_complete(self, success: bool, message: str):
        # The progress dialog is no longer needed, find and close it.
        for widget in QtWidgets.QApplication.allWidgets():
            if isinstance(widget, QtWidgets.QProgressDialog):
                widget.close()

        if success:
            QtWidgets.QMessageBox.information(self, "Installation Successful", message)
            self.log.appendPlainText(f"✅ {message}")
            # Prompt the user to re-check
            self.log.appendPlainText("Please restart the application to re-run the prerequisite checks.")
        else:
            QtWidgets.QMessageBox.critical(self, "Installation Failed", message)
            self.log.appendPlainText(f"❌ {message}")

    # ---------- Actions ----------
    async def _register_once() -> Dict:
        """
        Performs the on-chain registration. Returns a result dictionary.
        This version does NOT exit, making it safe to call from other modules.
        """
        try:
            cfg = load_config() # This can call sys.exit(), which raises SystemExit
            agent = HostAgent(cfg)
            result = await agent.register_device_if_needed()
        except SystemExit as e:
            # Catch the exit from load_config and turn it into a proper error result
            result = {"status": "error", "message": "Failed to load config.ini. Please check the file and ensure all required keys are present."}
        except Exception as e:
            result = {"status": "error", "message": f"An unexpected error occurred: {e}"}

        # Print a single-line outcome for CLI backward compatibility
        if result["status"] == "ok":
            msg = result["message"]
            tx = result.get("tx_hash")
            if tx: print(f"REGISTER_OK tx={tx}")
            else: print(f"REGISTER_OK {msg}")
        else:
            print(f"REGISTER_ERR {result['message']}")
            
        return result

    def _run_registration_task(self):
        """
        This function runs in a background thread. It loads the config
        and then executes the async _register_once function.
        """
        result = None
        try:
            load_config() 
            result = asyncio.run(_register_once())
            
            # Extract simple strings to send over the signal
            status = result.get("status", "error")
            message = result.get("message", "Unknown result")
            
            QtCore.QMetaObject.invokeMethod(self, "_on_registration_complete", QtCore.Qt.QueuedConnection, 
                                            QtCore.Q_ARG(str, status), QtCore.Q_ARG(str, message))
        except Exception as e:
            # On failure, also send simple strings
            status = "error"
            message = f"An exception occurred in the registration thread: {e}"
            QtCore.QMetaObject.invokeMethod(self, "_on_registration_complete", QtCore.Qt.QueuedConnection, 
                                            QtCore.Q_ARG(str, status), QtCore.Q_ARG(str, message))

    # --- REPLACE this function ---
    @QtCore.Slot(str, str) # <-- MODIFIED: Now accepts two strings
    def _on_registration_complete(self, status: str, message: str):
        """
        This function is a Qt Slot. It is safely called on the main GUI thread
        to update the UI with the result of the registration task.
        """
        if status == "ok":
            self.log.appendPlainText(f"✅ Registration successful: {message}")
            # Keep the button disabled if it succeeded.
        else:
            self.log.appendPlainText(f"❌ Registration failed: {message}")
            self.btn_register.setEnabled(True) # Re-enable the button on failure.
            
    def on_register(self):
        """
        Handles the 'Register' button click by starting the registration
        process in a background thread to keep the GUI responsive.
        """
        self.log.appendPlainText("Attempting to register device on-chain...")
        self.btn_register.setEnabled(False)  # Disable button during operation

        # Run the async registration function in a separate thread.
        thread = threading.Thread(target=self._run_registration_task, daemon=True)
        thread.start()

    def on_start(self):
        self.log.appendPlainText("Checking Docker status...")
        
        # --- THIS IS THE NEW LOGIC ---
        if not is_docker_running():
            self.log.appendPlainText("❌ Docker is not running. Agent cannot start.")
            
            # Show a user-friendly error message box
            msg_box = QtWidgets.QMessageBox(self)
            msg_box.setIcon(QtWidgets.QMessageBox.Warning)
            msg_box.setWindowTitle("Docker Not Found")
            msg_box.setText("The Docker daemon is not running.")
            msg_box.setInformativeText(
                "Please start Docker Desktop and wait for it to be fully initialized before starting the agent."
            )
            msg_box.setStandardButtons(QtWidgets.QMessageBox.Ok)
            msg_box.exec()
            
            # Abort the start process
            return
        # ---------------------------

        self.log.appendPlainText("✅ Docker is running. Starting agent in a background thread...")
        
        # This part is the same as before
        self.agent.start()
        
        self.btn_start.setEnabled(False)
        self.btn_stop.setEnabled(True)
        self.status_label.setText("Online")
        self.status_indicator.setStyleSheet("background-color: qradialgradient(cx: 0.5, cy: 0.5, radius: 0.5, fx: 0.5, fy: 0.5, stop: 0 #50fa7b, stop: 0.5 #28a745, stop: 1 #1a1b26);")
        self.setWindowTitle("Axess Protocol Agent - Online")

    def on_stop(self):
        self.log.appendPlainText("\nStopping agent by closing the application...")
        # A confirmation dialog is good UX
        reply = QtWidgets.QMessageBox.question(self, 'Confirm Stop', 
                                               "This will stop the agent and close the application. Are you sure?",
                                               QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No, 
                                               QtWidgets.QMessageBox.No)
        if reply == QtWidgets.QMessageBox.Yes:
            self.close()
        else:
            self.log.appendPlainText("Stop cancelled.")

    def on_settings(self):
        """Open the settings dialog"""
        try:
            dialog = SettingsDialog(self)
            result = dialog.exec()

            if result == QtWidgets.QDialog.Accepted:
                self.log.appendPlainText("✅ Settings updated successfully")
                if (
                    hasattr(self.agent, "proc")
                    and self.agent.proc
                    and self.agent.proc.poll() is None
                ):
                    self.log.appendPlainText(
                        "⚠️  Please restart the agent for changes to take effect"
                    )

        except Exception as e:
            self.log.appendPlainText(f"❌ Failed to open settings: {e}")

    # ---------- Helpers ----------
    def update_ui_for_stop(self):
        self.btn_start.setEnabled(True)
        self.btn_stop.setEnabled(False)
        self.status_label.setText("Offline")
        self.status_indicator.setStyleSheet(
            "background-color: qradialgradient(cx: 0.5, cy: 0.5, radius: 0.5, fx: 0.5, fy: 0.5, "
            "stop: 0 #ff5555, stop: 0.5 #dc3545, stop: 1 #1a1b26);"
        )
        self.setWindowTitle("Axess Protocol Agent - Offline")
        self.sessions_label.setText("Active Sessions: 0")
        if self.timer.isActive():
            self.timer.stop()

    def refresh_stats(self):
        if not BACKEND_BASE:
            return
        try:
            r = requests.get(
                f"{BACKEND_BASE}/api/v1/jobs", timeout=3
            )  # adjust to your API if desired
            if r.ok:
                data = r.json()
                count = len(data) if isinstance(data, (list, dict)) else 0
                self.sessions_label.setText(f"Active Sessions: {count}")
        except Exception:
            pass

    @QtCore.Slot(str)
    def on_agent_output(self, text: str):
        self.log.moveCursor(QtGui.QTextCursor.End)
        self.log.insertPlainText(text)

    @QtCore.Slot(int)
    def on_agent_exit(self, return_code: int):
        self.log.appendPlainText(f"\nAgent process finished with code {return_code}.")
        self.update_ui_for_stop()


class SettingsDialog(QtWidgets.QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Settings")
        self.setModal(True)
        self.setFixedSize(500, 300)

        # --- MODIFIED: Use the new loader ---
        self.config = load_user_settings()
        # ------------------------------------
        
        self.setup_ui()
        self.load_values()

    def setup_ui(self):
        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)

        # Title
        title = QtWidgets.QLabel("Configuration Settings")
        title.setObjectName("statusLabel")  # Use existing styling
        title.setAlignment(QtCore.Qt.AlignCenter)
        layout.addWidget(title)

        # Form layout
        form_layout = QtWidgets.QFormLayout()
        form_layout.setSpacing(15)

        # Private Key field
        self.private_key_edit = QtWidgets.QLineEdit()
        self.private_key_edit.setEchoMode(QtWidgets.QLineEdit.Password)
        self.private_key_edit.setPlaceholderText("Enter your Aptos private key (0x...)")

        # Show/Hide password button
        key_layout = QtWidgets.QHBoxLayout()
        key_layout.addWidget(self.private_key_edit)

        self.show_key_btn = QtWidgets.QPushButton("👁")
        self.show_key_btn.setMaximumWidth(40)
        self.show_key_btn.setCheckable(True)
        self.show_key_btn.clicked.connect(self.toggle_password_visibility)
        key_layout.addWidget(self.show_key_btn)

        key_widget = QtWidgets.QWidget()
        key_widget.setLayout(key_layout)
        form_layout.addRow("Private Key:", key_widget)

        # Price per second field
        self.price_edit = QtWidgets.QSpinBox()
        self.price_edit.setRange(1, 999999)
        self.price_edit.setSuffix(" units/second")
        self.price_edit.setToolTip("Price in smallest currency units per second")
        form_layout.addRow("Price per Second:", self.price_edit)

        layout.addLayout(form_layout)

        # Help text
        help_text = QtWidgets.QLabel(
            "• Private Key: Your Aptos wallet private key (starts with 0x)\n"
            "• Price per Second: How much you charge per second of GPU usage"
        )
        help_text.setStyleSheet("color: #9aa5ce; font-size: 12px;")
        help_text.setWordWrap(True)
        layout.addWidget(help_text)

        layout.addStretch()

        # Buttons
        button_layout = QtWidgets.QHBoxLayout()

        self.save_btn = QtWidgets.QPushButton("Save")
        self.save_btn.clicked.connect(self.save_config)

        self.cancel_btn = QtWidgets.QPushButton("Cancel")
        self.cancel_btn.clicked.connect(self.reject)

        button_layout.addStretch()
        button_layout.addWidget(self.cancel_btn)
        button_layout.addWidget(self.save_btn)

        layout.addLayout(button_layout)

    def load_current_config(self):
        """Load the current configuration file"""
        try:
            if self.config_path.exists():
                self.config.read(self.config_path)
            else:
                # Create default sections if file doesn't exist
                self.config.add_section("aptos")
                self.config.add_section("host")
        except Exception as e:
            QtWidgets.QMessageBox.warning(
                self, "Config Error", f"Error loading config: {e}"
            )

    def toggle_password_visibility(self):
        """Toggle password visibility for private key field"""
        if self.show_key_btn.isChecked():
            self.private_key_edit.setEchoMode(QtWidgets.QLineEdit.Normal)
            self.show_key_btn.setText("🙈")
        else:
            self.private_key_edit.setEchoMode(QtWidgets.QLineEdit.Password)
            self.show_key_btn.setText("👁")

    def validate_private_key(self, key):
        """Basic validation for private key format"""
        if not key:
            return False, "Private key cannot be empty"

        if not key.startswith("0x"):
            return False, "Private key must start with '0x'"

        if len(key) != 66:  # 0x + 64 hex characters
            return False, "Private key must be 66 characters long (0x + 64 hex chars)"

        try:
            int(key, 16)  # Check if it's valid hex
        except ValueError:
            return False, "Private key must contain only valid hexadecimal characters"

        return True, ""

    def load_values(self):
        """Load current values into the form."""
        private_key = self.config.get("aptos", "private_key", fallback="")
        self.private_key_edit.setText(private_key)
        
        price = self.config.getint("host", "price_per_second", fallback=1)
        self.price_edit.setValue(price)
        
    # --- THIS METHOD IS NOW MUCH SIMPLER ---
    def save_config(self):
        """
        Validates user input and saves it using the centralized config helper.
        """
        try:
            private_key = self.private_key_edit.text().strip()
            price_per_second = self.price_edit.value()

            is_valid, error_msg = self.validate_private_key(private_key)
            if not is_valid:
                QtWidgets.QMessageBox.warning(self, "Validation Error", error_msg)
                return
            
            # --- MODIFIED: Use the new save function ---
            save_user_settings(private_key, price_per_second)
            # -------------------------------------------

            QtWidgets.QMessageBox.information(
                self,
                "Settings Saved",
                "Your settings have been saved successfully!"
            )
            self.accept()
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Save Error", f"Failed to save configuration:\n{str(e)}")


def main():
    app = QtWidgets.QApplication(sys.argv)
    app.setStyleSheet(MODERN_STYLESHEET)

    icon_path = Path(__file__).parent / "icon.png"
    if icon_path.exists():
        app.setWindowIcon(QtGui.QIcon(str(icon_path)))

    win = SimpleMainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
