import os
import sys
import json
import subprocess
import threading
import time
from pathlib import Path

import requests  # only used for stats if you keep a separate backend; safe otherwise
from PySide6 import QtCore, QtWidgets, QtGui

# If you already have constants.py with STATE_FILE, import it; else define a temp path
try:
    from constants import STATE_FILE
except ImportError:
    STATE_FILE = Path(os.getenv("TEMP", "/tmp")) / "gpu_rental_state.json"

PROJECT_ROOT = Path(__file__).resolve().parent

# If you have a separate backend for listings/sessions, you can point to it here.
# The GUI no longer needs a local uvicorn control API.
BACKEND_BASE = os.getenv(
    "UC_BACKEND_BASE", "http://127.0.0.1:8000"
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
        self.proc = subprocess.Popen(
            [sys.executable, "-u", "main.py"],  # run agent loop
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
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


class SimpleMainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("GPU Rental Host")
        self.resize(1200, 800)

        self.agent = AgentProcess()
        self.agent.output.connect(self.on_agent_output)
        self.agent.exited.connect(self.on_agent_exit)

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

        btn_row = QtWidgets.QHBoxLayout()
        btn_row.addWidget(self.btn_register)
        btn_row.addWidget(self.btn_start)
        btn_row.addWidget(self.btn_stop)
        control_layout.addLayout(btn_row)

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

        # Timer (optional) to poll your separate backend for sessions
        self.timer = QtCore.QTimer(self)
        self.timer.setInterval(2000)
        self.timer.timeout.connect(self.refresh_stats)

        # Initial UI state
        self.update_ui_for_stop()

    # ---------- Actions ----------
    def on_register(self):
        self.log.appendPlainText("Registering device on-chain...")
        try:
            # Run agent registration path and stream output
            proc = subprocess.Popen(
                [sys.executable, "-u", "main.py", "--register"],
                cwd=str(PROJECT_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True,
            )
            for line in proc.stdout or []:
                self.log.appendPlainText(line.rstrip("\n"))
            rc = proc.wait()
            if rc == 0:
                self.log.appendPlainText("✅ Registration complete.")
                self.btn_register.setEnabled(False)  # one-time
            else:
                self.log.appendPlainText("❌ Registration failed. See logs above.")
        except Exception as e:
            self.log.appendPlainText(f"Registration error: {e}")

    def on_start(self):
        self.log.clear()
        self.log.appendPlainText("Attempting to start agent...")
        self.agent.start()
        self.btn_start.setEnabled(False)
        self.btn_stop.setEnabled(True)
        self.status_label.setText("Online")
        self.status_indicator.setStyleSheet(
            "background-color: qradialgradient(cx: 0.5, cy: 0.5, radius: 0.5, fx: 0.5, fy: 0.5, "
            "stop: 0 #50fa7b, stop: 0.5 #28a745, stop: 1 #1a1b26);"
        )
        self.setWindowTitle("GPU Rental Host - Online")
        # self.timer.start()

    def on_stop(self):
        self.agent.stop()
        self.update_ui_for_stop()
        self.log.appendPlainText("\nAgent has been stopped.")

    # ---------- Helpers ----------
    def update_ui_for_stop(self):
        self.btn_start.setEnabled(True)
        self.btn_stop.setEnabled(False)
        self.status_label.setText("Offline")
        self.status_indicator.setStyleSheet(
            "background-color: qradialgradient(cx: 0.5, cy: 0.5, radius: 0.5, fx: 0.5, fy: 0.5, "
            "stop: 0 #ff5555, stop: 0.5 #dc3545, stop: 1 #1a1b26);"
        )
        self.setWindowTitle("GPU Rental Host - Offline")
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

    def on_agent_output(self, line: str):
        self.log.appendPlainText(line)

    def on_agent_exit(self, rc: int):
        self.log.appendPlainText(f"\nAgent process exited with code {rc}.")
        self.update_ui_for_stop()


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
