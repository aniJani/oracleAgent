import os
import sys
import json
import subprocess
import threading
import time
import webbrowser
import requests
from pathlib import Path

from PySide6 import QtCore, QtWidgets, QtGui

# Assume constants.py exists and defines STATE_FILE
# For demonstration, let's define it here if it's not available
try:
    from constants import STATE_FILE
except ImportError:
    STATE_FILE = Path(os.getenv("TEMP", "/tmp")) / "gpu_rental_state.json"


PROJECT_ROOT = Path(__file__).resolve().parent
CONTROL_API_BASE = "http://127.0.0.1:8765"

# --- Main Stylesheet ---
# Centralized QSS for a modern, techy look
MODERN_STYLESHEET = """
QWidget {
    background-color: #1a1b26; /* Dark background */
    color: #c0caf5; /* Light text */
    font-family: 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif;
    font-size: 14px;
}

QMainWindow {
    background-color: #1a1b26;
}

/* ---- Panels and GroupBoxes ---- */
QFrame#controlPanel, QFrame#logPanel {
    background-color: #24283b;
    border-radius: 10px;
}

QGroupBox {
    border: 1px solid #414868;
    border-radius: 8px;
    margin-top: 1em;
    font-weight: bold;
    color: #7aa2f7; /* Accent blue for titles */
}

QGroupBox::title {
    subcontrol-origin: margin;
    subcontrol-position: top center;
    padding: 0 10px;
}

/* ---- Labels ---- */
QLabel#statusIndicator {
    border-radius: 60px; /* Perfect circle */
    min-width: 120px;
    max-width: 120px;
    min-height: 120px;
    max-height: 120px;
}

QLabel#statusLabel {
    font-size: 24px;
    font-weight: bold;
    padding: 10px;
    color: #c0caf5;
}

QLabel#sessionsLabel {
    font-size: 16px;
    color: #a9b1d6;
    padding: 5px;
}

/* ---- Buttons ---- */
QPushButton {
    background-color: #414868;
    color: #c0caf5;
    border: none;
    padding: 12px;
    border-radius: 8px;
    font-size: 16px;
    font-weight: bold;
}

QPushButton:hover {
    background-color: #565f89;
}

QPushButton:pressed {
    background-color: #7aa2f7;
    color: #1a1b26;
}

QPushButton:disabled {
    background-color: #2f334d;
    color: #545c7e;
}

/* ---- Log Viewer ---- */
QPlainTextEdit {
    background-color: #16161e;
    border: 1px solid #414868;
    border-radius: 8px;
    color: #a9b1d6;
    padding: 5px;
}

QCheckBox {
    spacing: 5px;
    color: #a9b1d6;
}

QCheckBox::indicator {
    width: 15px;
    height: 15px;
}

QCheckBox::indicator:unchecked {
    background-color: #16161e;
    border: 1px solid #414868;
    border-radius: 3px;
}

QCheckBox::indicator:checked {
    background-color: #7aa2f7;
    border: 1px solid #7aa2f7;
    border-radius: 3px;
}

/* ---- Scrollbars ---- */
QScrollBar:vertical {
    border: none;
    background: #24283b;
    width: 10px;
    margin: 0px 0px 0px 0px;
}
QScrollBar::handle:vertical {
    background: #414868;
    min-height: 20px;
    border-radius: 5px;
}
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
    height: 0px;
}
"""


def read_state():
    p = Path(STATE_FILE)
    if not p.exists():
        return {"sessions": {}}
    try:
        return json.loads(p.read_text(encoding="utf-8")) or {"sessions": {}}
    except Exception:
        return {"sessions": {}}


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
            [sys.executable, "-u", "main.py"],  # -u for unbuffered output
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
        )
        self._reader_thread = threading.Thread(target=self._read_stdout, daemon=True)
        self._reader_thread.start()
        threading.Thread(target=self._wait_exit, daemon=True).start()

    def _read_stdout(self):
        if not self.proc or not self.proc.stdout:
            return
        for line in self.proc.stdout:
            if self._stop_reader.is_set():
                break
            self.output.emit(line.rstrip())
        # No need for the extra read(), iterator handles it all

    def _wait_exit(self):
        if not self.proc:
            return
        code = self.proc.wait()
        self._stop_reader.set()
        self.exited.emit(code)

    def stop(self):
        if not self.proc:
            return
        try:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        except Exception:
            pass  # Ignore errors if process is already dead
        finally:
            self._stop_reader.set()
            self.proc = None

    def is_running(self):
        return self.proc is not None and self.proc.poll() is None


class SimpleMainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("GPU Rental Host")
        self.resize(1200, 800)

        self.agent = AgentProcess()
        self.agent.output.connect(self.on_agent_output)
        self.agent.exited.connect(self.on_agent_exit)

        # Main UI
        central_widget = QtWidgets.QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QtWidgets.QHBoxLayout(central_widget)
        main_layout.setContentsMargins(20, 20, 20, 20)
        main_layout.setSpacing(20)

        # --- Left Panel: Controls ---
        control_panel = QtWidgets.QFrame()
        control_panel.setObjectName("controlPanel")
        control_panel.setMaximumWidth(400)
        control_layout = QtWidgets.QVBoxLayout(control_panel)
        control_layout.setContentsMargins(20, 20, 20, 20)
        control_layout.setSpacing(15)

        # Status Display
        self.status_indicator = QtWidgets.QLabel()
        self.status_indicator.setObjectName("statusIndicator")
        self.status_indicator.setAlignment(QtCore.Qt.AlignCenter)

        self.status_label = QtWidgets.QLabel("Offline")
        self.status_label.setObjectName("statusLabel")
        self.status_label.setAlignment(QtCore.Qt.AlignCenter)

        self.sessions_label = QtWidgets.QLabel("Active Sessions: 0")
        self.sessions_label.setObjectName("sessionsLabel")
        self.sessions_label.setAlignment(QtCore.Qt.AlignCenter)

        status_layout = QtWidgets.QVBoxLayout()
        status_layout.addWidget(self.status_indicator, 0, QtCore.Qt.AlignCenter)
        status_layout.addWidget(self.status_label, 0, QtCore.Qt.AlignCenter)
        status_layout.addWidget(self.sessions_label, 0, QtCore.Qt.AlignCenter)
        status_layout.setSpacing(10)

        control_layout.addLayout(status_layout)
        control_layout.addStretch(1)

        # Control buttons
        self.btn_start = QtWidgets.QPushButton("Start Agent")
        self.btn_start.setIcon(self.style().standardIcon(QtWidgets.QStyle.SP_MediaPlay))
        self.btn_start.setIconSize(QtCore.QSize(24, 24))
        self.btn_start.setToolTip("Go online and make your GPU available for rent")

        self.btn_stop = QtWidgets.QPushButton("Stop Agent")
        self.btn_stop.setIcon(self.style().standardIcon(QtWidgets.QStyle.SP_MediaStop))
        self.btn_stop.setIconSize(QtCore.QSize(24, 24))
        self.btn_stop.setToolTip("Go offline and stop all active sessions")

        self.btn_config = QtWidgets.QPushButton("Settings")
        self.btn_config.setIcon(
            self.style().standardIcon(QtWidgets.QStyle.SP_ComputerIcon)
        )  # Using a more generic icon
        self.btn_config.setIconSize(QtCore.QSize(24, 24))
        self.btn_config.setToolTip("Open the configuration file (config.ini)")

        btn_layout = QtWidgets.QVBoxLayout()
        btn_layout.setSpacing(10)
        btn_layout.addWidget(self.btn_start)
        btn_layout.addWidget(self.btn_stop)
        btn_layout.addWidget(self.btn_config)
        control_layout.addLayout(btn_layout)

        control_layout.addStretch(1)

        # How It Works
        info_card = QtWidgets.QGroupBox("How It Works")
        info_layout = QtWidgets.QVBoxLayout(info_card)
        info_text = QtWidgets.QLabel(
            "1. Click <b>Start Agent</b> to go online.<br>"
            "2. Your GPU becomes available for rent.<br>"
            "3. Earnings accrue during active sessions.<br>"
            "4. Click <b>Stop Agent</b> to go offline."
        )
        info_text.setWordWrap(True)
        info_layout.addWidget(info_text)
        control_layout.addWidget(info_card)

        # --- Right Panel: Logs ---
        log_panel = QtWidgets.QFrame()
        log_panel.setObjectName("logPanel")
        log_layout = QtWidgets.QVBoxLayout(log_panel)
        log_layout.setContentsMargins(20, 20, 20, 20)

        self.log = QtWidgets.QPlainTextEdit()
        self.log.setReadOnly(True)
        font = QtGui.QFontDatabase.systemFont(QtGui.QFontDatabase.FixedFont)
        font.setPointSize(10)
        self.log.setFont(font)

        log_layout.addWidget(QtWidgets.QLabel("<h3>Technical Logs</h3>"))
        log_layout.addWidget(self.log)

        # Add panels to main layout
        main_layout.addWidget(control_panel)
        main_layout.addWidget(log_panel, 1)  # Add stretch factor of 1

        # Events
        self.btn_start.clicked.connect(self.on_start)
        self.btn_stop.clicked.connect(self.on_stop)
        self.btn_config.clicked.connect(self.on_open_config)

        # Timer to refresh stats
        self.timer = QtCore.QTimer(self)
        self.timer.setInterval(2000)
        self.timer.timeout.connect(self.refresh_stats)

        # Initial State
        self.update_ui_for_stop()

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
        self.timer.start()

    def on_stop(self):
        self.agent.stop()
        self.update_ui_for_stop()
        self.log.appendPlainText("\nAgent has been stopped.")

    def on_agent_exit(self, code: int):
        self.log.appendPlainText(f"\n--- Agent process exited with code: {code} ---")
        self.on_stop()

    def update_ui_for_stop(self):
        """Helper to reset UI to the 'stopped' state."""
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

    def on_open_config(self):
        cfg = PROJECT_ROOT / "config.ini"
        if not cfg.exists():
            cfg.write_text(
                "[aptos]\n"
                "private_key = \n"
                "contract_address = \n"
                "node_url = https://fullnode.mainnet.aptoslabs.com/v1\n"
                "backend_ws_url = ws://127.0.0.1:8000/ws\n\n"
                "[host]\n"
                "price_per_second = 1\n\n"
                "[tunnel]\n"
                "provider = cloudflare\n\n"
                "[ngrok]\n"
                "auth_token = \n",
                encoding="utf-8",
            )
        # Use a cross-platform way to open the file
        if sys.platform == "win32":
            os.startfile(str(cfg))
        elif sys.platform == "darwin":  # macOS
            subprocess.run(["open", str(cfg)])
        else:  # linux
            subprocess.run(["xdg-open", str(cfg)])

    def on_agent_output(self, line: str):
        self.log.appendPlainText(line)
        self.log.verticalScrollBar().setValue(self.log.verticalScrollBar().maximum())

    def refresh_stats(self):
        try:
            state = read_state()
            sessions = state.get("sessions", {})
            count = len(sessions)
            self.sessions_label.setText(f"Active Sessions: {count}")
        except Exception as e:
            print(f"Error refreshing stats: {e}")  # Log to console for debugging
            pass

    def closeEvent(self, event: QtGui.QCloseEvent):
        """Ensure agent is stopped when closing the window."""
        self.on_stop()
        event.accept()


def main():
    app = QtWidgets.QApplication(sys.argv)
    app.setStyleSheet(MODERN_STYLESHEET)

    # Set an application icon (optional, but good for polish)
    icon_path = (
        Path(__file__).parent / "icon.png"
    )  # Create a 256x256 icon.png for best results
    if icon_path.exists():
        app.setWindowIcon(QtGui.QIcon(str(icon_path)))

    win = SimpleMainWindow()
    win.showMaximized()
    sys.exit(app.exec())


if __name__ == "__main__":
    # This is a placeholder for the agent's main script
    # To make this runnable, create a dummy main.py file
    if not (PROJECT_ROOT / "main.py").exists():
        (PROJECT_ROOT / "main.py").write_text(
            "import time\n"
            "print('Agent process started...')\n"
            "print('Connecting to services...')\n"
            "time.sleep(3)\n"
            "print('Connection successful. Awaiting rental requests.')\n"
            "try:\n"
            "    while True:\n"
            "        print(f'Heartbeat OK - {time.ctime()}')\n"
            "        time.sleep(10)\n"
            "except KeyboardInterrupt:\n"
            "    print('Agent shutting down.')\n"
        )
    main()
