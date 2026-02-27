"""
================================================================================
NIAGARA BAS DOWNLOADER - Desktop GUI Application v2.0
================================================================================
Modern CustomTkinter GUI for downloading trend data from Niagara BAS systems.
Wraps the existing CLI tools into a polished Windows desktop application.

Build as .exe with: python build_exe.py
================================================================================
"""

import os
import sys
import time
import socket
import threading
import queue
import re
from datetime import datetime, timedelta
from io import StringIO

# ============================================================================
# PATH SETUP - Works for both dev and frozen .exe
# ============================================================================
if getattr(sys, 'frozen', False):
    # Running as compiled .exe
    APP_DIR = os.path.dirname(sys.executable)
else:
    APP_DIR = os.path.dirname(os.path.abspath(__file__))

# Add app directory to path for module imports
sys.path.insert(0, APP_DIR)

# ============================================================================
# GUI IMPORTS
# ============================================================================
try:
    import customtkinter as ctk
    CTK_AVAILABLE = True
except ImportError:
    CTK_AVAILABLE = False

import tkinter as tk
from tkinter import filedialog, messagebox

# ============================================================================
# BACKEND IMPORTS
# ============================================================================
from config_district_details import district_config
from niagara_download_engine import DownloadEngine, ProgressPrinter, filter_existing_files
from niagara_url_generator import URLGenerator, get_available_districts, get_point_list_path
from niagara_auth import NiagaraAuth
from credentials import get_district_credentials
from utils import APP_VERSION
from logging_config import get_logger, setup_logging

logger = get_logger("gui")

# ============================================================================
# THEME CONSTANTS
# ============================================================================
COLORS = {
    'bg_dark':      '#0f1117',
    'bg_card':      '#1a1d27',
    'bg_card_hover':'#22263a',
    'bg_input':     '#141720',
    'border':       '#2a2e3d',
    'border_focus': '#00d4aa',
    'accent':       '#00d4aa',
    'accent_dim':   '#00a884',
    'accent_glow':  '#00d4aa22',
    'text':         '#e8eaed',
    'text_dim':     '#8b8fa3',
    'text_muted':   '#5a5e72',
    'success':      '#34d399',
    'warning':      '#fbbf24',
    'error':        '#f87171',
    'info':         '#60a5fa',
}

FONT_FAMILY = 'Segoe UI'
FONT_MONO = 'Cascadia Code'


# ============================================================================
# UTILITY: Output Capture
# ============================================================================
class OutputCapture:
    """Captures stdout/stderr and routes to a queue for GUI display."""

    def __init__(self, msg_queue, tag='stdout'):
        self.queue = msg_queue
        self.tag = tag
        self._original = None

    def write(self, text):
        if text and text.strip():
            # Strip ANSI escape codes
            clean = re.sub(r'\x1b\[[0-9;]*m', '', text)
            clean = re.sub(r'\r', '', clean)
            if clean.strip():
                self.queue.put((self.tag, clean))

    def flush(self):
        pass


# ============================================================================
# MAIN APPLICATION
# ============================================================================
class NiagaraApp(ctk.CTk if CTK_AVAILABLE else tk.Tk):
    """Main application window."""

    def __init__(self):
        super().__init__()

        self.title("Niagara BAS Downloader")
        self.geometry("1100x780")
        self.minsize(900, 650)

        if CTK_AVAILABLE:
            ctk.set_appearance_mode("dark")
            ctk.set_default_color_theme("dark-blue")

        self.configure(fg_color=COLORS['bg_dark'] if CTK_AVAILABLE else None)

        # State
        self.msg_queue = queue.Queue()
        self.is_running = False
        self._districts_cache = []
        self._selected_districts = []

        # Build UI
        self._build_header()
        self._build_tabs()
        self._build_status_bar()

        # Start queue polling
        self._poll_queue()

    # ----------------------------------------------------------------
    # HEADER
    # ----------------------------------------------------------------
    def _build_header(self):
        header = ctk.CTkFrame(self, fg_color=COLORS['bg_card'], corner_radius=0, height=60)
        header.pack(fill='x', padx=0, pady=0)
        header.pack_propagate(False)

        title_frame = ctk.CTkFrame(header, fg_color='transparent')
        title_frame.pack(side='left', padx=20, pady=10)

        ctk.CTkLabel(
            title_frame, text="⬡  NIAGARA BAS",
            font=(FONT_FAMILY, 18, 'bold'), text_color=COLORS['accent']
        ).pack(side='left')

        ctk.CTkLabel(
            title_frame, text="  DOWNLOADER",
            font=(FONT_FAMILY, 18), text_color=COLORS['text_dim']
        ).pack(side='left')

        # Status indicator
        self.status_dot = ctk.CTkLabel(
            header, text="●", font=(FONT_FAMILY, 14),
            text_color=COLORS['text_muted']
        )
        self.status_dot.pack(side='right', padx=20)

        self.status_label = ctk.CTkLabel(
            header, text="Ready",
            font=(FONT_FAMILY, 11), text_color=COLORS['text_dim']
        )
        self.status_label.pack(side='right', padx=(0, 5))

    # ----------------------------------------------------------------
    # TABS
    # ----------------------------------------------------------------
    def _build_tabs(self):
        if CTK_AVAILABLE:
            self.tabview = ctk.CTkTabview(
                self, fg_color=COLORS['bg_dark'],
                segmented_button_fg_color=COLORS['bg_card'],
                segmented_button_selected_color=COLORS['accent_dim'],
                segmented_button_selected_hover_color=COLORS['accent'],
                segmented_button_unselected_color=COLORS['bg_card'],
                segmented_button_unselected_hover_color=COLORS['bg_card_hover'],
                text_color=COLORS['text'],
                text_color_disabled=COLORS['text_muted'],
                corner_radius=8
            )
        else:
            self.tabview = ctk.CTkTabview(self)

        self.tabview.pack(fill='both', expand=True, padx=15, pady=(10, 5))

        self.tab_download = self.tabview.add("  Download  ")
        self.tab_config = self.tabview.add("  Configuration  ")
        self.tab_districts = self.tabview.add("  Districts  ")
        self.tab_log = self.tabview.add("  Activity Log  ")

        self._build_download_tab()
        self._build_config_tab()
        self._build_districts_tab()
        self._build_log_tab()

    # ----------------------------------------------------------------
    # DOWNLOAD TAB
    # ----------------------------------------------------------------
    def _build_download_tab(self):
        parent = self.tab_download

        # Top row: District selection + params
        top = ctk.CTkFrame(parent, fg_color='transparent')
        top.pack(fill='x', padx=10, pady=10)

        # --- District Selection Card ---
        dist_card = self._card(top, "District Selection")
        dist_card.pack(side='left', fill='both', expand=True, padx=(0, 5))

        sel_frame = ctk.CTkFrame(dist_card, fg_color='transparent')
        sel_frame.pack(fill='x', padx=15, pady=(5, 10))

        self.district_var = ctk.StringVar(value="Select a district...")
        districts = self._get_district_list()

        self.district_menu = ctk.CTkOptionMenu(
            sel_frame, variable=self.district_var,
            values=districts if districts else ["No districts found"],
            fg_color=COLORS['bg_input'], button_color=COLORS['accent_dim'],
            button_hover_color=COLORS['accent'],
            dropdown_fg_color=COLORS['bg_card'],
            dropdown_hover_color=COLORS['bg_card_hover'],
            text_color=COLORS['text'], font=(FONT_FAMILY, 12),
            width=320, height=36
        )
        self.district_menu.pack(side='left', padx=(0, 10))

        ctk.CTkButton(
            sel_frame, text="Refresh", width=80, height=36,
            fg_color=COLORS['bg_input'], hover_color=COLORS['bg_card_hover'],
            border_color=COLORS['border'], border_width=1,
            text_color=COLORS['text_dim'], font=(FONT_FAMILY, 11),
            command=self._refresh_districts
        ).pack(side='left')

        # District info label
        self.dist_info = ctk.CTkLabel(
            dist_card, text="Choose a district to view details",
            font=(FONT_FAMILY, 11), text_color=COLORS['text_muted'],
            anchor='w'
        )
        self.dist_info.pack(fill='x', padx=15, pady=(0, 10))

        # Bind selection change
        self.district_var.trace_add('write', self._on_district_change)

        # --- Parameters Card ---
        param_card = self._card(top, "Parameters")
        param_card.pack(side='left', fill='both', expand=True, padx=(5, 0))

        params = ctk.CTkFrame(param_card, fg_color='transparent')
        params.pack(fill='x', padx=15, pady=(5, 10))

        # Days
        ctk.CTkLabel(params, text="Days:", font=(FONT_FAMILY, 12),
                      text_color=COLORS['text_dim']).grid(row=0, column=0, sticky='w', pady=3)
        self.days_var = ctk.StringVar(value="90")
        ctk.CTkEntry(
            params, textvariable=self.days_var, width=80, height=32,
            fg_color=COLORS['bg_input'], border_color=COLORS['border'],
            text_color=COLORS['text'], font=(FONT_FAMILY, 12)
        ).grid(row=0, column=1, padx=(10, 20), pady=3)

        # Workers
        ctk.CTkLabel(params, text="Workers:", font=(FONT_FAMILY, 12),
                      text_color=COLORS['text_dim']).grid(row=0, column=2, sticky='w', pady=3)
        self.workers_var = ctk.StringVar(value="10")
        ctk.CTkEntry(
            params, textvariable=self.workers_var, width=80, height=32,
            fg_color=COLORS['bg_input'], border_color=COLORS['border'],
            text_color=COLORS['text'], font=(FONT_FAMILY, 12)
        ).grid(row=0, column=3, padx=(10, 0), pady=3)

        # Output directory
        out_frame = ctk.CTkFrame(param_card, fg_color='transparent')
        out_frame.pack(fill='x', padx=15, pady=(0, 10))

        ctk.CTkLabel(out_frame, text="Output:", font=(FONT_FAMILY, 12),
                      text_color=COLORS['text_dim']).pack(side='left')

        self.output_var = ctk.StringVar(value=os.environ.get('OUTPUT_DIR', os.path.join(APP_DIR, 'output')))
        self.output_entry = ctk.CTkEntry(
            out_frame, textvariable=self.output_var, height=32,
            fg_color=COLORS['bg_input'], border_color=COLORS['border'],
            text_color=COLORS['text'], font=(FONT_FAMILY, 11)
        )
        self.output_entry.pack(side='left', fill='x', expand=True, padx=10)

        ctk.CTkButton(
            out_frame, text="Browse", width=70, height=32,
            fg_color=COLORS['bg_input'], hover_color=COLORS['bg_card_hover'],
            border_color=COLORS['border'], border_width=1,
            text_color=COLORS['text_dim'], font=(FONT_FAMILY, 11),
            command=self._browse_output
        ).pack(side='right')

        # --- Action buttons ---
        action_frame = ctk.CTkFrame(parent, fg_color='transparent')
        action_frame.pack(fill='x', padx=10, pady=(5, 10))

        self.start_btn = ctk.CTkButton(
            action_frame, text="▶  Start Download", height=44, width=200,
            fg_color=COLORS['accent_dim'], hover_color=COLORS['accent'],
            text_color='#000000', font=(FONT_FAMILY, 14, 'bold'),
            corner_radius=8, command=self._start_download
        )
        self.start_btn.pack(side='left', padx=(0, 10))

        self.stop_btn = ctk.CTkButton(
            action_frame, text="■  Stop", height=44, width=100,
            fg_color=COLORS['error'], hover_color='#ef4444',
            text_color='#ffffff', font=(FONT_FAMILY, 13, 'bold'),
            corner_radius=8, command=self._stop_download, state='disabled'
        )
        self.stop_btn.pack(side='left')

        # --- Progress Section ---
        prog_card = self._card(parent, "Progress")
        prog_card.pack(fill='both', expand=True, padx=10, pady=(5, 10))

        self.progress_bar = ctk.CTkProgressBar(
            prog_card, fg_color=COLORS['bg_input'],
            progress_color=COLORS['accent'], height=8, corner_radius=4
        )
        self.progress_bar.pack(fill='x', padx=15, pady=(10, 5))
        self.progress_bar.set(0)

        self.progress_label = ctk.CTkLabel(
            prog_card, text="Waiting to start...",
            font=(FONT_FAMILY, 11), text_color=COLORS['text_dim'], anchor='w'
        )
        self.progress_label.pack(fill='x', padx=15, pady=(0, 5))

        # Live output
        self.live_output = ctk.CTkTextbox(
            prog_card, fg_color=COLORS['bg_input'],
            text_color=COLORS['text'], font=(FONT_MONO, 11),
            border_color=COLORS['border'], border_width=1,
            corner_radius=6, height=200
        )
        self.live_output.pack(fill='both', expand=True, padx=15, pady=(0, 15))

    # ----------------------------------------------------------------
    # CONFIGURATION TAB
    # ----------------------------------------------------------------
    def _build_config_tab(self):
        parent = self.tab_config

        # .env file card
        env_card = self._card(parent, "Environment Configuration (.env)")
        env_card.pack(fill='both', expand=True, padx=10, pady=10)

        env_info = ctk.CTkFrame(env_card, fg_color='transparent')
        env_info.pack(fill='x', padx=15, pady=(5, 10))

        env_path = os.path.join(APP_DIR, '.env')
        env_exists = os.path.exists(env_path)

        status_text = f"✓  .env found: {env_path}" if env_exists else f"✗  .env not found at: {env_path}"
        status_color = COLORS['success'] if env_exists else COLORS['warning']

        ctk.CTkLabel(
            env_info, text=status_text,
            font=(FONT_FAMILY, 12), text_color=status_color, anchor='w'
        ).pack(fill='x')

        # Show env contents
        self.env_text = ctk.CTkTextbox(
            env_card, fg_color=COLORS['bg_input'],
            text_color=COLORS['text'], font=(FONT_MONO, 11),
            border_color=COLORS['border'], border_width=1,
            corner_radius=6
        )
        self.env_text.pack(fill='both', expand=True, padx=15, pady=(0, 10))

        if env_exists:
            try:
                with open(env_path, 'r') as f:
                    content = f.read()
                # Mask passwords
                masked = re.sub(r'(_PASS=)(.+)', r'\1****', content)
                self.env_text.insert('1.0', masked)
            except Exception as e:
                self.env_text.insert('1.0', f"Error reading .env: {e}")
        else:
            self.env_text.insert('1.0', "No .env file found.\n\nCopy .env.template to .env in the application directory and fill in your credentials.")

        btn_frame = ctk.CTkFrame(env_card, fg_color='transparent')
        btn_frame.pack(fill='x', padx=15, pady=(0, 15))

        ctk.CTkButton(
            btn_frame, text="Open .env Location", height=36, width=160,
            fg_color=COLORS['bg_input'], hover_color=COLORS['bg_card_hover'],
            border_color=COLORS['border'], border_width=1,
            text_color=COLORS['text'], font=(FONT_FAMILY, 12),
            command=lambda: self._open_folder(APP_DIR)
        ).pack(side='left', padx=(0, 10))

        ctk.CTkButton(
            btn_frame, text="Reload Config", height=36, width=140,
            fg_color=COLORS['accent_dim'], hover_color=COLORS['accent'],
            text_color='#000000', font=(FONT_FAMILY, 12, 'bold'),
            command=self._reload_config
        ).pack(side='left')

    # ----------------------------------------------------------------
    # DISTRICTS TAB
    # ----------------------------------------------------------------
    def _build_districts_tab(self):
        parent = self.tab_districts

        dist_card = self._card(parent, "All Configured Districts")
        dist_card.pack(fill='both', expand=True, padx=10, pady=10)

        # Headers
        hdr = ctk.CTkFrame(dist_card, fg_color=COLORS['bg_input'], corner_radius=4, height=35)
        hdr.pack(fill='x', padx=15, pady=(10, 0))
        hdr.pack_propagate(False)

        for col, w in [("District", 220), ("Base IP", 220), ("Creds", 60), ("Points", 60), ("VPN Type", 140)]:
            ctk.CTkLabel(
                hdr, text=col, font=(FONT_FAMILY, 11, 'bold'),
                text_color=COLORS['text_dim'], width=w, anchor='w'
            ).pack(side='left', padx=5)

        # Scrollable list
        self.district_list = ctk.CTkScrollableFrame(
            dist_card, fg_color='transparent',
            scrollbar_button_color=COLORS['border'],
            scrollbar_button_hover_color=COLORS['accent_dim']
        )
        self.district_list.pack(fill='both', expand=True, padx=15, pady=(2, 15))

        self._populate_district_list()

    def _populate_district_list(self):
        # Clear existing
        for w in self.district_list.winfo_children():
            w.destroy()

        districts = sorted(district_config.keys())

        for i, dist_name in enumerate(districts):
            config = district_config.get(dist_name, {})
            base_ip = config.get('BASE_IP', 'N/A')
            bg = COLORS['bg_card'] if i % 2 == 0 else COLORS['bg_dark']

            row = ctk.CTkFrame(self.district_list, fg_color=bg, corner_radius=2, height=30)
            row.pack(fill='x', pady=1)
            row.pack_propagate(False)

            # Name
            ctk.CTkLabel(row, text=dist_name, font=(FONT_FAMILY, 11),
                          text_color=COLORS['text'], width=220, anchor='w').pack(side='left', padx=5)

            # Base IP
            ip_display = base_ip[:35] if base_ip else 'N/A'
            ctk.CTkLabel(row, text=ip_display, font=(FONT_MONO, 10),
                          text_color=COLORS['text_dim'], width=220, anchor='w').pack(side='left', padx=5)

            # Credentials check
            u, p = get_district_credentials(dist_name)
            has_creds = bool(u and p)
            cred_text = "✓" if has_creds else "—"
            cred_color = COLORS['success'] if has_creds else COLORS['text_muted']
            ctk.CTkLabel(row, text=cred_text, font=(FONT_FAMILY, 13),
                          text_color=cred_color, width=60, anchor='center').pack(side='left', padx=5)

            # Point list check
            path, _ = get_point_list_path(dist_name)
            has_pts = path is not None
            pt_text = "✓" if has_pts else "—"
            pt_color = COLORS['success'] if has_pts else COLORS['text_muted']
            ctk.CTkLabel(row, text=pt_text, font=(FONT_FAMILY, 13),
                          text_color=pt_color, width=60, anchor='center').pack(side='left', padx=5)

            # VPN type — V2.0: VPN_DATA is just the type string
            vpn_type = config.get('VPN_DATA', 'na')
            if vpn_type.lower() in ('na', 'n/a', ''):
                vpn_type = '—'
            ctk.CTkLabel(row, text=vpn_type, font=(FONT_FAMILY, 11),
                          text_color=COLORS['text_dim'], width=140, anchor='w').pack(side='left', padx=5)

    # ----------------------------------------------------------------
    # LOG TAB
    # ----------------------------------------------------------------
    def _build_log_tab(self):
        parent = self.tab_log

        log_card = self._card(parent, "Activity Log")
        log_card.pack(fill='both', expand=True, padx=10, pady=10)

        self.log_text = ctk.CTkTextbox(
            log_card, fg_color=COLORS['bg_input'],
            text_color=COLORS['text'], font=(FONT_MONO, 11),
            border_color=COLORS['border'], border_width=1,
            corner_radius=6
        )
        self.log_text.pack(fill='both', expand=True, padx=15, pady=(10, 10))

        btn_frame = ctk.CTkFrame(log_card, fg_color='transparent')
        btn_frame.pack(fill='x', padx=15, pady=(0, 15))

        ctk.CTkButton(
            btn_frame, text="Clear Log", height=32, width=100,
            fg_color=COLORS['bg_input'], hover_color=COLORS['bg_card_hover'],
            border_color=COLORS['border'], border_width=1,
            text_color=COLORS['text_dim'], font=(FONT_FAMILY, 11),
            command=lambda: self.log_text.delete('1.0', 'end')
        ).pack(side='left', padx=(0, 10))

        ctk.CTkButton(
            btn_frame, text="Save Log", height=32, width=100,
            fg_color=COLORS['bg_input'], hover_color=COLORS['bg_card_hover'],
            border_color=COLORS['border'], border_width=1,
            text_color=COLORS['text_dim'], font=(FONT_FAMILY, 11),
            command=self._save_log
        ).pack(side='left')

        # Initial log entry
        self._log("Application started.")
        self._log(f"App directory: {APP_DIR}")
        self._log(f"Districts available: {len(district_config)}")

    # ----------------------------------------------------------------
    # STATUS BAR
    # ----------------------------------------------------------------
    def _build_status_bar(self):
        bar = ctk.CTkFrame(self, fg_color=COLORS['bg_card'], corner_radius=0, height=28)
        bar.pack(fill='x', side='bottom')
        bar.pack_propagate(False)

        ctk.CTkLabel(
            bar, text=f"  v{APP_VERSION}  |  {len(district_config)} districts",
            font=(FONT_FAMILY, 10), text_color=COLORS['text_muted']
        ).pack(side='left', padx=10)

        self.clock_label = ctk.CTkLabel(
            bar, text="", font=(FONT_FAMILY, 10), text_color=COLORS['text_muted']
        )
        self.clock_label.pack(side='right', padx=10)
        self._update_clock()

    # ----------------------------------------------------------------
    # HELPER: Card builder
    # ----------------------------------------------------------------
    def _card(self, parent, title):
        """Create a styled card frame with a title label."""
        card = ctk.CTkFrame(
            parent, fg_color=COLORS['bg_card'],
            border_color=COLORS['border'], border_width=1,
            corner_radius=8
        )

        ctk.CTkLabel(
            card, text=title,
            font=(FONT_FAMILY, 13, 'bold'), text_color=COLORS['text'],
            anchor='w'
        ).pack(fill='x', padx=15, pady=(12, 2))

        return card

    # ----------------------------------------------------------------
    # CLOCK
    # ----------------------------------------------------------------
    def _update_clock(self):
        now = datetime.now().strftime("%Y-%m-%d  %H:%M:%S")
        self.clock_label.configure(text=now)
        self.after(1000, self._update_clock)

    # ----------------------------------------------------------------
    # QUEUE POLLING
    # ----------------------------------------------------------------
    def _poll_queue(self):
        """Poll the message queue and route to appropriate displays."""
        try:
            while True:
                tag, text = self.msg_queue.get_nowait()
                self._append_live(text, tag)
                self._log(text)

                # Parse progress info from output
                self._parse_progress(text)
        except queue.Empty:
            pass

        self.after(100, self._poll_queue)

    def _parse_progress(self, text):
        """Try to extract progress info from download output text."""
        # Match patterns like [  10/200] or [10/200]
        match = re.search(r'\[\s*(\d+)/(\d+)\]', text)
        if match:
            current = int(match.group(1))
            total = int(match.group(2))
            if total > 0:
                pct = current / total
                self.progress_bar.set(pct)
                self.progress_label.configure(
                    text=f"Downloading: {current}/{total} ({pct*100:.1f}%)"
                )

    # ----------------------------------------------------------------
    # LOGGING
    # ----------------------------------------------------------------
    def _log(self, text):
        """Append text to the activity log."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        try:
            self.log_text.insert('end', f"[{timestamp}] {text}\n")
            self.log_text.see('end')
        except Exception:
            pass

    def _append_live(self, text, tag='stdout'):
        """Append text to the live output display."""
        try:
            self.live_output.insert('end', text + '\n')
            self.live_output.see('end')
        except Exception:
            pass

    # ----------------------------------------------------------------
    # DISTRICT LIST MANAGEMENT
    # ----------------------------------------------------------------
    def _get_district_list(self):
        """Get list of available districts."""
        try:
            districts = get_available_districts()
            self._districts_cache = districts
            return districts
        except Exception as e:
            self._log(f"Error loading districts: {e}")
            return []

    def _refresh_districts(self):
        """Refresh the district dropdown."""
        districts = self._get_district_list()
        if districts:
            self.district_menu.configure(values=districts)
            self._log(f"Refreshed: {len(districts)} districts found.")
        else:
            self.district_menu.configure(values=["No districts found"])
            self._log("No districts found.")

    def _on_district_change(self, *args):
        """Handle district selection change."""
        dist = self.district_var.get()
        if dist in ("Select a district...", "No districts found"):
            return

        config = district_config.get(dist, {})
        base_ip = config.get('BASE_IP', 'N/A')

        # Check credentials
        u, p = get_district_credentials(dist)
        has_creds = "Yes" if (u and p) else "No"

        # Check point list
        path, source = get_point_list_path(dist)
        if path:
            from niagara_url_generator import load_point_list
            points = load_point_list(path)
            pts_info = f"{len(points)} points ({source})"
        else:
            pts_info = "No point list"

        # VPN type — V2.0: VPN_DATA is just the type string
        vpn_type = config.get('VPN_DATA', 'na')
        if vpn_type.lower() in ('na', 'n/a', ''):
            vpn_display = "None"
        else:
            vpn_display = vpn_type

        info = f"IP: {base_ip}  |  Creds: {has_creds}  |  {pts_info}  |  VPN: {vpn_display}"
        self.dist_info.configure(text=info)
        self._log(f"Selected: {dist} — {info}")

    # ----------------------------------------------------------------
    # BROWSE OUTPUT
    # ----------------------------------------------------------------
    def _browse_output(self):
        """Open directory browser for output path."""
        path = filedialog.askdirectory(
            initialdir=self.output_var.get(),
            title="Select Output Directory"
        )
        if path:
            self.output_var.set(path)
            self._log(f"Output directory: {path}")

    # ----------------------------------------------------------------
    # OPEN FOLDER
    # ----------------------------------------------------------------
    def _open_folder(self, path):
        """Open a folder in the system file manager."""
        try:
            if sys.platform == 'win32':
                os.startfile(path)
            elif sys.platform == 'darwin':
                import subprocess
                subprocess.Popen(['open', path])
            else:
                import subprocess
                subprocess.Popen(['xdg-open', path])
        except Exception as e:
            self._log(f"Error opening folder: {e}")

    # ----------------------------------------------------------------
    # RELOAD CONFIG
    # ----------------------------------------------------------------
    def _reload_config(self):
        """Reload environment and config."""
        self._log("Reloading configuration...")

        # Re-read .env display
        env_path = os.path.join(APP_DIR, '.env')
        if os.path.exists(env_path):
            try:
                with open(env_path, 'r') as f:
                    content = f.read()
                masked = re.sub(r'(_PASS=)(.+)', r'\1****', content)
                self.env_text.delete('1.0', 'end')
                self.env_text.insert('1.0', masked)
                self._log("Config reloaded from .env")
            except Exception as e:
                self._log(f"Error reloading .env: {e}")
        else:
            self._log(".env file not found")

        # Refresh districts
        self._refresh_districts()
        self._populate_district_list()

    # ----------------------------------------------------------------
    # SAVE LOG
    # ----------------------------------------------------------------
    def _save_log(self):
        """Save activity log to file."""
        path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
            initialfile=f"niagara_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
            title="Save Activity Log"
        )
        if path:
            try:
                content = self.log_text.get('1.0', 'end')
                with open(path, 'w') as f:
                    f.write(content)
                self._log(f"Log saved to: {path}")
            except Exception as e:
                self._log(f"Error saving log: {e}")
                messagebox.showerror("Error", f"Could not save log:\n{e}")

    # ----------------------------------------------------------------
    # STATUS UPDATE
    # ----------------------------------------------------------------
    def _set_status(self, text, color=None):
        """Update the status indicator in the header."""
        if color is None:
            color = COLORS['text_dim']
        self.status_label.configure(text=text, text_color=color)

        # Update dot color
        if color == COLORS['success']:
            self.status_dot.configure(text_color=COLORS['success'])
        elif color == COLORS['error']:
            self.status_dot.configure(text_color=COLORS['error'])
        elif color == COLORS['warning']:
            self.status_dot.configure(text_color=COLORS['warning'])
        else:
            self.status_dot.configure(text_color=COLORS['text_muted'])

    # ----------------------------------------------------------------
    # DOWNLOAD CONTROL
    # ----------------------------------------------------------------
    def _start_download(self):
        """Start the download process."""
        dist = self.district_var.get()
        if dist in ("Select a district...", "No districts found"):
            messagebox.showwarning("No District", "Please select a district first.")
            return

        # Validate inputs
        try:
            days = int(self.days_var.get())
            if days < 1:
                raise ValueError
        except ValueError:
            messagebox.showwarning("Invalid Days", "Please enter a valid number of days (1+).")
            return

        try:
            workers = int(self.workers_var.get())
            if workers < 1:
                raise ValueError
        except ValueError:
            messagebox.showwarning("Invalid Workers", "Please enter a valid number of workers (1+).")
            return

        output_dir = self.output_var.get()
        if not output_dir:
            messagebox.showwarning("No Output", "Please select an output directory.")
            return

        # Check credentials
        u, p = get_district_credentials(dist)
        if not (u and p):
            messagebox.showerror(
                "No Credentials",
                f"No credentials found for {dist}.\n\n"
                "Please configure them in .env file:\n"
                f"  {dist}_USER=...\n"
                f"  {dist}_PASS=..."
            )
            return

        # Check point list
        path, _ = get_point_list_path(dist)
        if not path:
            messagebox.showerror(
                "No Point List",
                f"No point list found for {dist}.\n\n"
                "Run the point list fetcher first:\n"
                f"  python fetch_pointlist.py --district {dist}"
            )
            return

        # Disable start, enable stop
        self.is_running = True
        self.start_btn.configure(state='disabled')
        self.stop_btn.configure(state='normal')
        self._set_status("Downloading...", COLORS['info'])

        # Clear live output
        self.live_output.delete('1.0', 'end')
        self.progress_bar.set(0)
        self.progress_label.configure(text="Initializing...")

        self._log(f"Starting download: {dist} ({days} days, {workers} workers)")

        # Launch download thread
        thread = threading.Thread(
            target=self._download_thread,
            args=(dist, days, workers, output_dir),
            daemon=True
        )
        thread.start()

    def _stop_download(self):
        """Stop the download process."""
        self.is_running = False
        self._set_status("Stopping...", COLORS['warning'])
        self._log("Stop requested — finishing current downloads...")

    def _download_thread(self, district, days, workers, output_dir):
        """Background thread for downloading data."""
        # Capture stdout/stderr
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = OutputCapture(self.msg_queue, 'stdout')
        sys.stderr = OutputCapture(self.msg_queue, 'stderr')

        try:
            self.msg_queue.put(('stdout', f"Authenticating to {district}..."))

            # Authenticate
            auth = NiagaraAuth(district)
            cookies = auth.login(headless=True)

            if not cookies:
                self.msg_queue.put(('stderr', f"Authentication failed for {district}"))
                self._finish_download(False, "Authentication failed")
                return

            self.msg_queue.put(('stdout', "Authentication successful!"))

            # Generate URLs
            self.msg_queue.put(('stdout', f"Generating URLs for {days} days..."))

            url_gen = URLGenerator(district)
            url_list = url_gen.generate(days=days)

            self.msg_queue.put(('stdout', f"Generated {len(url_list)} URLs"))

            # Filter existing
            url_list, skipped = filter_existing_files(url_list, output_dir)
            if skipped:
                self.msg_queue.put(('stdout', f"Skipped {skipped} existing files"))

            if not url_list:
                self.msg_queue.put(('stdout', "All files already downloaded!"))
                self._finish_download(True, "Complete — all files exist")
                return

            # Download
            self.msg_queue.put(('stdout', f"Downloading {len(url_list)} files with {workers} workers..."))

            def progress_cb(current, total, point_path, status):
                if not self.is_running:
                    return
                pct = (current / total * 100) if total > 0 else 0
                sym = {'success': 'OK', 'empty': 'EMPTY', 'failed': 'FAIL'}.get(status, '??')
                self.msg_queue.put(('stdout', f"[{current:4d}/{total}] {pct:5.1f}% {sym:>5} | {point_path[:60]}"))

            engine = DownloadEngine(
                cookies=cookies,
                max_workers=workers,
                progress_callback=progress_cb
            )

            stats = engine.download_batch_with_resume(
                url_list=url_list,
                output_folder=output_dir,
                district=district
            )
            engine.close()

            # Report results
            self.msg_queue.put(('stdout', ""))
            self.msg_queue.put(('stdout', "=" * 60))
            self.msg_queue.put(('stdout', f"  DOWNLOAD COMPLETE"))
            self.msg_queue.put(('stdout', f"  {stats.summary()}"))
            self.msg_queue.put(('stdout', "=" * 60))

            if stats.errors:
                self.msg_queue.put(('stderr', f"  {len(stats.errors)} errors occurred"))
                for pt, err in stats.errors[:10]:
                    self.msg_queue.put(('stderr', f"    {pt}: {err}"))
                if len(stats.errors) > 10:
                    self.msg_queue.put(('stderr', f"    ... and {len(stats.errors) - 10} more"))

            success = stats.failed == 0
            status_msg = f"Done: {stats.success} OK, {stats.failed} failed, {stats.empty} empty"
            self._finish_download(success, status_msg)

        except Exception as e:
            logger.exception("Download thread error")
            self.msg_queue.put(('stderr', f"Error: {e}"))
            self._finish_download(False, f"Error: {e}")

        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr

    def _finish_download(self, success, message):
        """Called when download finishes (from background thread)."""
        def _update():
            self.is_running = False
            self.start_btn.configure(state='normal')
            self.stop_btn.configure(state='disabled')

            if success:
                self._set_status(message, COLORS['success'])
                self.progress_bar.set(1.0)
                self.progress_label.configure(text=message)
            else:
                self._set_status(message, COLORS['error'])
                self.progress_label.configure(text=message)

            self._log(message)

        # Schedule UI update on main thread
        self.after(0, _update)


# ============================================================================
# MAIN
# ============================================================================
def main():
    """Application entry point."""
    setup_logging()

    if not CTK_AVAILABLE:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror(
            "Missing Dependency",
            "CustomTkinter is not installed.\n\n"
            "Install it with:\n"
            "  pip install customtkinter\n\n"
            "The application cannot start without it."
        )
        root.destroy()
        sys.exit(1)

    app = NiagaraApp()
    app.mainloop()


if __name__ == '__main__':
    main()
