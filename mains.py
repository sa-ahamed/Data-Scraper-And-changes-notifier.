#!/usr/bin/env python3
"""
Selenium-based Site Crawler + Change Tracker + Email Notifier

Features:
  • Headless Chrome rendering
  • Multi-threaded crawling under same domain
  • Saves pages as Markdown
  • Detects content changes and generates diffs
  • Sends formatted HTML + text email notifications (via SMTP SSL)
"""

from __future__ import annotations
import os
import re
import time
import html
import queue
import socket
import smtplib
import urllib.parse
from datetime import datetime, timezone
from typing import Set, Optional, List, Dict, Tuple
import difflib
import threading
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException, TimeoutException
from bs4 import BeautifulSoup
import html2text

# -------------------------
# CONFIGURATION
# -------------------------
START_URL = "https://example.com/"
THREADS = 4
REQUEST_TIMEOUT = 20  # seconds
USER_AGENT = "SiteCrawler/1.0 (+https://example.com/)"

ALLOWLIST_PATH_PREFIXES: List[str] = []
BLOCKLIST_PATH_PREFIXES: List[str] = ["/admin", "/wp-login.php", "/cart"]

OUTPUT_BASE_DIR = "crawls"
INCLUDE_PATH_IN_FILENAME = True
MAX_FILENAME_LEN = 200

TRUNCATE_DIFF_LINES = 150
DIFF_CONTEXT_LINES = 3

EMAIL_ENABLED = True
SMTP_HOST = "smtp.example.com"
SMTP_PORT = 465  # SSL port
SMTP_USERNAME = "email@example.com"
SMTP_PASSWORD = "Passeord"
EMAIL_FROM = "email@example.com"
EMAIL_TO = ["newemail@example.com"]
EMAIL_SUBJECT_PREFIX = "[SiteCrawler]"

SKIP_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg',
                   '.pdf', '.zip', '.rar', '.7z', '.tar', '.gz',
                   '.mp3', '.wav', '.mp4', '.avi', '.mov', '.ogg',
                   '.woff', '.woff2', '.ttf', '.eot', '.ico')

MAX_RETRY = 2

# -------------------------
# UTILITIES
# -------------------------
INVALID_FILENAME_CHARS = re.compile(r'[^A-Za-z0-9\-_\. ]+')

def log(msg: str):
    now = datetime.now(timezone.utc).astimezone().isoformat()
    print(f"[{now}] {msg}", flush=True)

def sanitize_filename(s: str, max_len: Optional[int] = MAX_FILENAME_LEN) -> str:
    if not s:
        s = "untitled"
    s = html.unescape(s).strip()
    s = re.sub(r'\s+', ' ', s)
    s = INVALID_FILENAME_CHARS.sub('_', s)
    if max_len and len(s) > max_len:
        s = s[:max_len].rstrip('_')
    return s

def make_site_folder_name(start_url: str) -> str:
    parsed = urllib.parse.urlparse(start_url)
    host = parsed.netloc.replace(':', '_') or "site"
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{host}_{ts}"

def is_same_domain(url: str, base_netloc: str) -> bool:
    try:
        parsed = urllib.parse.urlparse(url)
        return parsed.netloc == base_netloc or parsed.netloc == ""
    except Exception:
        return False

def normalize_url(href: str, base: str) -> str:
    if not href:
        return ""
    href = href.strip().split('#', 1)[0]
    abs_url = urllib.parse.urljoin(base, href)
    if abs_url.endswith('/') and len(abs_url) > len(urllib.parse.urlparse(abs_url).scheme + "://"):
        abs_url = abs_url.rstrip('/')
    return abs_url

def path_allowed(path: str) -> bool:
    if not path.startswith('/'):
        path = '/' + path
    for bp in BLOCKLIST_PATH_PREFIXES:
        if bp and path.startswith(bp):
            return False
    if not ALLOWLIST_PATH_PREFIXES:
        return True
    for ap in ALLOWLIST_PATH_PREFIXES:
        if path.startswith(ap):
            return True
    return False

def looks_like_html(url: str) -> bool:
    path = urllib.parse.urlparse(url).path.lower()
    for ext in SKIP_EXTENSIONS:
        if path.endswith(ext):
            return False
    return True

# -------------------------
# SELENIUM FETCH
# -------------------------
chrome_options = Options()
chrome_options.add_argument('--headless=new')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument(f'user-agent={USER_AGENT}')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--window-size=1920,1080')

def fetch_page_selenium(url: str) -> Optional[str]:
    for attempt in range(1, MAX_RETRY + 1):
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.set_page_load_timeout(REQUEST_TIMEOUT)
            driver.get(url)
            html_text = driver.page_source
            driver.quit()
            return html_text
        except (WebDriverException, TimeoutException) as e:
            log(f"Retry {attempt}/{MAX_RETRY} for {url}: {e}")
            time.sleep(1)
    log(f"Failed to fetch {url} via Selenium.")
    return None

# -------------------------
# LINK EXTRACTION & MARKDOWN
# -------------------------
def extract_links(html_text: str, base_url: str) -> Set[str]:
    soup = BeautifulSoup(html_text, "html.parser")
    found = set()
    for tag in soup.find_all(["a", "area", "link"]):
        href = tag.get("href")
        if href:
            normalized = normalize_url(href, base_url)
            found.add(normalized)
    return found

def html_to_markdown(html_text: str, base_url: str) -> str:
    h = html2text.HTML2Text()
    h.ignore_images = True
    h.ignore_links = False
    h.body_width = 0
    md = h.handle(html_text or "")
    header = f"<!-- Source: {base_url} -->\n\n"
    return header + md

def save_markdown(output_dir: str, url: str, html_text: str) -> str:
    soup = BeautifulSoup(html_text or "", "html.parser")
    title_tag = soup.title.string.strip() if soup.title and soup.title.string else None
    parsed = urllib.parse.urlparse(url)
    url_path = parsed.path or "/"
    title = title_tag or parsed.path or url
    base_title = sanitize_filename(title)
    if INCLUDE_PATH_IN_FILENAME:
        safe_path = sanitize_filename(url_path.strip('/') or "root", 80)
        filename = f"{base_title}__{safe_path}.md"
    else:
        filename = f"{base_title}.md"
    filename = filename[:MAX_FILENAME_LEN]
    if not filename.lower().endswith(".md"):
        filename += ".md"
    filepath = os.path.join(output_dir, filename)
    md = html_to_markdown(html_text, url)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(md)
    log(f"Saved: {filepath}")
    return filepath

# -------------------------
# CRAWLER
# -------------------------
def crawl(start_url: str, output_base_dir: str, threads: int = THREADS) -> str:
    parsed_start = urllib.parse.urlparse(start_url)
    base_netloc = parsed_start.netloc
    if not parsed_start.scheme:
        raise ValueError("START_URL must include scheme (http/https)")

    site_folder_name = make_site_folder_name(start_url)
    output_dir = os.path.join(output_base_dir, site_folder_name)
    os.makedirs(output_dir, exist_ok=True)
    log(f"Starting crawl: {start_url}")

    visited_lock = threading.Lock()
    visited: Set[str] = set()
    q: queue.Queue[str] = queue.Queue()
    q.put(start_url)

    def worker():
        while True:
            try:
                url = q.get_nowait()
            except queue.Empty:
                return
            with visited_lock:
                if url in visited:
                    q.task_done()
                    continue
                visited.add(url)
            log(f"Fetching: {url}")
            if not looks_like_html(url):
                q.task_done()
                continue
            html_text = fetch_page_selenium(url)
            if not html_text or "<html" not in html_text.lower():
                q.task_done()
                continue
            save_markdown(output_dir, url, html_text)
            for link in extract_links(html_text, url):
                if not is_same_domain(link, base_netloc):
                    continue
                link_parsed = urllib.parse.urlparse(link)
                if not path_allowed(link_parsed.path or "/"):
                    continue
                with visited_lock:
                    if link not in visited:
                        q.put(link)
            q.task_done()

    workers = []
    for _ in range(threads):
        t = threading.Thread(target=worker, daemon=True)
        t.start()
        workers.append(t)
    q.join()
    for t in workers:
        t.join(timeout=1)
    log(f"Crawl complete. {len(visited)} pages saved -> {output_dir}")
    return output_dir

# -------------------------
# DIFF / CHANGE TRACKING & EMAIL
# -------------------------
def list_crawl_folders(base_dir: str) -> List[str]:
    if not os.path.exists(base_dir):
        return []
    entries = [os.path.join(base_dir, p) for p in os.listdir(base_dir)
               if os.path.isdir(os.path.join(base_dir, p))]
    entries.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return entries

def read_all_pages(folder: str) -> Dict[str, str]:
    pages = {}
    for fname in os.listdir(folder):
        if fname.lower().endswith(".md"):
            path = os.path.join(folder, fname)
            with open(path, "r", encoding="utf-8") as f:
                pages[fname] = f.read()
    return pages

def generate_diffs(old_pages: Dict[str, str], new_pages: Dict[str, str],
                   context: int = DIFF_CONTEXT_LINES,
                   truncate: Optional[int] = TRUNCATE_DIFF_LINES) -> Dict[str, str]:
    diffs = {}
    all_files = set(old_pages) | set(new_pages)
    for fname in sorted(all_files):
        old = old_pages.get(fname, "").splitlines(keepends=True)
        new = new_pages.get(fname, "").splitlines(keepends=True)
        if old == new:
            continue
        ud = difflib.unified_diff(old, new, fromfile=f"old/{fname}", tofile=f"new/{fname}",
                                  lineterm="", n=context)
        ud_lines = list(ud)
        if truncate and len(ud_lines) > truncate:
            ud_lines = ud_lines[:truncate] + ["... (diff truncated) ..."]
        diffs[fname] = "\n".join(ud_lines)
    return diffs

def send_email_html(subject: str, plain_text: str, html_body: str):
    msg = MIMEMultipart("alternative")
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(EMAIL_TO)
    msg["Subject"] = subject

    msg.attach(MIMEText(plain_text, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))
    log("Connecting to SMTP server with SSL...")

    try:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=30) as smtp:
            smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
            smtp.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
        log("Email sent successfully.")
    except Exception as e:
        log(f"Email send failed: {e}")

def build_email_html(site: str, changed_pages: Dict[str, str], new_folder: str, old_folder: str) -> Tuple[str, str]:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    plain_lines = [
        f"Site change report for {site}",
        f"Detected {len(changed_pages)} changed pages.",
        f"New crawl: {new_folder}",
        f"Previous: {old_folder}",
        f"Time: {timestamp}",
        "",
    ]
    html_parts = [
        f"<h2 style='font-family:Arial'>Site Change Report</h2>",
        f"<p><b>Site:</b> {site}<br><b>Changed pages:</b> {len(changed_pages)}<br><b>Time:</b> {timestamp}</p>",
        "<hr>"
    ]
    for fname, diff_text in changed_pages.items():
        snippet = diff_text if len(diff_text) < 4000 else diff_text[:4000] + "\n... (truncated) ..."
        plain_lines.append(f"--- {fname} ---\n{snippet}\n")
        safe_diff = html.escape(snippet)
        html_parts.append(f"<details><summary><b>{html.escape(fname)}</b></summary>"
                          f"<pre style='background:#f7f7f7;padding:10px;border-radius:8px;"
                          f"overflow-x:auto;font-family:monospace'>{safe_diff}</pre></details><br>")
    html_parts.append("<hr><p style='font-size:small;color:#555'>Generated automatically by SiteCrawler.</p>")
    return "\n".join(plain_lines), "\n".join(html_parts)

# -------------------------
# MAIN
# -------------------------
def main():
    log("Crawler started.")
    os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)

    new_folder = crawl(START_URL, OUTPUT_BASE_DIR, threads=THREADS)
    folders = list_crawl_folders(OUTPUT_BASE_DIR)

    if len(folders) < 2:
        log("First crawl completed. Nothing to compare yet.")
        return

    new_pages = read_all_pages(folders[0])
    old_pages = read_all_pages(folders[1])
    diffs = generate_diffs(old_pages, new_pages)

    if not diffs:
        log("No changes detected.")
        return

    log(f"Detected {len(diffs)} changed pages.")

    plain_text, html_body = build_email_html(START_URL, diffs, folders[0], folders[1])

    summary_path = os.path.join(folders[0], "summary.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(plain_text)
    log(f"Saved summary: {summary_path}")

    if EMAIL_ENABLED:
        subject = f"{EMAIL_SUBJECT_PREFIX} Changes detected on {urllib.parse.urlparse(START_URL).netloc}"
        send_email_html(subject, plain_text, html_body)
    else:
        log("Email notifications disabled.")

    log("Task complete.")

if __name__ == "__main__":
    main()

