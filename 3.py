#!/usr/bin/env python3
"""
Selenium-based Site Crawler + Change Tracker + Webhook Notifier

Features:
  • Headless Chrome rendering
  • Multi-threaded crawling under same domain
  • Saves pages as Markdown
  • Detects content changes (new, updated, deleted files)
  • Sends formatted JSON notifications to a webhook
  • **NEW: Detects and removes duplicate content.**
  • **NEW: Strictly scopes crawling to the initial domain.**
"""

from __future__ import annotations
import os
import re
import time
import html
import queue
import socket
import smtplib # Kept for backward compatibility, but won't be used for notifications
import urllib.parse
import hashlib # For creating unique filenames based on URL and for content hashing
import json # For webhook payload
import requests # For sending webhook requests
from datetime import datetime, timezone
from typing import Set, Optional, List, Dict, Tuple, Any
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
START_URL = "https://www.ville.kirkland.qc.ca/" # <--- IMPORTANT: replace this with yours
THREADS = 1
REQUEST_TIMEOUT = 20  # seconds
USER_AGENT = "SiteCrawler/1.0 (+https://www.ville.kirkland.qc.ca/)" # <--- IMPORTANT: replace this with yours

WEBHOOK_ENABLED = True
WEBHOOK_URL = "https://connect.buyaqsa.com/webhook.php" # <--- IMPORTANT: SET YOUR WEBHOOK URL HERE

ALLOWLIST_PATH_PREFIXES: List[str] = []
BLOCKLIST_PATH_PREFIXES: List[str] = ["/wp-admin", "/admin", "/wp-login.php", "/cart", "/wp"]

OUTPUT_BASE_DIR = "crawls"
MAX_FILENAME_LEN = 200 # Still useful for base name, but URL hash handles uniqueness

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
    """Sanitizes a string for use in a filename."""
    if not s:
        s = "untitled"
    s = html.unescape(s).strip()
    s = re.sub(r'\s+', ' ', s)
    s = INVALID_FILENAME_CHARS.sub('_', s)
    if max_len and len(s) > max_len:
        s = s[:max_len].rstrip('_')
    return s

def url_to_filename(url: str, title: Optional[str] = None) -> str:
    """Generates a unique and readable filename for a URL."""
    # Create a hash of the URL for uniqueness
    url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()[:8] # Short hash

    # NEW: Handle common "homepage" URL variations more consistently for filename,
    # though the content hash will ultimately determine duplication.
    # This might make filenames for '/' and '/index.php' more similar if their titles are the same.
    parsed = urllib.parse.urlparse(url)
    path = parsed.path.strip('/')
    if not path or path == 'index.php' or path == 'index.html':
        # Treat root, /index.php, /index.html as effectively the same for base name part
        base_name_candidate = sanitize_filename(parsed.netloc or "root", MAX_FILENAME_LEN - 9 - 3)
    else:
        base_name_candidate = sanitize_filename(path, MAX_FILENAME_LEN - 9 - 3)

    if title:
        # Prioritize a sanitized title for readability
        base_name = sanitize_filename(title, MAX_FILENAME_LEN - 9 - 3)
    else:
        base_name = base_name_candidate


    return f"{base_name}__{url_hash}.md"

def make_site_folder_name(start_url: str) -> str:
    parsed = urllib.parse.urlparse(start_url)
    host = parsed.netloc.replace(':', '_') or "site"
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{host}_{ts}"

def is_same_domain(url: str, base_netloc: str) -> bool:
    try:
        parsed = urllib.parse.urlparse(url)
        # NEW: Ensure strict domain match. Subdomains are considered different.
        # e.g., for base_netloc 'example.com', 'www.example.com' is different.
        # If you need to include subdomains, adjust this logic (e.g., check .endswith(base_netloc)).
        return parsed.netloc == base_netloc
    except Exception:
        return False

def normalize_url(href: str, base: str) -> str:
    if not href:
        return ""
    href = href.strip().split('#', 1)[0]
    abs_url = urllib.parse.urljoin(base, href)
    
    # NEW: Remove common default filenames from the end of the URL path for canonicalization
    parsed_abs_url = urllib.parse.urlparse(abs_url)
    path = parsed_abs_url.path
    if path.endswith(('/index.php', '/index.html', '/home.html', '/default.html')):
        path = path[:path.rfind('/') + 1] # Keep trailing slash if it was there before the filename
        abs_url = urllib.parse.urlunparse(parsed_abs_url._replace(path=path))

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
        except (WebDriverException, TimeoutException, socket.timeout) as e:
            log(f"Retry {attempt}/{MAX_RETRY} for {url}: {type(e).__name__} - {e}")
            time.sleep(1) # Wait before retrying
        except Exception as e:
            log(f"Unexpected error fetching {url}: {type(e).__name__} - {e}")
            driver.quit() # Ensure driver is closed on unexpected errors
            return None
    log(f"Failed to fetch {url} via Selenium after {MAX_RETRY} attempts.")
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

def html_to_markdown(html_text: str, source_url: str) -> str:
    h = html2text.HTML2Text()
    h.ignore_images = True
    h.ignore_links = False
    h.body_width = 0
    md = h.handle(html_text or "")
    header = f"<!-- Source: {source_url} -->\n\n"
    return header + md

def save_markdown(output_dir: str, url: str, html_text: str) -> str:
    """
    Saves markdown content to a file, generating a unique filename based on the URL and title.
    Returns the generated filename (relative to output_dir).
    """
    soup = BeautifulSoup(html_text or "", "html.parser")
    title_tag = soup.title.string.strip() if soup.title and soup.title.string else None

    # Generate filename using the URL and title
    filename = url_to_filename(url, title_tag)
    filepath = os.path.join(output_dir, filename)

    md = html_to_markdown(html_text, url)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(md)
    return filename # Return the relative filename

# -------------------------
# CRAWLER
# -------------------------
def crawl(start_url: str, output_base_dir: str, threads: int = THREADS) -> Tuple[str, Dict[str, str]]:
    parsed_start = urllib.parse.urlparse(start_url)
    base_netloc = parsed_start.netloc
    if not parsed_start.scheme:
        raise ValueError("START_URL must include scheme (http/https)")

    site_folder_name = make_site_folder_name(start_url)
    output_dir = os.path.join(output_base_dir, site_folder_name)
    os.makedirs(output_dir, exist_ok=True)
    log(f"Starting crawl: {start_url} into {output_dir}")

    visited_lock = threading.Lock()
    visited: Set[str] = set() # Stores URLs that have been processed or queued
    q: queue.Queue[str] = queue.Queue()
    q.put(start_url)

    # NEW: Store content hashes to detect duplicates
    content_hashes: Dict[str, str] = {} # {content_hash: canonical_url_that_saved_it}
    content_hashes_lock = threading.Lock()

    # Store a mapping of filename to URL and content, for change detection and later cleanup
    crawled_files: Dict[str, str] = {} # {filename: markdown_content}
    crawled_urls_to_filenames: Dict[str, str] = {} # {url: filename}
    crawled_files_lock = threading.Lock()


    def worker():
        nonlocal crawled_files, content_hashes, crawled_urls_to_filenames
        while True:
            try:
                url = q.get_nowait()
            except queue.Empty:
                return

            # NEW: Normalize URL before checking visited, but also ensure it's on the right domain
            normalized_url = normalize_url(url, start_url)

            # NEW: Strict domain check
            if not is_same_domain(normalized_url, base_netloc):
                log(f"Skipping (outside domain): {normalized_url}")
                q.task_done()
                continue
            
            with visited_lock:
                if normalized_url in visited:
                    q.task_done()
                    continue
                visited.add(normalized_url)

            log(f"Fetching: {normalized_url}")
            if not looks_like_html(normalized_url):
                log(f"Skipping (not HTML): {normalized_url}")
                q.task_done()
                continue

            html_text = fetch_page_selenium(normalized_url)
            if not html_text or "<html" not in html_text.lower():
                log(f"Skipping (no valid HTML content): {normalized_url}")
                q.task_done()
                continue

            md_content = html_to_markdown(html_text, normalized_url)
            content_hash = hashlib.md5(md_content.encode('utf-8')).hexdigest()

            with content_hashes_lock:
                if content_hash in content_hashes:
                    # Duplicate content detected
                    original_url_for_content = content_hashes[content_hash]
                    log(f"DUPLICATE CONTENT: {normalized_url} is identical to {original_url_for_content}. Skipping save.")
                    
                    # Instead of saving, we might want to map this URL to the existing filename
                    # This ensures change detection correctly identifies the content associated with a URL
                    with crawled_files_lock:
                        existing_filename = crawled_urls_to_filenames.get(original_url_for_content)
                        if existing_filename:
                            crawled_urls_to_filenames[normalized_url] = existing_filename
                            # The content isn't added to crawled_files directly for this URL,
                            # as it's already represented by the canonical URL's filename.
                    q.task_done()
                    continue
                else:
                    content_hashes[content_hash] = normalized_url # Store the first URL that provided this content

            # Save markdown and get the generated filename
            filename = save_markdown(output_dir, normalized_url, html_text)
            
            with crawled_files_lock:
                crawled_files[filename] = md_content # Store content mapped by filename
                crawled_urls_to_filenames[normalized_url] = filename # Map URL to its saved filename

            for link in extract_links(html_text, normalized_url):
                if not is_same_domain(link, base_netloc): # This check is redundant due to worker's early exit but good as a fail-safe
                    continue
                link_parsed = urllib.parse.urlparse(link)
                if not path_allowed(link_parsed.path or "/"):
                    continue
                normalized_link = normalize_url(link, normalized_url) # Ensure consistent normalization
                with visited_lock:
                    if normalized_link not in visited: # Only add to queue if not yet visited/queued
                        q.put(normalized_link)
            q.task_done()

    workers = []
    for _ in range(threads):
        t = threading.Thread(target=worker, daemon=True)
        t.start()
        workers.append(t)
    q.join() # Wait for all tasks to be done

    for t in workers:
        t.join(timeout=5)

    log(f"Crawl complete. {len(visited)} pages processed. {len(crawled_files)} unique content files saved in {output_dir}")

    # NEW: After crawling, explicitly delete any markdown files in the new_folder
    # that correspond to duplicate content which was identified but not saved.
    # This ensures only unique content files remain.
    all_md_files_in_dir = {f for f in os.listdir(output_dir) if f.lower().endswith(".md")}
    saved_md_files = set(crawled_files.keys())

    deleted_duplicates_count = 0
    for filename_to_check in all_md_files_in_dir:
        if filename_to_check not in saved_md_files:
            filepath_to_delete = os.path.join(output_dir, filename_to_check)
            try:
                os.remove(filepath_to_delete)
                log(f"Deleted duplicate content file: {filepath_to_delete}")
                deleted_duplicates_count += 1
            except Exception as e:
                log(f"Error deleting file {filepath_to_delete}: {e}")
    if deleted_duplicates_count > 0:
        log(f"Removed {deleted_duplicates_count} duplicate content files.")

    return output_dir, crawled_files

# -------------------------
# CHANGE TRACKING & WEBHOOK
# -------------------------
def list_crawl_folders(base_dir: str) -> List[str]:
    if not os.path.exists(base_dir):
        return []
    parsed_start_url_for_filter = urllib.parse.urlparse(START_URL)
    host_filter = parsed_start_url_for_filter.netloc.replace(':', '_')

    entries = [os.path.join(base_dir, p) for p in os.listdir(base_dir)
               if os.path.isdir(os.path.join(base_dir, p)) and p.startswith(host_filter)
              ]
    entries.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return entries

def read_all_pages_from_folder(folder: str) -> Dict[str, str]:
    pages = {}
    if not os.path.exists(folder):
        return pages
    for fname in os.listdir(folder):
        if fname.lower().endswith(".md"):
            path = os.path.join(folder, fname)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    pages[fname] = f.read()
            except Exception as e:
                log(f"Error reading file {path}: {e}")
    return pages

# Change type definitions
CHANGE_TYPE_ADDED = "added"
CHANGE_TYPE_UPDATED = "updated"
CHANGE_TYPE_DELETED = "deleted"

def detect_changes(old_pages: Dict[str, str], new_pages: Dict[str, str]) -> List[Dict[str, Any]]:
    changes: List[Dict[str, Any]] = []
    all_files = set(old_pages.keys()) | set(new_pages.keys())

    for fname in sorted(all_files):
        old_content = old_pages.get(fname)
        new_content = new_pages.get(fname)

        if old_content is None and new_content is not None:
            # New file added
            changes.append({
                "type": CHANGE_TYPE_ADDED,
                "filename": fname,
                "content": new_content.encode('utf-8') # Send as binary buffer
            })
        elif old_content is not None and new_content is None:
            # File deleted
            changes.append({
                "type": CHANGE_TYPE_DELETED,
                "filename": fname
            })
        elif old_content is not None and new_content is not None and old_content != new_content:
            # File updated
            changes.append({
                "type": CHANGE_TYPE_UPDATED,
                "filename": fname,
                "content": new_content.encode('utf-8') # Send as binary buffer
            })
    return changes

def send_webhook_notification(change_payload: Dict[str, Any]):
    if not WEBHOOK_ENABLED or not WEBHOOK_URL:
        log("Webhook notifications disabled or URL not set.")
        return

    # For 'added' and 'updated' changes, 'content' is bytes and needs to be base64 encoded for JSON
    # For 'deleted', 'content' might not exist or be None
    if 'content' in change_payload and isinstance(change_payload['content'], bytes):
        import base64
        change_payload['content'] = base64.b64encode(change_payload['content']).decode('utf-8')
        change_payload['content_encoding'] = 'base64'

    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.post(WEBHOOK_URL, json=change_payload, headers=headers, timeout=30)
        response.raise_for_status() # Raise an exception for HTTP errors
        log(f"Webhook notification sent for {change_payload['type']} {change_payload['filename']}. Status: {response.status_code}")
    except requests.exceptions.Timeout:
        log(f"Webhook request timed out for {change_payload.get('filename', 'unknown file')}")
    except requests.exceptions.RequestException as e:
        log(f"Error sending webhook notification for {change_payload.get('filename', 'unknown file')}: {e}")
    except Exception as e:
        log(f"An unexpected error occurred while sending webhook: {e}")

# -------------------------
# MAIN
# -------------------------
def main():
    log("Crawler started.")
    os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)

    # Perform the crawl and get the new folder path and the dictionary of crawled files
    new_folder, new_pages_data = crawl(START_URL, OUTPUT_BASE_DIR, threads=THREADS)
    folders = list_crawl_folders(OUTPUT_BASE_DIR)

    if len(folders) < 2:
        log("First crawl completed. Nothing to compare yet. Webhook not sent.")
        # If it's the first crawl, consider sending 'added' notifications for all files
        if WEBHOOK_ENABLED and WEBHOOK_URL:
            log("Sending 'added' notifications for all files from the first crawl...")
            for filename, content in new_pages_data.items():
                change_payload = {
                    "site_url": START_URL,
                    "crawl_id": os.path.basename(new_folder),
                    "type": CHANGE_TYPE_ADDED,
                    "filename": filename,
                    "content": content.encode('utf-8')
                }
                send_webhook_notification(change_payload)
            log("First crawl 'added' notifications sent.")
        return

    # For subsequent crawls, compare with the previous one
    # The 'new_pages_data' already contains the content of the newly crawled files.
    # We need to load 'old_pages' from the previous crawl folder.
    old_folder = folders[1]
    old_pages = read_all_pages_from_folder(old_folder) # Read contents from the previous folder

    changes = detect_changes(old_pages, new_pages_data) # Pass the dictionary from crawl directly

    if not changes:
        log("No changes detected.")
        return

    log(f"Detected {len(changes)} changes.")

    # Send webhook notifications for each change
    if WEBHOOK_ENABLED:
        log("Sending webhook notifications for detected changes...")
        for change in changes:
            full_payload = {
                "site_url": START_URL,
                "crawl_id": os.path.basename(new_folder), # Identifier for this crawl run
                **change # Merge the change dict into the full payload
            }
            send_webhook_notification(full_payload)
        log("All change webhook notifications sent.")
    else:
        log("Webhook notifications disabled.")

    # You might still want a summary file for local debugging/record keeping
    # For now, we'll keep a basic summary, but you can remove it if webhook is the only output.
    summary_plain_lines = [
        f"Site change report for {START_URL}",
        f"Crawl ID: {os.path.basename(new_folder)}",
        f"Detected {len(changes)} changes.",
        f"Previous crawl: {os.path.basename(old_folder)}",
        f"Time: {datetime.now(timezone.utc).astimezone().isoformat()}",
        ""
    ]
    for change in changes:
        summary_plain_lines.append(f"- {change['type'].upper()}: {change['filename']}")

    summary_path = os.path.join(new_folder, "change_summary.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("\n".join(summary_plain_lines))
    log(f"Saved local change summary: {summary_path}")

    log("Task complete.")

if __name__ == "__main__":
    main()
