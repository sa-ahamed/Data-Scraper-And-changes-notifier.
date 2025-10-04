import os
import queue
import threading
import time
import random
import argparse
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor

import requests
from bs4 import BeautifulSoup
import html2text


def get_page_name(url):
    parsed = urlparse(url)
    path = parsed.path.strip('/')
    if not path:
        return 'index.md'
    return path.replace('/', '-') + '.md'


def save_content(content, filename, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"Saved {filepath}")


def fetch_page(url, headers=None):
    """Fetch page content with retry and polite delay."""
    for _ in range(3):
        try:
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code == 200 and "text/html" in response.headers.get("Content-Type", ""):
                return response.text
        except Exception as e:
            print(f"Error fetching {url}: {e}")
        time.sleep(random.uniform(1, 3))
    return None


def extract_main_content(html, content_selector, exclude_selectors):
    """Extract main content from HTML, removing noisy elements."""
    soup = BeautifulSoup(html, 'html.parser')

    # Remove unwanted elements
    for selector in exclude_selectors:
        for element in soup.select(selector):
            element.decompose()

    # Find main content
    main_content = soup.select_one(content_selector)
    if not main_content:
        main_content = soup.body or soup
        print(f"Warning: Content selector '{content_selector}' not found, using full body.")

    return str(main_content)


def fetch_and_process(url, base_url, output_dir, visited, lock, frontier,
                      content_selector, exclude_selectors, headers):
    try:
        html = fetch_page(url, headers=headers)
        if not html:
            return

        cleaned_html = extract_main_content(html, content_selector, exclude_selectors)
        md_content = html2text.html2text(cleaned_html)
        save_content(md_content, get_page_name(url), output_dir)

        # Extract links
        soup = BeautifulSoup(html, 'html.parser')
        domain = urlparse(base_url).netloc
        for a in soup.find_all("a", href=True):
            abs_url = urljoin(url, a['href'])
            parsed_abs = urlparse(abs_url)

            # --- SKIP if it's an image or non-HTML file ---
            if any(abs_url.lower().endswith(ext) for ext in [
                ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp",
                ".bmp", ".tiff", ".ico", ".pdf", ".mp4", ".mp3", ".zip", ".rar"
            ]):
                continue

            if parsed_abs.netloc == domain and not parsed_abs.fragment:
                normalized = abs_url.split('#')[0].rstrip('/')
                with lock:
                    if normalized not in visited:
                        visited.add(normalized)
                        frontier.put(normalized)

    except Exception as e:
        print(f"Error processing {url}: {e}")


def crawl_website(base_url, output_dir='scraped_site', max_workers=3,
                  content_selector='main, article, div#content, div.main-content',
                  exclude_selectors='header, footer, nav, aside'):
    base_url = base_url.rstrip('/')
    visited = set([base_url])
    frontier = queue.Queue()
    frontier.put(base_url)
    lock = threading.Lock()

    exclude_selectors = [s.strip() for s in exclude_selectors.split(',')]

    # Fake user-agent header (important for anti-bot detection)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36"
    }

    while not frontier.empty():
        urls = []
        while len(urls) < max_workers and not frontier.empty():
            urls.append(frontier.get())

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(fetch_and_process, url, base_url, output_dir,
                                visited, lock, frontier, content_selector,
                                exclude_selectors, headers)
                for url in urls
            ]
            for future in futures:
                future.result()

        time.sleep(random.uniform(2, 5))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Requests+BS4 web scraper to save cleaned website pages as Markdown files."
    )
    parser.add_argument("url", help="Website URL to scrape (e.g., https://example.com/)")
    parser.add_argument("--threads", type=int, default=3,
                        help="Number of threads for concurrent fetching")
    parser.add_argument("--content-selector", default="main, article, div#content, div.main-content",
                        help="CSS selector(s) for main content (comma-separated)")
    parser.add_argument("--exclude-selectors", default="header, footer, nav, aside",
                        help="CSS selector(s) for elements to exclude (comma-separated)")
    parser.add_argument("--output", default="scraped_site",
                        help="Output directory to save .md files")
    args = parser.parse_args()

    max_workers = max(1, args.threads)
    print(f"Using {max_workers} threads for scraping with Requests. Saving to '{args.output}'")

    crawl_website(
        args.url,
        output_dir=args.output,
        max_workers=max_workers,
        content_selector=args.content_selector,
        exclude_selectors=args.exclude_selectors
    )
