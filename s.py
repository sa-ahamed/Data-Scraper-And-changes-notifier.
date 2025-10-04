import requests
from bs4 import BeautifulSoup
import html2text
from urllib.parse import urljoin, urlparse
import os
import queue
import threading
from concurrent.futures import ThreadPoolExecutor
import argparse

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

def fetch_and_process(url, base_url, output_dir, visited, lock, frontier):
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        html = resp.text
        soup = BeautifulSoup(html, 'html.parser')
        
        # Convert HTML to Markdown
        md_content = html2text.html2text(html)
        
        page_name = get_page_name(url)
        save_content(md_content, page_name, output_dir)
        
        # Find and enqueue new links
        domain = urlparse(base_url).netloc
        for a in soup.find_all('a', href=True):
            href = a['href']
            abs_url = urljoin(url, href)
            parsed_abs = urlparse(abs_url)
            if parsed_abs.netloc == domain and not parsed_abs.fragment:
                normalized = abs_url.split('#')[0].rstrip('/')
                with lock:
                    if normalized not in visited:
                        visited.add(normalized)
                        frontier.put(normalized)
    except Exception as e:
        print(f"Error processing {url}: {e}")

def crawl_website(base_url, output_dir='scraped_site', max_workers=10):
    """
    Crawls the website starting from base_url, saves pages as .md files.
    
    Args:
    - base_url: The starting URL (e.g., 'https://example.com/')
    - output_dir: Directory to save .md files
    - max_workers: Number of threads for concurrent fetching (matches user-specified threads)
    """
    base_url = base_url.rstrip('/')
    visited = set([base_url])
    frontier = queue.Queue()
    frontier.put(base_url)
    lock = threading.Lock()
    
    while not frontier.empty():
        # Get all available URLs up to max_workers
        urls = []
        while len(urls) < max_workers and not frontier.empty():
            urls.append(frontier.get())
        
        # Process URLs in parallel with specified thread count
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(fetch_and_process, url, base_url, output_dir, visited, lock, frontier) for url in urls]
            # Wait for all tasks in this batch to complete
            for future in futures:
                future.result()

if __name__ == "__main__":
    # Set up argument parser for command-line input
    parser = argparse.ArgumentParser(description="Web scraper to convert website pages to Markdown files.")
    parser.add_argument("url", help="Website URL to scrape (e.g., https://example.com/)")
    parser.add_argument("--threads", type=int, default=os.cpu_count(), help="Number of threads for concurrent fetching (e.g., 12 for 12 threads)")
    args = parser.parse_args()
    
    # Ensure threads is at least 1
    max_workers = max(1, args.threads)
    print(f"Using {max_workers} threads for scraping.")
    
    # Run the crawler with user-specified URL and thread count
    crawl_website(args.url, output_dir="scraped_site", max_workers=max_workers)