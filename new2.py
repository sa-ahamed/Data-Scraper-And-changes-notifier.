import os
import re
import argparse
import threading
import requests
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import html2text
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

visited = set()
lock = threading.Lock()

def sanitize_filename(name):
    """Sanitize filename to be safe for .md files."""
    name = re.sub(r'[^a-zA-Z0-9_-]', '_', name)
    return name if name else "index"

def save_as_markdown(url, html, output_dir="output"):
    """Convert HTML to Markdown and save as .md file."""
    text_maker = html2text.HTML2Text()
    text_maker.ignore_links = False
    markdown = text_maker.handle(html)

    parsed = urlparse(url)
    filename = sanitize_filename(parsed.path.strip("/")) + ".md"

    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(f"# {url}\n\n")
        f.write(markdown)
    print(f"[+] Saved {filepath}")

def crawl(url, domain, driver, output_dir):
    """Crawl a single URL and save its content."""
    with lock:
        if url in visited:
            return
        visited.add(url)

    try:
        driver.get(url)
        html = driver.page_source
        save_as_markdown(url, html, output_dir)

        soup = BeautifulSoup(html, "html.parser")
        for link in soup.find_all("a", href=True):
            new_url = urljoin(url, link["href"])
            if domain in new_url and new_url not in visited:
                crawl(new_url, domain, driver, output_dir)
    except Exception as e:
        print(f"[-] Error crawling {url}: {e}")

def worker(start_url, domain, output_dir):
    """Thread worker."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)

    crawl(start_url, domain, driver, output_dir)
    driver.quit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Website to Markdown Crawler")
    parser.add_argument("url", help="Starting URL")
    parser.add_argument("--threads", type=int, default=4, help="Number of threads")
    parser.add_argument("--output", default="output", help="Output folder")
    args = parser.parse_args()

    domain = urlparse(args.url).netloc

    threads = []
    for i in range(args.threads):
        t = threading.Thread(target=worker, args=(args.url, domain, args.output))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
