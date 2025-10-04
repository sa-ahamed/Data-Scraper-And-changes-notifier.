import os
import queue
import threading
import time
import random
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor
import argparse
import html2text
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

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

def create_driver():
    """Create a stealthy Chrome driver to avoid detection."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def extract_main_content(html, content_selector, exclude_selectors):
    """Extract main content from HTML, removing noisy elements."""
    soup = BeautifulSoup(html, 'html.parser')
    
    # Remove unwanted elements (e.g., header, footer, nav)
    for selector in exclude_selectors:
        for element in soup.select(selector):
            element.decompose()
    
    # Find main content
    main_content = soup.select_one(content_selector)
    if not main_content:
        # Fallback: use entire body if selector fails
        main_content = soup.body or soup
        print(f"Warning: Content selector '{content_selector}' not found, using full body.")
    
    return str(main_content)

def fetch_and_process(url, base_url, output_dir, visited, lock, frontier, content_selector, exclude_selectors):
    driver = None
    try:
        driver = create_driver()
        driver.get(url)
        
        # Wait for page to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Simulate human behavior: random scroll
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
        time.sleep(random.uniform(1, 3))
        driver.execute_script("window.scrollTo(0, 0);")
        
        # Get rendered HTML, filter content, and convert to Markdown
        html = driver.page_source
        cleaned_html = extract_main_content(html, content_selector, exclude_selectors)
        md_content = html2text.html2text(cleaned_html)
        save_content(md_content, get_page_name(url), output_dir)
        
        # Find and enqueue new links
        domain = urlparse(base_url).netloc
        links = driver.find_elements(By.TAG_NAME, "a")
        for a in links:
            href = a.get_attribute('href')
            if href:
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
    finally:
        if driver:
            driver.quit()

def crawl_website(base_url, output_dir='scraped_site', max_workers=3, content_selector='main, article, div#content, div.main-content', exclude_selectors=['header', 'footer', 'nav', 'aside']):
    """
    Crawls the website using Selenium, saves cleaned content as .md files.
    
    Args:
    - base_url: The starting URL (e.g., 'https://example.com/')
    - output_dir: Directory to save .md files
    - max_workers: Number of threads for concurrent fetching (keep low, e.g., 3 for stealth)
    - content_selector: CSS selector(s) for main content (comma-separated)
    - exclude_selectors: CSS selector(s) for elements to exclude (comma-separated)
    """
    base_url = base_url.rstrip('/')
    visited = set([base_url])
    frontier = queue.Queue()
    frontier.put(base_url)
    lock = threading.Lock()
    
    # Split comma-separated selectors
    content_selector = content_selector
    exclude_selectors = [s.strip() for s in exclude_selectors.split(',')]
    
    while not frontier.empty():
        # Get URLs up to max_workers
        urls = []
        while len(urls) < max_workers and not frontier.empty():
            urls.append(frontier.get())
        
        # Process in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(fetch_and_process, url, base_url, output_dir, visited, lock, frontier, content_selector, exclude_selectors) for url in urls]
            for future in futures:
                future.result()
        
        # Delay between batches to be polite
        time.sleep(random.uniform(2, 5))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Selenium web scraper to save cleaned website pages as Markdown files (anti-detection).")
    parser.add_argument("url", help="Website URL to scrape (e.g., https://example.com/)")
    parser.add_argument("--threads", type=int, default=3, help="Number of threads for concurrent fetching (keep low, e.g., 3 for stealth)")
    parser.add_argument("--content-selector", default="main, article, div#content, div.main-content", help="CSS selector(s) for main content (comma-separated)")
    parser.add_argument("--exclude-selectors", default="header, footer, nav, aside", help="CSS selector(s) for elements to exclude (comma-separated)")
    args = parser.parse_args()
    
    max_workers = max(1, args.threads)
    print(f"Using {max_workers} threads for scraping with Selenium.")
    
    crawl_website(
        args.url,
        output_dir="scraped_site",
        max_workers=max_workers,
        content_selector=args.content_selector,
        exclude_selectors=args.exclude_selectors
    )