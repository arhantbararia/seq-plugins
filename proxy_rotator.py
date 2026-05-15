import time
import os
import requests
import logging

# Configure logging to see rotation in logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)

API_URL = "https://proxy.webshare.io/api/v2/proxy/list/download/adhyxwulthlexlxjvpqyzdfityaqywesbddvyqfx/-/any/username/direct/-/?plan_id=13345999"

def fetch_proxies():
    """Downloads the list of proxies from Webshare API."""
    try:
        logging.info("Fetching fresh proxy list from Webshare...")
        response = requests.get(API_URL, timeout=15)
        response.raise_for_status()
        # API returns list in IP:PORT:USER:PASS format
        proxies = response.text.strip().split('\n')
        valid_proxies = [p.strip() for p in proxies if p.strip()]
        logging.info(f"Successfully retrieved {len(valid_proxies)} proxies.")
        return valid_proxies
    except Exception as e:
        logging.error(f"Error fetching proxy list: {e}")
        return []

def format_proxy_url(proxy_line):
    """Converts IP:PORT:USER:PASS into http://user:pass@ip:port"""
    parts = proxy_line.split(':')
    if len(parts) == 4:
        ip, port, user, password = parts
        return f"http://{user}:{password}@{ip}:{port}"
    logging.warning(f"Skipping malformed proxy entry: {proxy_line}")
    return None

def rotate_proxies():
    """Endless loop that rotates proxies every 10 minutes."""
    while True:
        proxies = fetch_proxies()
        
        if not proxies:
            logging.warning("Proxy list empty, retrying in 60 seconds...")
            time.sleep(60)
            continue
            
        for proxy_line in proxies:
            proxy_url = format_proxy_url(proxy_line)
            if not proxy_url:
                continue
                
            logging.info(f"Rotating proxy -> {proxy_url}")
            
            # Set environment variables for the current process and its future children
            os.environ['HTTP_PROXY'] = proxy_url
            os.environ['HTTPS_PROXY'] = proxy_url
            
            # Since a child process cannot modify the parent shell's environment,
            # we write to a shared file that other processes can source if needed.
            try:
                with open("/tmp/proxy.env", "w") as f:
                    f.write(f"export HTTP_PROXY=\"{proxy_url}\"\n")
                    f.write(f"export HTTPS_PROXY=\"{proxy_url}\"\n")
                # Also try updating the global system environment if we have permissions
                # (Commonly used in certain container environments)
                with open("/etc/environment", "a") as f:
                     # This is just a helper, actual system-wide update is OS dependent
                     pass
            except Exception as e:
                logging.debug(f"Could not write to shared env file: {e}")

            # Wait for 10 minutes before moving to the next proxy in the current list
            time.sleep(600)
            
        logging.info("Used up current proxy list. Re-fetching...")

if __name__ == "__main__":
    # Ensure requests is installed if running in a fresh env
    try:
        import requests
    except ImportError:
        logging.error("The 'requests' library is missing. Please run: pip install requests")
        exit(1)
        
    rotate_proxies()
