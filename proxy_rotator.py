import time
import os
import requests
import logging
import signal
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)

API_URL = "https://proxy.webshare.io/api/v2/proxy/list/download/adhyxwulthlexlxjvpqyzdfityaqywesbddvyqfx/-/any/username/direct/-/?plan_id=13345999"
CONFIG_TEMPLATE = "/app/tinyproxy.conf.template"
CONFIG_FILE = "/app/tinyproxy.conf"
PID_FILE = "/app/tinyproxy.pid"

def fetch_proxies():
    """Downloads the list of proxies from Webshare API."""
    for attempt in range(3):
        try:
            logging.info(f"Fetching fresh proxy list from Webshare (Attempt {attempt+1}/3)...")
            response = requests.get(API_URL, timeout=30)
            response.raise_for_status()
            proxies = response.text.strip().split('\n')
            valid_proxies = [p.strip() for p in proxies if p.strip()]
            logging.info(f"Successfully retrieved {len(valid_proxies)} proxies.")
            return valid_proxies
        except Exception as e:
            logging.error(f"Error fetching proxy list: {e}")
            if attempt < 2:
                time.sleep(5)
    return []

def update_tinyproxy_upstream(proxy_line):
    """Updates tinyproxy.conf with the new upstream proxy and reloads tinyproxy."""
    parts = proxy_line.split(':')
    if len(parts) != 4:
        logging.warning(f"Malformed proxy entry: {proxy_line}")
        return False
        
    ip, port, user, password = parts
    upstream_line = f"Upstream http {user}:{password}@{ip}:{port}"
    
    try:
        # Load template
        with open(CONFIG_TEMPLATE, "r") as f:
            template = f.read()
            
        # Write new config with upstream
        with open(CONFIG_FILE, "w") as f:
            f.write(template)
            f.write(f"\n# Dynamic Upstream\n{upstream_line}\n")
            
        logging.info(f"Updated tinyproxy config with upstream {ip}:{port}")
        
        # Signal tinyproxy to reload
        if os.path.exists(PID_FILE):
            with open(PID_FILE, "r") as f:
                pid = int(f.read().strip())
            os.kill(pid, signal.SIGHUP)
            logging.info("Sent SIGHUP to tinyproxy for config reload.")
        else:
            logging.warning("Tinyproxy PID file not found. Reload skipped.")
            
        return True
    except Exception as e:
        logging.error(f"Failed to update tinyproxy config: {e}")
        return False

def setup_tinyproxy_without_upstream():
    """Writes the base template without an upstream proxy."""
    try:
        with open(CONFIG_TEMPLATE, "r") as f:
            template = f.read()
        with open(CONFIG_FILE, "w") as f:
            f.write(template)
        logging.info("Set up tinyproxy config without upstream proxy.")
    except Exception as e:
        logging.error(f"Failed to set up initial tinyproxy config: {e}")

def main():
    # If run with --init, just do one update and exit
    is_init = "--init" in sys.argv
    
    while True:
        proxies = fetch_proxies()
        
        if not proxies:
            if is_init:
                logging.warning("Failed to fetch initial proxies. Falling back to direct connection.")
                setup_tinyproxy_without_upstream()
                sys.exit(0)
            logging.warning("Proxy list empty, retrying in 60 seconds...")
            time.sleep(60)
            continue
            
        if is_init:
            # Just do the first one and exit
            update_tinyproxy_upstream(proxies[0])
            logging.info("Initial proxy setup complete.")
            sys.exit(0)
            
        for proxy_line in proxies:
            if update_tinyproxy_upstream(proxy_line):
                # Wait for 10 minutes before rotating to the next proxy
                time.sleep(600)
            else:
                time.sleep(10) # Quick retry on failure
                
        logging.info("Used up current proxy list. Re-fetching...")

if __name__ == "__main__":
    main()
