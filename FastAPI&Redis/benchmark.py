import requests
import time


def test_speed(endpoint_name, url):
    print(f"--- Testing: {endpoint_name} ---")

    # 1. First request (Hits the DB, saves to Redis)
    start_time = time.time()
    response1 = requests.get(url)
    db_time = (time.time() - start_time) * 1000  # Convert seconds to milliseconds

    # Short pause
    time.sleep(0.1)

    # 2. Second request (Should return instantly from Redis)
    start_time = time.time()
    response2 = requests.get(url)
    cache_time = (time.time() - start_time) * 1000

    # If the DB responds as fast as the cache (or faster),

    if cache_time > 0:
        speedup = db_time / cache_time
    else:
        speedup = 0

    print(f"Response status: {response1.status_code}")
    print(f"Without Redis (DB): {db_time:.2f} ms")
    print(f"With Redis (Cache): {cache_time:.2f} ms")
    print(f"Speedup:            {speedup:.1f}x faster!\n")


# Base URL for local API
base_url = "http://localhost:8000"

# test speed
test_speed("Campaign Performance", f"{base_url}/campaign/665/performance")
test_speed("Advertiser Spending", f"{base_url}/advertiser/66/spending")
test_speed("User Engagements", f"{base_url}/user/589028/engagements")