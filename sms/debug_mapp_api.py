"""
Debug script to test MAPP API endpoints.
Tests various endpoints to find the correct one for SMS messages.
"""

import sys
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sms.config import MAPP_USERNAME, MAPP_PASSWORD

# Test parameters
MESSAGE_ID = 4201469668
ACTIVITY_ID = "C012420.001"
BASE_URL = "https://newsletterit.esprinet.com/api/rest/"
AUTH = HTTPBasicAuth(MAPP_USERNAME, MAPP_PASSWORD)

print("=" * 70)
print("MAPP API Debug - Testing Endpoints")
print("=" * 70)
print(f"MESSAGE_ID: {MESSAGE_ID}")
print(f"ACTIVITY_ID: {ACTIVITY_ID}")
print(f"BASE_URL: {BASE_URL}")
print(f"USERNAME: {MAPP_USERNAME}")
print("=" * 70)

# Test endpoints to try
endpoints_to_test = [
    # Statistics endpoints
    ("GET", "message/getStatistics", {"messageId": MESSAGE_ID}),
    ("GET", "statistics/message", {"messageId": MESSAGE_ID}),
    ("GET", f"message/{MESSAGE_ID}/statistics", {}),

    # Content endpoints
    ("GET", "preparedmessage/get", {"messageId": MESSAGE_ID, "contactId": 1}),
    ("GET", "message/get", {"messageId": MESSAGE_ID}),
    ("GET", f"message/{MESSAGE_ID}", {}),

    # List/search endpoints
    ("GET", "message/list", {}),
    ("GET", "messages", {}),
]

print("\nTesting endpoints...\n")

for method, endpoint, params in endpoints_to_test:
    url = BASE_URL + endpoint
    print(f"\n[{method}] {url}")
    if params:
        print(f"    Params: {params}")

    try:
        response = requests.request(
            method=method,
            url=url,
            params=params,
            auth=AUTH,
            headers={"Accept": "application/json"},
            timeout=10,
        )

        print(f"    Status: {response.status_code}")

        if response.status_code == 200:
            print(f"    ✅ SUCCESS!")
            print(f"    Response preview: {response.text[:200]}...")
        elif response.status_code == 404:
            print(f"    ❌ Not Found")
            print(f"    Response: {response.text[:150]}")
        elif response.status_code == 401:
            print(f"    ❌ Unauthorized (check credentials)")
        elif response.status_code == 400:
            print(f"    ❌ Bad Request")
            print(f"    Response: {response.text[:150]}")
        else:
            print(f"    ⚠ Unexpected status")
            print(f"    Response: {response.text[:150]}")

    except Exception as e:
        print(f"    ❌ Error: {str(e)}")

print("\n" + "=" * 70)
print("Testing complete!")
print("=" * 70)
