import urllib.request
import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUT_PATH = PROJECT_ROOT / "artifacts" / "sprint1_manual_validation" / "custom_runs.json"

tests = [
  {"id": "CUSTOM_LOW", "merchant_id": "100000000121215", "lane": "growth", "prompt": "Why are so many payments failing when customers scan the QR code?"},
  {"id": "CUSTOM_MED", "merchant_id": "100000000121215", "lane": "operations", "prompt": "Can you explain why some of my recent settlements have deductions?"},
  {"id": "CUSTOM_HIGH", "merchant_id": "100000000121215", "lane": "operations", "prompt": "Show me the chargeback dispute status and their impact on net settlement over the last window."}
]

results = []

for t in tests:
    req = urllib.request.Request("http://127.0.0.1:8765/test/ask", data=json.dumps(t).encode("utf-8"), headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as response:
            res = json.loads(response.read().decode())
            results.append({
                "case_id": t["id"],
                "response": res
            })
    except Exception as e:
        print(f"Error on {t['id']}: {e}")

OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
with OUTPUT_PATH.open("w", encoding="utf-8") as f:
    json.dump(results, f, indent=2)

print(f"Custom tests completed and saved to {OUTPUT_PATH}")
