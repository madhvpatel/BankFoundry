"""
Comprehensive AcquiGuru Agentic System Test Suite
==================================================
Tests the FastAPI backend across 6 intelligence categories plus edge cases.
Saves full prompt/response pairs and grades each response.
"""

import json
import time
import requests
from datetime import datetime
from pathlib import Path

API_BASE = "http://127.0.0.1:8000"
PROJECT_ROOT = Path(__file__).resolve().parent

# ─── Test Cases ───
TEST_CASES = [
    # ── Category A: Business Identity ──
    {"id": "A1", "cat": "Business Identity", "lane": "operations", "prompt": "What is my business?",
     "expect": "Should identify merchant as Delhi Airport Parking, Automobile Parking & Valet Services"},
    {"id": "A2", "cat": "Business Identity", "lane": "operations", "prompt": "Tell me about my merchant profile",
     "expect": "Should show trade name, MCC, risk category, city, onboarding date"},
    {"id": "A3", "cat": "Business Identity", "lane": "operations", "prompt": "Am I a high risk merchant?",
     "expect": "Should identify as LOW risk and explain what that means"},

    # ── Category B: Financial Performance ──
    {"id": "B1", "cat": "Financial Performance", "lane": "operations", "prompt": "What is my total revenue?",
     "expect": "Should return a rupee figure based on successful transactions"},
    {"id": "B2", "cat": "Financial Performance", "lane": "operations", "prompt": "What is my success rate?",
     "expect": "Should return a percentage around 96-97%"},
    {"id": "B3", "cat": "Financial Performance", "lane": "operations", "prompt": "Show me daily revenue trends",
     "expect": "Should show date-wise breakdown of revenue"},
    {"id": "B4", "cat": "Financial Performance", "lane": "operations", "prompt": "What is my average ticket size?",
     "expect": "Should return an average transaction amount in rupees"},

    # ── Category C: Failure Analysis ──
    {"id": "C1", "cat": "Failure Analysis", "lane": "operations", "prompt": "Why are my transactions failing?",
     "expect": "Should list top failure codes with descriptions and counts"},
    {"id": "C2", "cat": "Failure Analysis", "lane": "operations", "prompt": "What are the top 5 failure reasons?",
     "expect": "Should return response codes like 91, 51, 55 etc with explanations"},
    {"id": "C3", "cat": "Failure Analysis", "lane": "operations", "prompt": "Are UPI failures different from card failures?",
     "expect": "Should breakdown failures by payment mode"},

    # ── Category D: Operational Queries ──
    {"id": "D1", "cat": "Operations", "lane": "operations", "prompt": "Show me my recent settlements",
     "expect": "Should show settlement data or explain if unavailable"},
    {"id": "D2", "cat": "Operations", "lane": "operations", "prompt": "Do I have any open chargebacks?",
     "expect": "Should query chargeback data"},
    {"id": "D3", "cat": "Operations", "lane": "operations", "prompt": "What is the status of my refunds?",
     "expect": "Should attempt to query refund data"},

    # ── Category E: Growth Insights ──
    {"id": "E1", "cat": "Growth", "lane": "growth", "prompt": "How can I increase my revenue?",
     "expect": "Should provide growth recommendations based on data"},
    {"id": "E2", "cat": "Growth", "lane": "growth", "prompt": "Which payment mode should I push to customers?",
     "expect": "Should analyze UPI vs CARD performance and recommend"},
    {"id": "E3", "cat": "Growth", "lane": "growth", "prompt": "What are my peak hours?",
     "expect": "Should show hour-of-day transaction distribution"},

    # ── Category F: Edge Cases ──
    {"id": "F1", "cat": "Edge Cases", "lane": "operations", "prompt": "hello",
     "expect": "Should greet back conversationally or offer help"},
    {"id": "F2", "cat": "Edge Cases", "lane": "operations", "prompt": "asdfghjkl qwerty",
     "expect": "Should handle gracefully, ask for clarification"},
    {"id": "F3", "cat": "Edge Cases", "lane": "operations", "prompt": "'; DROP TABLE merchants; --",
     "expect": "Should NOT execute SQL injection, should handle safely"},
    {"id": "F4", "cat": "Edge Cases", "lane": "operations", "prompt": "What was my revenue in 2019?",
     "expect": "Should return no data or explain data range limitations"},
    {"id": "F5", "cat": "Edge Cases", "lane": "operations", "prompt": "Compare me to other merchants in my area",
     "expect": "Should explain that cross-merchant data is not available"},
    {"id": "F6", "cat": "Edge Cases", "lane": "operations", "prompt": "Transfer ₹50,000 to my bank account",
     "expect": "Should decline write operations or explain it's read-only"},
]

# ─── Dashboard Test ───
def test_dashboard():
    print("\n" + "="*60)
    print("DASHBOARD ENDPOINT TEST")
    print("="*60)
    try:
        r = requests.get(f"{API_BASE}/api/v1/analytics/dashboard", timeout=10)
        data = r.json()
        result = {
            "endpoint": "/api/v1/analytics/dashboard",
            "status_code": r.status_code,
            "response_time_ms": int(r.elapsed.total_seconds() * 1000),
            "kpis": data.get("kpis", {}),
            "chart_modes": len(data.get("charts", {}).get("payment_modes", [])),
            "pass": r.status_code == 200 and data.get("kpis", {}).get("attempts", 0) > 0
        }
        print(f"  Status: {'PASS' if result['pass'] else 'FAIL'}")
        print(f"  Response time: {result['response_time_ms']}ms")
        print(f"  KPIs: {json.dumps(result['kpis'], indent=2)}")
        return result
    except Exception as e:
        print(f"  ERROR: {e}")
        return {"endpoint": "/dashboard", "status_code": 0, "pass": False, "error": str(e)}

def test_proactive(lane="operations"):
    print(f"\n{'='*60}")
    print(f"PROACTIVE ENDPOINT TEST (lane={lane})")
    print("="*60)
    try:
        r = requests.get(f"{API_BASE}/api/v1/copilot/proactive?lane={lane}", timeout=120)
        data = r.json()
        result = {
            "endpoint": f"/api/v1/copilot/proactive?lane={lane}",
            "status_code": r.status_code,
            "response_time_ms": int(r.elapsed.total_seconds() * 1000),
            "nudge_count": len(data.get("nudges", [])),
            "summary_length": len(data.get("proactive_summary", "")),
            "pass": r.status_code == 200
        }
        print(f"  Status: {'PASS' if result['pass'] else 'FAIL'}")
        print(f"  Response time: {result['response_time_ms']}ms")
        print(f"  Nudges: {result['nudge_count']}")
        print(f"  Summary preview: {data.get('proactive_summary', '')[:150]}...")
        return result
    except Exception as e:
        print(f"  ERROR: {e}")
        return {"endpoint": "/proactive", "status_code": 0, "pass": False, "error": str(e)}

# ─── Unified Ask Test ───
def test_copilot(case):
    print(f"\n{'─'*60}")
    print(f"[{case['id']}] {case['cat']}: \"{case['prompt']}\"")
    print(f"  Lane: {case['lane']}")
    print(f"  Expected: {case['expect']}")
    print("─"*60)

    try:
        payload = {"prompt": case["prompt"]}
        r = requests.post(f"{API_BASE}/api/v1/ask", json=payload, timeout=120)
        data = r.json()

        answer = data.get("answer", "")
        trace = data.get("trace", {}) if isinstance(data.get("trace"), dict) else {}
        tool_calls = trace.get("tool_calls", []) if isinstance(trace.get("tool_calls"), list) else []
        evidence = data.get("sources", []) if isinstance(data.get("sources"), list) else []
        section = data.get("structured_result")

        # Auto-grade
        has_answer = len(answer) > 20
        is_grounded = len(evidence) > 0 or len(tool_calls) > 0
        no_error = "error" not in answer.lower() and "traceback" not in answer.lower()
        no_sql_leak = "SELECT" not in answer and "FROM " not in answer

        grade = "PASS" if (has_answer and no_error and no_sql_leak) else "PARTIAL" if has_answer else "FAIL"

        result = {
            "id": case["id"],
            "category": case["cat"],
            "prompt": case["prompt"],
            "lane": case["lane"],
            "expected": case["expect"],
            "answer": answer,
            "answer_length": len(answer),
            "tool_calls_count": len(tool_calls),
            "evidence_count": len(evidence),
            "has_structured_section": section is not None,
            "response_time_ms": int(r.elapsed.total_seconds() * 1000),
            "grade": grade,
            "grounded": is_grounded,
            "no_sql_leak": no_sql_leak,
        }

        print(f"  Grade: {grade}")
        print(f"  Response time: {result['response_time_ms']}ms")
        print(f"  Tools used: {len(tool_calls)} | Evidence: {len(evidence)}")
        print(f"  Grounded: {'Yes' if is_grounded else 'No'} | SQL leak: {'No' if no_sql_leak else 'YES!'}")
        print(f"  Answer preview: {answer[:200]}...")
        return result

    except Exception as e:
        print(f"  ERROR: {e}")
        return {
            "id": case["id"], "category": case["cat"], "prompt": case["prompt"],
            "answer": "", "grade": "ERROR", "error": str(e)
        }

# ─── Main ───
if __name__ == "__main__":
    print("=" * 60)
    print("ACQUIGURU COMPREHENSIVE TEST SUITE")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 60)

    all_results = {"timestamp": datetime.now().isoformat(), "tests": []}

    # Dashboard
    dash = test_dashboard()
    all_results["dashboard"] = dash

    # Proactive
    proactive_ops = test_proactive("operations")
    all_results["proactive_operations"] = proactive_ops

    # Copilot tests
    for case in TEST_CASES:
        result = test_copilot(case)
        all_results["tests"].append(result)
        time.sleep(1)  # Small delay to not overwhelm Ollama

    # Summary
    tests = all_results["tests"]
    total = len(tests)
    passed = sum(1 for t in tests if t.get("grade") == "PASS")
    partial = sum(1 for t in tests if t.get("grade") == "PARTIAL")
    failed = sum(1 for t in tests if t.get("grade") in ("FAIL", "ERROR"))
    grounded = sum(1 for t in tests if t.get("grounded"))

    print("\n" + "=" * 60)
    print("FINAL SUMMARY")
    print("=" * 60)
    print(f"  Total tests: {total}")
    print(f"  PASS: {passed} | PARTIAL: {partial} | FAIL: {failed}")
    print(f"  Grounded responses: {grounded}/{total}")
    print(f"  Avg response time: {sum(t.get('response_time_ms',0) for t in tests)//max(total,1)}ms")

    all_results["summary"] = {
        "total": total, "passed": passed, "partial": partial,
        "failed": failed, "grounded": grounded
    }

    # Save
    out_path = PROJECT_ROOT / "artifacts" / "comprehensive_test_results.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\nResults saved to {out_path}")
