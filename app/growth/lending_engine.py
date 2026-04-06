"""
lending_engine.py — Merchant Scoring & Eligibility Engine

Calculates a risk and performance-based score for merchants to determine
eligibility for Loan Against Credit Receivables (LACR) and Overdraft facilities.
"""
from __future__ import annotations

import logging
from typing import Any
from datetime import datetime
from sqlalchemy import text

logger = logging.getLogger("growth.lending_engine")

def calculate_merchant_score(engine: Any, merchant_id: str) -> dict[str, Any]:
    """Calculate a 0-100 score based on 30-day performance, holds, and chargebacks."""
    score = 100
    factors = []

    try:
        with engine.connect() as conn:
            # 1. Fetch 30-day Volume and HELD counts from settlements
            settlements_query = text('''
                SELECT 
                    SUM(COALESCE(gross_amount, 0)) as vol_30d,
                    COUNT(CASE WHEN settlement_status = 'HELD' THEN 1 END) as holds_30d
                FROM settlements
                WHERE mid = :mid
                AND settlement_date >= current_date - interval '30 days'
            ''')
            settlements_res = conn.execute(settlements_query, {"mid": merchant_id}).mappings().first()
            
            vol_30d = float(settlements_res.get("vol_30d") or 0.0) if settlements_res else 0.0
            holds_30d = int(settlements_res.get("holds_30d") or 0) if settlements_res else 0
            
            # 2. Fetch Chargeback counts in 30 days
            cb_query = text('''
                SELECT COUNT(*) as cb_count 
                FROM chargebacks 
                WHERE mid = :mid
                AND filed_date >= current_date - interval '30 days'
            ''')
            cb_res = conn.execute(cb_query, {"mid": merchant_id}).mappings().first()
            cb_count = int(cb_res.get("cb_count") or 0) if cb_res else 0

            # 3. Fetch Merchant Risk Profile
            merch_query = text('''
                SELECT merchant_risk_category 
                FROM merchants 
                WHERE mid = :mid
            ''')
            merch_res = conn.execute(merch_query, {"mid": merchant_id}).mappings().first()
            risk_cat = str(merch_res.get("merchant_risk_category") or "LOW").upper() if merch_res else "LOW"

            # --- Scoring Logic ---
            
            # Base deduction based on explicit risk category
            if risk_cat == "HIGH":
                score -= 30
                factors.append("High risk merchant category (-30)")
            elif risk_cat == "MEDIUM":
                score -= 10
                factors.append("Medium risk merchant category (-10)")
                
            # Volume based bonus / caps
            if vol_30d < 500000:
                score -= 15
                factors.append("Very low recent transaction volume (-15)")
            elif vol_30d < 2000000:
                score -= 5
                factors.append("Moderate recent transaction volume (-5)")
            else:
                factors.append("Strong recent transaction volume (+0)")
                
            # Deduct for holds (severe operational risk)
            if holds_30d > 0:
                deduction = holds_30d * 20
                score -= deduction
                factors.append(f"Recent held settlements ({holds_30d}) (-{deduction})")
                
            # Deduct for chargebacks
            if cb_count > 0:
                deduction = cb_count * 15
                score -= deduction
                factors.append(f"Recent chargebacks ({cb_count}) (-{deduction})")
                
            # Enforce bounds
            score = max(0, min(100, score))
            
            if score == 100:
                factors.append("No active deductions (Perfect record)")

            return {
                "score": score,
                "factors": factors,
                "vol_30d": vol_30d
            }
            
    except Exception as exc:
        logger.error(f"Error calculating merchant score: {exc}")
        return {"score": 0, "factors": [f"System error calculating score: {str(exc)}"], "vol_30d": 0.0}

def get_lending_offers(engine: Any, merchant_id: str) -> dict[str, Any]:
    """Determines LACR and Overdraft offers based on the Merchant Score."""
    score_data = calculate_merchant_score(engine, merchant_id)
    score = score_data.get("score", 0)
    vol_30d = score_data.get("vol_30d", 0.0)
    
    offers = []
    eligibility_tier = "Tier 3 (Ineligible)"
    
    if score >= 80:
        eligibility_tier = "Tier 1 (Prime)"
        max_lacr = vol_30d * 2.0
        if max_lacr > 0:
            offers.append({
                "product": "Loan Against Credit Receivables (LACR)",
                "limit_inr": max_lacr,
                "apr_percent": 12.0,
                "term_months": 12,
                "status": "PRE_APPROVED"
            })
            offers.append({
                "product": "Overdraft",
                "limit_inr": min(500000.0, vol_30d * 0.5),
                "apr_percent": 14.0,
                "status": "PRE_APPROVED"
            })
    elif score >= 60:
        eligibility_tier = "Tier 2 (Standard)"
        max_lacr = vol_30d * 1.0
        if max_lacr > 0:
            offers.append({
                "product": "Loan Against Credit Receivables (LACR)",
                "limit_inr": max_lacr,
                "apr_percent": 16.0,
                "term_months": 6,
                "status": "ELIGIBLE_FOR_REVIEW"
            })
            offers.append({
                "product": "Overdraft",
                "limit_inr": min(100000.0, vol_30d * 0.25),
                "apr_percent": 18.0,
                "status": "ELIGIBLE_FOR_REVIEW"
            })
        
    return {
        "merchant_id": merchant_id,
        "score": score,
        "eligibility_tier": eligibility_tier,
        "factors": score_data.get("factors", []),
        "recent_30d_volume_inr": vol_30d,
        "offers": offers,
        "evidence": [f"lending_assessment:{merchant_id}"],
        "timestamp": datetime.now().isoformat()
    }
