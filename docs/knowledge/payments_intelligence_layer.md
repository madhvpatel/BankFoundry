# Payments Intelligence Layer: Expert Reference

This document serves as the core intelligence layer for analyzing payment failures, chargebacks, and network errors in the Indian and Global payments ecosystem.

## 1. ISO8583 Response Codes (Issuer/Switch Declines)
These are standard codes returned by banking switches (NPCI, Mastercard, Visa, Amex) to indicate the result of an authorization request.

| Code | Meaning | Analysis & Action |
|:---|:---|:---|
| **00** | Approved / Success | Transaction completed successfully. |
| **05** | Do Not Honor | Generic decline. Usually means issuer's security system flagged it. Action: Ask customer to call their bank. |
| **12** | Invalid Transaction | Parameters like merchant ID or transaction type are incorrect for this card. |
| **14** | Invalid Card Number | Typo or inactive card. |
| **51** | Insufficient Funds | Most common customer-side failure. Action: Pivot to UPI or lower amount. |
| **54** | Expired Card | Re-onboard customer with a new card. |
| **57** | Trans. Not Permitted (Card) | Card blocked for online/international use. Action: Enable and retry. |
| **61** | Exceeds Withdrawal Limit | Merchant/Card daily limit hit. |
| **91** | Issuer Inoperative | Issuer bank down. Action: Retry after 15 minutes. |
| **96** | System Malfunction | Gateway or Card Network technical error. |

## 2. UPI Failure Reason Codes (NPCI Standard)
UPI (Unified Payments Interface) has specific failure patterns often related to VPA validation or PIN entry.

| Code | Reason | Merchant Strategy |
|:---|:---|:---|
| **U16** | Risk Threshold Exceeded | Transaction blocked by NPCI/Bank risk engine. Common for high-value new VPAs. |
| **U30** | Debit/Credit Limit Exceeded | Bank-level daily UPI limit reached (typically ₹1L - ₹2L). |
| **U67** | Incorrect PIN | User entered wrong UPI PIN. Action: Auto-retrigger PIN prompt. |
| **U69** | Collect Request Expired | User didn't approve in the App within 5-10 mins. Action: Re-initiate intent. |
| **ZM** | Invalid VPA / Account | VPA does not exist or account is closed. |
| **BT** | Acquirer/NPCI Down | Infrastructure failure. Action: Switch to backup PG if multi-PG setup exists. |

## 3. Chargeback Reason Codes (Visa/Mastercard)
Detailed breakdown for merchant dispute management.

| Card Scheme | Code | Category | Detail |
|:---|:---|:---|:---|
| **Visa** | 10.4 | Fraud | Other Fraud (Card-Absent Environment). |
| **Visa** | 13.1 | Service | Merchandise/Services Not Received. |
| **Mastercard** | 4837 | Fraud | No Cardholder Authorization. |
| **Mastercard** | 4853 | Service | Goods or Services Not Provided. |
| **Amex** | F29 | Fraud | Card Not Present Fraud. |

## 4. Issuer Decline Patterns & Network Errors
- **Multiple "05 - Do Not Honor"**: Usually indicates a specific card BIN (Bank Identification Number) is being targeted by fraud filters or the merchant's MCC (Merchant Category Code) is restricted by that bank.
- **"91 - Issuer Down" during Month-End**: Common during peak salary processing days (1st-5th). 
- **Timeouts (TMO)**: If the latency between Acquirer and Issuer exceeds 30-45 seconds, the network drops the packet. Merchants should check their PG timeout settings.

## 5. Failure Rate Benchmarks (Industry Avg 2026)
- **UPI Intent**: 85% - 92% (Highest)
- **UPI Collect**: 70% - 75% (Drop due to app switching)
- **Card (Saved)**: 80% - 88%
- **Card (New)**: 65% - 72%
