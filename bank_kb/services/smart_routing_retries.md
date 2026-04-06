# Smart Routing & Retries

## What it is
A service that optimizes payment success by routing transactions through the best-performing path and applying safe retries when failures look transient.

## Merchant value
- Higher payment success rate
- Lower failed GMV
- Less customer drop-off at checkout

## What it can optimize (examples)
- UPI PSP routing (where multiple PSP rails exist)
- Timeout/retry strategy for transient network issues

## Guardrails
- Retries are capped to avoid duplicate charges and customer frustration
- Changes are logged and can require approval depending on policy

## When to recommend
- A measurable success-rate dip with clustering in timeout/technical failure buckets
- High failed GMV concentrated in specific hours/terminals
