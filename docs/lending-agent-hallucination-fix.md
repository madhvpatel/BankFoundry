# Fixing Agent Hallucinations on Lending Queries

## What was broken
When a user asked "What funding options do I have?", the Copilot initially halluciated generic loan values (e.g., "revolving credit line of ₹50,000" or generic advice about bank transfers). 

This happened because:
1. The orchestrator LLM (Ollama running `qwen3:8b`) was receiving all 20+ tools in its context but refused to select the newly added `get_merchant_lending_offers` tool because the user's intent was not perfectly mimicking the tool's description.
2. The LLM fell back to calling the broad `merchant_profile` tool or ignored tools entirely.
3. Without explicit constraints regarding financial data, the LLM defaulted to inventing generic (plausible-sounding) lending details to fulfill its role as a "helpful" agent.

## What was changed
We moved the deterministic intent routing to run **before** the LLM's tool-selection step.

In `app/agent/service.py`, inside `run_agent_turn`:
```python
# Extract intent early to guide tool selection
routed = route_chat_intent(question=prompt_text, scope=scope, history=conversation_history)
resolved_intent = (routed.get("intent") if routed else "agent_turn") or "agent_turn"

# Force tool selection for critical paths like lending
if resolved_intent == "lending_eligibility":
    tools = [t for t in tools if t.name == "get_merchant_lending_offers"]
```

By identifying the intent first (`lending_eligibility`), we filter the available tools passed to the LLM down to exactly one: `get_merchant_lending_offers`. This effectively "forces" the LLM's hand by removing all unrelated pathways, ensuring the LLM relies entirely on the deterministic scoring engine output and reducing the hallucination search space to zero.

Additionally, we added instructions against hallucinating financial data inside the system prompt (`AGENTS.md`) and improved the tool docstrings (`toolcalling.py`) to reinforce appropriate scope limits for LLMs.

## How to verify the fix
1. Start the agent platform and type `What funding options do I have?` in the chat, or click the respective quick action.
2. The response will now accurately reflect the output of `get_merchant_lending_offers`. For a struggling merchant, it will accurately state:
   - Eligibility tier (e.g., Tier 3 Ineligible)
   - Reason for score (e.g., 2 chargebacks, un-processed volume)
3. You will no longer see invented loan lines in the output or missing tool evidence errors in the validation layer.
