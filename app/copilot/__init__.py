"""Merchant-facing Copilot runtime (MD-directed + typed tools).

This package is intentionally small and boring:
- Typed tools enforce tenant scoping and stable outputs.
- The LLM plans tool calls and writes merchant-friendly narratives.
- All tool calls are traced for transparency/debugging.
"""
