import os
from typing import Optional, Tuple

import pytest
from playwright.sync_api import Page, expect, sync_playwright
from sqlalchemy import create_engine, text


def _database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        # Matches main.py docs.
        url = "postgresql://demo:demo@localhost:5433/payments_demo"
    return url


def _sample_ids(merchant_id: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Return (tx_id, settlement_id, chargeback_id) for the merchant if present."""
    eng = create_engine(_database_url())
    with eng.connect() as conn:
        tx_id = conn.execute(
            text("SELECT tx_id FROM transaction_fact WHERE merchant_id = :mid LIMIT 1"),
            {"mid": merchant_id},
        ).scalar()
        settlement_id = conn.execute(
            text("SELECT settlement_id FROM settlements WHERE merchant_id = :mid LIMIT 1"),
            {"mid": merchant_id},
        ).scalar()
        chargeback_id = conn.execute(
            text("SELECT chargeback_id FROM chargebacks WHERE merchant_id = :mid LIMIT 1"),
            {"mid": merchant_id},
        ).scalar()
    return (
        str(tx_id) if tx_id else None,
        str(settlement_id) if settlement_id else None,
        str(chargeback_id) if chargeback_id else None,
    )


def _merchant_id_from_env_or_default() -> Optional[str]:
    mid = os.environ.get("MERCHANT_ID")
    return mid.strip() if mid else None


@pytest.fixture(scope="session")
def browser():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        yield browser
        browser.close()


@pytest.fixture()
def page(browser):
    page = browser.new_page()
    yield page
    page.close()


def _current_merchant_id(page: Page) -> str:
    # Sidebar text input label is "merchant_id" in main.py
    mid_input = page.get_by_label("merchant_id")
    expect(mid_input).to_be_visible(timeout=30_000)
    return mid_input.input_value().strip()


def _ask(page: Page, question: str):
    chat = page.get_by_placeholder("Ask about success rate")
    expect(chat).to_be_visible(timeout=30_000)
    chat.click()
    chat.fill(question)
    chat.press("Enter")


def _expand_latest_tool_trace(page: Page):
    # Streamlit expander toggle text
    exp = page.get_by_text("Tool trace").last
    expect(exp).to_be_visible(timeout=60_000)
    exp.click()


def _expect_tool_used(page: Page, tool_name: str):
    # After expanding, tool name should appear.
    expect(page.get_by_text(tool_name).last).to_be_visible(timeout=60_000)


def _wait_for_answer(page: Page):
    # Tone.md forces a "What I checked" section in responses.
    expect(page.get_by_text("What I checked").last).to_be_visible(timeout=120_000)


@pytest.mark.e2e
def test_compute_kpis_by_payment_mode(streamlit_url: str, page: Page):
    page.goto(streamlit_url, wait_until="domcontentloaded")
    expect(page.get_by_text("AcquiGuru — Merchant Copilot (Demo)")).to_be_visible(timeout=30_000)

    _ask(page, "show success rate by payment mode last 30 days")
    _wait_for_answer(page)
    _expand_latest_tool_trace(page)
    _expect_tool_used(page, "compute_kpis")


@pytest.mark.e2e
def test_end_to_end_analysis(streamlit_url: str, page: Page):
    page.goto(streamlit_url, wait_until="domcontentloaded")
    expect(page.get_by_text("AcquiGuru — Merchant Copilot (Demo)")).to_be_visible(timeout=30_000)

    _ask(page, "run an end to end analysis for my merchant")
    _wait_for_answer(page)
    _expand_latest_tool_trace(page)
    _expect_tool_used(page, "end_to_end_analysis")


@pytest.mark.e2e
def test_terminal_performance(streamlit_url: str, page: Page):
    page.goto(streamlit_url, wait_until="domcontentloaded")
    expect(page.get_by_text("AcquiGuru — Merchant Copilot (Demo)")).to_be_visible(timeout=30_000)

    _ask(page, "which terminals are performing the worst in the last 30 days?")
    _wait_for_answer(page)
    _expand_latest_tool_trace(page)
    _expect_tool_used(page, "terminal_performance")


@pytest.mark.e2e
def test_list_transactions(streamlit_url: str, page: Page):
    page.goto(streamlit_url, wait_until="domcontentloaded")
    expect(page.get_by_text("AcquiGuru — Merchant Copilot (Demo)")).to_be_visible(timeout=30_000)

    _ask(page, "list my last 20 transactions")
    _wait_for_answer(page)
    _expand_latest_tool_trace(page)
    _expect_tool_used(page, "list_transactions")


@pytest.mark.e2e
def test_merchant_context_kyc(streamlit_url: str, page: Page):
    page.goto(streamlit_url, wait_until="domcontentloaded")
    expect(page.get_by_text("AcquiGuru — Merchant Copilot (Demo)")).to_be_visible(timeout=30_000)

    _ask(page, "what is my kyc status and risk profile summary?")
    _wait_for_answer(page)
    _expand_latest_tool_trace(page)
    _expect_tool_used(page, "get_merchant_context")


@pytest.mark.e2e
def test_settlements_and_detail(streamlit_url: str, page: Page):
    page.goto(streamlit_url, wait_until="domcontentloaded")
    expect(page.get_by_text("AcquiGuru — Merchant Copilot (Demo)")).to_be_visible(timeout=30_000)

    # First: list settlements
    _ask(page, "list my settlements for the last 30 days")
    _wait_for_answer(page)
    _expand_latest_tool_trace(page)
    _expect_tool_used(page, "list_settlements")


@pytest.mark.e2e
def test_chargebacks_and_detail(streamlit_url: str, page: Page):
    page.goto(streamlit_url, wait_until="domcontentloaded")
    expect(page.get_by_text("AcquiGuru — Merchant Copilot (Demo)")).to_be_visible(timeout=30_000)

    _ask(page, "list my chargebacks in the last 30 days")
    _wait_for_answer(page)
    _expand_latest_tool_trace(page)
    _expect_tool_used(page, "list_chargebacks")


@pytest.mark.e2e
def test_refunds(streamlit_url: str, page: Page):
    page.goto(streamlit_url, wait_until="domcontentloaded")
    expect(page.get_by_text("AcquiGuru — Merchant Copilot (Demo)")).to_be_visible(timeout=30_000)

    _ask(page, "list my refunds in the last 30 days")
    _wait_for_answer(page)
    _expand_latest_tool_trace(page)
    _expect_tool_used(page, "list_refunds")


@pytest.mark.e2e
def test_transaction_detail_by_id(streamlit_url: str, page: Page):
    page.goto(streamlit_url, wait_until="domcontentloaded")
    expect(page.get_by_text("AcquiGuru — Merchant Copilot (Demo)")).to_be_visible(timeout=30_000)

    mid = _current_merchant_id(page)
    tx_id, _, _ = _sample_ids(mid)
    if not tx_id:
        pytest.skip("No tx_id found for this merchant")

    _ask(page, f"show transaction details for tx_id {tx_id}")
    _wait_for_answer(page)
    _expand_latest_tool_trace(page)
    _expect_tool_used(page, "get_transaction_detail")


@pytest.mark.e2e
def test_settlement_detail_by_id(streamlit_url: str, page: Page):
    page.goto(streamlit_url, wait_until="domcontentloaded")
    expect(page.get_by_text("AcquiGuru — Merchant Copilot (Demo)")).to_be_visible(timeout=30_000)

    mid = _current_merchant_id(page)
    _, settlement_id, _ = _sample_ids(mid)
    if not settlement_id:
        pytest.skip("No settlement_id found for this merchant")

    _ask(page, f"show settlement details for settlement_id {settlement_id}")
    _wait_for_answer(page)
    _expand_latest_tool_trace(page)
    _expect_tool_used(page, "get_settlement_detail")


@pytest.mark.e2e
def test_chargeback_detail_by_id(streamlit_url: str, page: Page):
    page.goto(streamlit_url, wait_until="domcontentloaded")
    expect(page.get_by_text("AcquiGuru — Merchant Copilot (Demo)")).to_be_visible(timeout=30_000)

    mid = _current_merchant_id(page)
    _, _, chargeback_id = _sample_ids(mid)
    if not chargeback_id:
        pytest.skip("No chargeback_id found for this merchant")

    _ask(page, f"show chargeback details for chargeback_id {chargeback_id}")
    _wait_for_answer(page)
    _expand_latest_tool_trace(page)
    _expect_tool_used(page, "get_chargeback_detail")


@pytest.mark.e2e
def test_propose_merchant_action(streamlit_url: str, page: Page):
    page.goto(streamlit_url, wait_until="domcontentloaded")
    expect(page.get_by_text("AcquiGuru — Merchant Copilot (Demo)")).to_be_visible(timeout=30_000)

    _ask(page, "propose a merchant action to investigate the top UPI failure reason and create it only after confirmation")
    _wait_for_answer(page)
    _expand_latest_tool_trace(page)
    _expect_tool_used(page, "propose_and_create_merchant_action")


@pytest.mark.e2e
def test_compare_kpis(streamlit_url: str, page: Page):
    page.goto(streamlit_url, wait_until="domcontentloaded")
    expect(page.get_by_text("AcquiGuru — Merchant Copilot (Demo)")).to_be_visible(timeout=30_000)

    _ask(page, "compare success rate by payment mode for last 7 days vs previous 7 days")
    _wait_for_answer(page)
    _expand_latest_tool_trace(page)
    _expect_tool_used(page, "compare_kpis")
