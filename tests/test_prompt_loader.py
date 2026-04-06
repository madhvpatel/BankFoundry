import tempfile
import unittest
from pathlib import Path

from app.intelligence.prompt_loader import load_prompt_section


class PromptLoaderTest(unittest.TestCase):
    def test_loads_section_from_agents_md(self):
        with tempfile.TemporaryDirectory() as td:
            md = Path(td) / "AGENTS.md"
            md.write_text(
                "# Test\n\n"
                "## alpha\n"
                "Prompt A line 1\n"
                "Prompt A line 2\n\n"
                "## beta\n"
                "Prompt B\n",
                encoding="utf-8",
            )
            text = load_prompt_section(md, "alpha", "fallback")
            self.assertEqual(text, "Prompt A line 1\nPrompt A line 2")

    def test_fallback_for_missing_section_or_file(self):
        with tempfile.TemporaryDirectory() as td:
            md = Path(td) / "AGENTS.md"
            md.write_text("# Empty\n", encoding="utf-8")
            self.assertEqual(load_prompt_section(md, "unknown", "fallback"), "fallback")
            self.assertEqual(load_prompt_section(Path(td) / "nope.md", "alpha", "fallback"), "fallback")


if __name__ == "__main__":
    unittest.main()
