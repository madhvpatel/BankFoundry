import unittest
from pathlib import Path

from app.project_paths import repo_path, repo_root, resolve_repo_relative


class ProjectPathsTest(unittest.TestCase):
    def test_repo_root_and_repo_relative_resolution(self):
        root = repo_root()
        self.assertTrue((root / "app").is_dir())
        self.assertTrue((root / "config.py").is_file())
        self.assertEqual(repo_path("tests", "fixtures"), root / "tests" / "fixtures")
        self.assertEqual(resolve_repo_relative("artifacts/demo.json"), root / "artifacts" / "demo.json")

    def test_active_code_does_not_embed_machine_specific_absolute_paths(self):
        root = repo_root()
        forbidden_markers = ("/Users/", "file://", "New_demo copy")
        candidate_paths = list((root / "app").rglob("*.py"))
        candidate_paths.extend((root / "scripts").rglob("*.sh"))
        candidate_paths.extend(
            [
                root / "run_custom_tests.py",
                root / "run_comprehensive_tests.py",
                root / "test.py",
            ]
        )

        offenders: list[str] = []
        for path in candidate_paths:
            if not path.exists():
                continue
            text = path.read_text(encoding="utf-8")
            if any(marker in text for marker in forbidden_markers):
                offenders.append(str(path.relative_to(root)))

        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
