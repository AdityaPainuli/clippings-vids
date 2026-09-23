from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]
REQUIREMENTS = ROOT / "backend" / "requirements-bolcap.txt"
RELEASE_WORKFLOW = ROOT / ".github" / "workflows" / "bolcap-release.yml"


class ReleaseDependencyTests(unittest.TestCase):
    EXPECTED_RUNTIME_DEPENDENCIES = {
        "fastapi",
        "uvicorn",
        "python-multipart",
        "pydantic",
        "requests",
        "faster-whisper",
        "indic-transliteration",
    }

    def test_bolcap_runtime_dependencies_preserve_expected_package_set(self):
        requirement_lines = [
            line.strip()
            for line in REQUIREMENTS.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]

        dependency_names = {line.split("==", 1)[0] for line in requirement_lines}
        self.assertEqual(
            dependency_names,
            self.EXPECTED_RUNTIME_DEPENDENCIES,
        )

    def test_bolcap_runtime_dependencies_are_exactly_pinned(self):
        requirement_lines = [
            line.strip()
            for line in REQUIREMENTS.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]

        self.assertEqual(
            len(requirement_lines),
            len(self.EXPECTED_RUNTIME_DEPENDENCIES),
        )
        for line in requirement_lines:
            self.assertRegex(
                line,
                r"^[A-Za-z0-9_.-]+==[^=\s]+$",
                msg=f"Unpinned or non-exact dependency: {line}",
            )

    def test_release_workflow_pins_pip_and_pyinstaller(self):
        workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")

        self.assertRegex(workflow, r"pip install --upgrade pip==[0-9][^\s]*")
        self.assertRegex(
            workflow,
            r"pip install -r requirements-bolcap\.txt pyinstaller==[0-9][^\s]*",
        )
        self.assertNotRegex(workflow, r"pip install --upgrade pip(?:\s|$)")
        self.assertNotRegex(
            workflow,
            r"pip install -r requirements-bolcap\.txt pyinstaller(?:\s|$)",
        )


if __name__ == "__main__":
    unittest.main()
