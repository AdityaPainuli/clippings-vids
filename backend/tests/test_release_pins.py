import os
import re
import unittest


BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO_DIR = os.path.dirname(BACKEND_DIR)


def _lines(path):
    with open(path) as f:
        return [l.split("#", 1)[0].strip() for l in f if l.split("#", 1)[0].strip()]


def _name(spec):
    return re.split(r"[<>=!~;\s\[]", spec, 1)[0].lower().replace("_", "-").replace(".", "-")


class ReleasePinsTest(unittest.TestCase):
    """Release builds must resolve to versions the repo controls (#108)."""

    def setUp(self):
        self.constraints = _lines(os.path.join(BACKEND_DIR, "constraints-bolcap.txt"))

    def test_every_constraint_is_an_exact_pin(self):
        for spec in self.constraints:
            self.assertRegex(spec.split(";", 1)[0].strip(), r"^[A-Za-z0-9._-]+==[^=<>!~*,]+$", spec)

    def test_every_direct_dep_and_build_tool_is_constrained(self):
        pinned = {_name(s) for s in self.constraints}
        direct = {_name(s) for s in _lines(os.path.join(BACKEND_DIR, "requirements-bolcap.txt"))}
        for name in direct | {"pyinstaller", "pyinstaller-hooks-contrib"}:
            self.assertIn(name, pinned)

    def test_release_workflow_installs_pinned(self):
        with open(os.path.join(REPO_DIR, ".github", "workflows", "bolcap-release.yml")) as f:
            wf = f.read()
        self.assertRegex(wf, r"pip install pip==[0-9][0-9.]*(\s|$)")
        for line in wf.splitlines():
            if re.search(r"\bpip install\b", line) and "pip==" not in line:
                self.assertIn("-c constraints-bolcap.txt", line)


if __name__ == "__main__":
    unittest.main()
