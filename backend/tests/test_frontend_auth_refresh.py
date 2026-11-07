import shutil
import subprocess
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
FRONTEND_TEST = REPO_ROOT / "frontend" / "tests" / "auth.test.mjs"


class FrontendAuthRefreshRegressionTest(unittest.TestCase):

    @unittest.skipUnless(
        shutil.which("node"),
        "Node.js is required for frontend authentication tests",
    )
    def test_javascript_authentication_regression_suite(self):
        self.assertTrue(FRONTEND_TEST.is_file())

        result = subprocess.run(
            ["node", "--test", str(FRONTEND_TEST)],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(
            result.returncode,
            0,
            msg=(
                f"JavaScript authentication tests failed:\n"
                f"{result.stdout}\n{result.stderr}"
            ),
        )


if __name__ == "__main__":
    unittest.main()