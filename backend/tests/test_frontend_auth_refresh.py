from pathlib import Path
import subprocess
import unittest


REPO_ROOT = Path(__file__).resolve().parents[2]

HOME_PAGE = REPO_ROOT / "frontend" / "src" / "app" / "page.tsx"
MY_CLIPS_PAGE = REPO_ROOT / "frontend" / "src" / "app" / "my-clips" / "page.tsx"
AUTH_MODULE = REPO_ROOT / "frontend" / "src" / "lib" / "auth.mjs"
FRONTEND_TEST = REPO_ROOT / "frontend" / "tests" / "auth.test.mjs"


class FrontendAuthRefreshRegressionTest(unittest.TestCase):

    def read_source(self, path):
        self.assertTrue(path.is_file(), f"Missing file: {path}")
        return path.read_text(encoding="utf-8")

    def test_homepage_integrates_refresh_tokens(self):
        source = self.read_source(HOME_PAGE)

        self.assertIn(
            "storeTokens(data.access_token, data.refresh_token)",
            source,
        )
        self.assertIn("authenticatedFetch(", source)

    def test_my_clips_uses_shared_authentication(self):
        source = self.read_source(MY_CLIPS_PAGE)

        self.assertIn("authenticatedFetch(", source)
        self.assertIn("getRefreshToken()", source)

    def test_auth_module_implements_token_rotation(self):
        source = self.read_source(AUTH_MODULE)

        self.assertIn("clipwave_refresh_token", source)
        self.assertIn("${apiBaseUrl}/auth/refresh", source)
        self.assertIn(
            "storeTokens(data.access_token, data.refresh_token, storage)",
            source,
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