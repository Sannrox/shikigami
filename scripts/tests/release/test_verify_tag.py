"""verify-tag.sh against real lightweight, unsigned, and signed tags in a
scratch repository whose trusted branch carries the signer allowlist."""

import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest

from scripts.tests.release import RELEASE

SCRIPT = RELEASE / "verify-tag.sh"
UNKNOWN_FPR = "AEDA618886BF0331105B9CFCE4DC1F4A579505FB"


def cargo_toml(version: str) -> str:
    # A [workspace.dependencies] version must not be read as the release version.
    return f'[workspace]\nmembers = []\n\n[workspace.package]\nversion = "{version}"\n\n[workspace.dependencies]\nversion = "9.9.9"\n'


@unittest.skipUnless(shutil.which("gpg"), "gpg is required")
class VerifyTag(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Short base path: gpg-agent socket paths have a length limit.
        cls.tmp = pathlib.Path(tempfile.mkdtemp(prefix=f"vt{os.getpid()}-", dir="/tmp"))
        gnupg = cls.tmp / "g"
        gnupg.mkdir(mode=0o700)
        cls.env = {**os.environ, "GNUPGHOME": str(gnupg), "RELEASE_TRUST_REF": "main",
                   "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}
        cls.addClassCleanup(shutil.rmtree, cls.tmp, ignore_errors=True)
        cls.addClassCleanup(subprocess.run, ["gpgconf", "--kill", "gpg-agent"], env=cls.env, capture_output=True)
        subprocess.run(
            ["gpg", "--batch", "--pinentry-mode", "loopback", "--passphrase", "",
             "--quick-generate-key", "Release test <release-test@example.invalid>", "ed25519", "default", "never"],
            env=cls.env, check=True, capture_output=True,
        )
        listing = subprocess.run(["gpg", "--batch", "--list-secret-keys", "--with-colons"],
                                 env=cls.env, check=True, capture_output=True, text=True).stdout
        cls.fpr = next(line.split(":")[9] for line in listing.splitlines() if line.startswith("fpr:"))
        cls.work = cls.tmp / "w"
        subprocess.run(["git", "init", "-q", "-b", "main", str(cls.work)], env=cls.env, check=True)
        for key, value in {
            "user.name": "Release test", "user.email": "release-test@example.invalid",
            "commit.gpgsign": "false", "tag.gpgSign": "false",
            "user.signingkey": cls.fpr, "gpg.program": shutil.which("gpg"),
        }.items():
            cls.git("config", key, value)
        (cls.work / "security").mkdir()
        key = subprocess.run(["gpg", "--batch", "--armor", "--export", cls.fpr], env=cls.env, check=True,
                             capture_output=True).stdout
        (cls.work / "security" / "release-tag-key.asc").write_bytes(key)
        (cls.work / "security" / "release-tag-fingerprints").write_text(f"# test\n{cls.fpr}\n")
        (cls.work / "Cargo.toml").write_text(cargo_toml("0.1.0"))
        cls.git("add", ".")
        cls.git("commit", "-q", "-m", "first")
        cls.first = cls.git("rev-parse", "HEAD")
        cls.git("tag", "v0.0.7", cls.first)
        cls.git("tag", "-a", "v0.0.9", "-m", "unsigned", cls.first)
        cls.git("tag", "-s", "v0.1.0", "-m", "signed", cls.first)
        cls.git("tag", "-s", "v0.2.0", "-m", "wrong version", cls.first)
        (cls.work / "Cargo.toml").write_text(cargo_toml("0.3.0-rc.1"))
        cls.git("commit", "-q", "-am", "rc")
        cls.rc = cls.git("rev-parse", "HEAD")
        cls.git("tag", "-s", "v0.3.0-rc.1", "-m", "rc", cls.rc)
        cls.git("tag", "-s", "v0.3.0", "-m", "final on rc", cls.rc)
        # A side branch that brings no key of its own still is not trusted.
        cls.git("checkout", "-q", "-b", "side")
        cls.git("commit", "-q", "--allow-empty", "-m", "side")
        cls.git("tag", "-s", "v0.4.0-rc.1", "-m", "side")
        cls.git("checkout", "-q", "main")
        cls.main = cls.git("rev-parse", "HEAD")

    @classmethod
    def git(cls, *args: str) -> str:
        return subprocess.run(["git", "-C", str(cls.work), *args], env=cls.env, check=True,
                              capture_output=True, text=True).stdout.strip()

    def tearDown(self):
        self.git("reset", "-q", "--hard", self.main)

    def verify(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(["bash", str(SCRIPT), "verify", *args], cwd=self.work, env=self.env,
                              capture_output=True, text=True)

    def assertRefused(self, message: str, *args: str):
        result = self.verify(*args)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(message, result.stderr)

    def test_accepts_a_signed_tag_matching_the_workspace_version(self):
        for tag, sha in (("v0.1.0", self.first), ("v0.3.0-rc.1", self.rc)):
            with self.subTest(tag=tag):
                result = self.verify(tag, sha)
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_refuses_lightweight_unsigned_and_malformed_tags(self):
        self.assertRefused("is lightweight", "v0.0.7", self.first)
        self.assertRefused("unsigned", "v0.0.9", self.first)
        self.assertRefused("invalid release tag", "v0.1", self.first)
        self.assertRefused("invalid release tag", "v0.3.0-rc", self.rc)

    def test_refuses_a_tag_for_another_commit(self):
        self.assertRefused(f"resolves to {self.first}, expected {'0' * 40}", "v0.1.0", "0" * 40)

    def test_refuses_a_version_mismatch_and_a_final_tag_on_an_rc_version(self):
        self.assertRefused("does not match workspace version 0.1.0", "v0.2.0", self.first)
        self.assertRefused("does not match workspace version 0.3.0-rc.1", "v0.3.0", self.rc)

    def test_refuses_a_tag_off_the_trusted_branch(self):
        self.assertRefused("is not on main", "v0.4.0-rc.1")

    def test_the_allowlist_on_the_trusted_ref_decides(self):
        (self.work / "security" / "release-tag-fingerprints").write_text(UNKNOWN_FPR + "\n")
        self.git("commit", "-q", "-am", "drop the test key")
        self.assertRefused("unknown key", "v0.1.0", self.first)

    def test_revoking_the_key_on_the_trusted_ref_rejects_earlier_tags(self):
        # Revoke in a copy of the keyring so the other tests keep a valid key.
        gnupg = self.tmp / "r"
        shutil.copytree(self.env["GNUPGHOME"], gnupg, ignore=shutil.ignore_patterns("S.*"))
        env = {**self.env, "GNUPGHOME": str(gnupg)}
        self.addCleanup(subprocess.run, ["gpgconf", "--kill", "gpg-agent"], env=env, capture_output=True)
        # gpg prefixes the stored certificate's armor with ':' against accidental use.
        cert = (gnupg / "openpgp-revocs.d" / f"{self.fpr}.rev").read_text().replace(":-----BEGIN", "-----BEGIN")
        subprocess.run(["gpg", "--batch", "--import"], input=cert.encode(), env=env, check=True, capture_output=True)
        key = subprocess.run(["gpg", "--batch", "--armor", "--export", self.fpr], env=env, check=True,
                             capture_output=True).stdout
        (self.work / "security" / "release-tag-key.asc").write_bytes(key)
        self.git("commit", "-q", "-am", "revoke the test key")
        result = subprocess.run(["bash", str(SCRIPT), "verify", "v0.1.0"], cwd=self.work, env=env,
                                capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("expired or revoked", result.stderr)

if __name__ == "__main__":
    unittest.main()
