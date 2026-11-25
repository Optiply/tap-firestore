"""Verify package metadata does not authenticate or inspect local job data."""

import json
import subprocess


def test_about_does_not_require_credentials_or_log_local_json(tmp_path):
    """Keep metadata discovery offline even beside local configuration files."""
    marker = "synthetic-local-config-must-not-be-logged"
    (tmp_path / "config.json").write_text(json.dumps({"apiCredentials": marker}))
    result = subprocess.run(
        ["tap-firestore", "--about", "--format=json"],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        timeout=30,
        check=True,
    )
    assert "hotglue-exceptions-classes" in json.loads(result.stdout)["capabilities"]
    assert marker not in result.stdout + result.stderr
