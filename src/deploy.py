"""
Layer 6 — HuggingFace Auto-Deploy
Called by weekly_retrain.yml after gate check passes.

Requires:
    HF_TOKEN env var (set in GitHub Secrets)
"""
import logging
import os
import sys
from pathlib import Path

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

REPO_ROOT   = Path(__file__).resolve().parent.parent
HF_REPO_ID  = "ShiphrahB/epl-match-predictor"

FILES_TO_DEPLOY = {
    REPO_ROOT / "models" / "model.cbm":          "models/model.cbm",
    REPO_ROOT / "models" / "feature_cols.pkl":   "models/feature_cols.pkl",
    REPO_ROOT / "models" / "metadata.json":      "models/metadata.json",
    REPO_ROOT / "src"    / "api.py":             "src/api.py",
    REPO_ROOT / "src"    / "predict.py":         "src/predict.py",
    REPO_ROOT / "src"    / "config.py":          "src/config.py",
    REPO_ROOT / "requirements.txt":              "requirements.txt",
    REPO_ROOT / "Dockerfile":                    "Dockerfile",
    REPO_ROOT / "README.md":                     "README.md",
}


def deploy():
    token = os.environ.get("HF_TOKEN")
    if not token:
        log.error("❌ HF_TOKEN not set — add to GitHub Secrets")
        sys.exit(1)

    try:
        from huggingface_hub import HfApi
        import hf_xet  # noqa: F401 — required for .cbm binary upload
    except ImportError:
        log.error("❌ huggingface_hub or hf_xet not installed")
        log.error("   Run: pip install huggingface_hub hf_xet")
        sys.exit(1)

    api = HfApi()
    print(f"\n🚀 Deploying to {HF_REPO_ID}...")
    print("=" * 55)

    failed = []
    for local_path, repo_path in FILES_TO_DEPLOY.items():
        if not local_path.exists():
            log.warning(f"   ⚠️  Skipping missing: {local_path.name}")
            continue
        try:
            api.upload_file(
                path_or_fileobj=str(local_path),
                path_in_repo=repo_path,
                repo_id=HF_REPO_ID,
                repo_type="space",
                token=token,
                commit_message=f"auto-deploy: update {repo_path}",
            )
            print(f"   ✅ {repo_path}")
        except Exception as e:
            log.error(f"   ❌ Failed to upload {repo_path}: {e}")
            failed.append(repo_path)

    print("=" * 55)

    if failed:
        print(f"\n❌ Deploy failed for {len(failed)} file(s): {failed}")
        sys.exit(1)

    print(f"\n✅ Deploy complete → https://huggingface.co/spaces/{HF_REPO_ID}")
    print("   Space will rebuild automatically in ~3 minutes\n")


if __name__ == "__main__":
    deploy()
