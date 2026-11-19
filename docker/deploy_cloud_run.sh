#!/usr/bin/env bash
# DLM-Gate Mod B: Google Cloud Run + Firestore Deployment Script (< $1.00 Budget Guard)
# Usage (after local testing): GCP_PROJECT_ID="your-project-id" bash docker/deploy_cloud_run.sh

set -euo pipefail
cd "$(dirname "$0")/.."  # --source . needs the repo root (Dockerfile lives there)

PROJECT_ID="${GCP_PROJECT_ID:?Please set GCP_PROJECT_ID before deploying to Cloud Run}"
REGION="${GCP_REGION:-europe-west1}"
SERVICE_NAME="dlm-gate-runtime"

echo "[1/5] Enabling Cloud Run, Cloud Build and Firestore APIs on project: ${PROJECT_ID}..."
gcloud services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com firestore.googleapis.com --project "${PROJECT_ID}"

echo "[2/5] Creating the Firestore (Native mode) database if it does not exist yet..."
if ! gcloud firestore databases describe --database="(default)" --project "${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud firestore databases create --location="${REGION}" --project "${PROJECT_ID}"
fi

echo "[3/5] Allowing the Cloud Run service account to read/write Firestore..."
PROJECT_NUMBER="$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')"
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role="roles/datastore.user" \
  --condition=None >/dev/null

echo "[4/5] Building & deploying the image to Cloud Run (scale-to-zero, single instance so dashboard state stays consistent)..."
gcloud run deploy "${SERVICE_NAME}" \
  --source . \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --allow-unauthenticated \
  --min-instances=0 \
  --max-instances=1 \
  --memory=512Mi \
  --cpu=1 \
  --set-env-vars="GCP_PROJECT_ID=${PROJECT_ID},LOCAL_FALLBACK=true,ENABLE_SPARK=false"

echo "[5/5] Deployment complete! Service URL:"
gcloud run services describe "${SERVICE_NAME}" --project "${PROJECT_ID}" --region "${REGION}" --format="value(status.url)"
echo "Panelde '3c' senaryosunu calistirin: sag ustte 'Defter: gercek Firestore' yaziyorsa defter Firestore'da."
