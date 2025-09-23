#!/usr/bin/env bash
# setup.sh — create/update a local .venv (mamba/conda) and install deps

# Make working directory the script's directory
cd -- "$(dirname -- "${BASH_SOURCE[0]}")" || exit 1
# Source context.sh to get conda_exe, conda_venv, python_exe, etc.
source ./context.sh


# -------------------- Args --------------------
SHOW_HELP=false
DO_RESET=false
DO_DEBUG=false
DO_RESTART=false

for arg in "${@:-}"; do
  case "${arg:-}" in
    --help|-h)
      SHOW_HELP=true ;;
    --restart)
      DO_RESTART=true ;
      DO_RESET=true ;;
    --debug)
      DO_DEBUG=true ;
      DO_RESET=true ;;
    --reset)
      DO_RESET=true ;;
    *) ;;
  esac
done

if $SHOW_HELP; then
  cat <<'EOF'
Usage: ./setup.sh [--help] [--reset] [--restart] [--debug]

Options:
  --help      Show this help and exit
  --reset     Clear data/debug caches, reinstall requirements if needed
  --restart   Recreate the env at ./.venv (python=${PY_VERSION:-3.11})
  --debug     Also clears debug snapshots; sets config.json .debug=true

Behavior:
  - Ensures ./.venv exists (creates it if missing) with PY_VERSION=${PY_VERSION}
  - Installs requirements if env was just created, --reset was given, or if requirements.txt is newer than ./.venv
  - Touches ./.venv after a successful install to update its mtime
EOF

  exit 0
fi

# Recreate env if requested
CREATED_ENV=false
if $DO_RESTART || [[ ! -d "$(conda_venv)" ]]; then
  echo ">> Recreating env at $(conda_venv) (python=$PY_VERSION)"
  rm -rf "$(conda_venv)"
  conda_exe create -y -p "$(conda_venv)" "python=$PY_VERSION" 1>&2 || exit 1
  CREATED_ENV=true
fi

if $DO_RESET; then
  echo ">> Resetting data"
  if [[ -d data ]]; then
    rm -rf data/*
  fi
fi

if $DO_DEBUG; then
  echo ">> Resetting debug data"
  if [[ -d debug ]]; then
    rm -rf debug/*
  fi
fi

if [[ -f config.json ]]; then
  current_debug=$(jq -r '.debug // false' config.json 2>/dev/null || echo false)
  if [[ "$current_debug" != "$DO_DEBUG" ]]; then
    tmpfile=$(mktemp)
    jq ".debug = $DO_DEBUG" config.json > "$tmpfile" && mv "$tmpfile" config.json
    echo ">> Debug mode set to $DO_DEBUG"
  fi
fi


# Install requirements conditionally (on --restart or if requirements.txt newer than .venv)
if [[ -f requirements.txt ]]; then
  if $CREATED_ENV || $DO_RESTART || [[ requirements.txt -nt "$(conda_venv)" ]]; then
    echo ">> Installing requirements into $(conda_venv)"
    python_exe -m pip install --upgrade pip
    python_exe -m pip install -r requirements.txt
    # Mark env as updated so future runs can compare mtimes
    touch "$(conda_venv)"
  fi
fi


# Ensure a local Streamlit config to suppress first-run onboarding prompt

if [[ ! -f .streamlit/config.toml ]]; then
mkdir -p .streamlit
  cat > .streamlit/config.toml <<EOF
[browser]
gatherUsageStats = $(streamlit_browser_gather_usage_stats)

[server]
headless = $(streamlit_server_headless)
EOF
fi

# setup may have changed the streamlit config, so export the new values
streamlit_exports