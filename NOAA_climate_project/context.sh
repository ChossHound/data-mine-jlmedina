#!/usr/bin/env bash

function context_dir() {
    echo "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
}

function conda_exe() {
    local cmd bin dist
    for cmd in mamba conda; do
        for dist in miniforge3 miniconda3; do
            if command -v $cmd >/dev/null 2>&1 || [ -x "$HOME/$dist/bin/$cmd" ]; then
                if [ $# -eq 0 ]; then
                    echo $cmd
                else
                    $cmd "$@"
                fi
                return
            fi
        done
    done
    echo "ERROR: neither 'mamba' nor 'conda' found in PATH." 1>&2
    exit 1
}

function conda_venv() {
    echo "$(context_dir)/.venv"
}

function python_exe() {
    "$(conda_exe)" run -p "$(conda_venv)" python "$@"
}

function config_json() {
    local file="$(context_dir)/config.json"
    if [ $# -eq 0 ]; then
        echo "$file"
    else
        jq "$@" "$file"
    fi
}

function streamlit_config() {
    echo "$(context_dir)/.streamlit/config.toml"
}

function streamlit_browser_gather_usage_stats() {
    config_json .streamlit.browser_gather_usage_stats
}

function streamlit_server_headless() {
    config_json .streamlit.server_headless
}

function streamlit_debug_snapshots() {
    config_json .debug
}

function streamlit_exports() {
    export STREAMLIT_BROWSER_GATHERUSAGESTATS="$(streamlit_browser_gather_usage_stats)"
    export STREAMLIT_SERVER_HEADLESS="$(streamlit_server_headless)"
    export STREAMLIT_DEBUG_SNAPSHOTS="$(streamlit_debug_snapshots)"
}

# Also set env vars explicitly for non-interactive runs
streamlit_exports

export -f conda_exe
export -f conda_venv
export -f python_exe
export -f context_dir
export -f config_json
export -f streamlit_config
export -f streamlit_browser_gather_usage_stats
export -f streamlit_server_headless
export -f streamlit_debug_snapshots
export -f streamlit_exports

if [[ "${BASH_SOURCE[0]:-}" == "${0:-}" ]]; then
    "$@"
fi
