## NOAA Ouray County CLimate Project


### Quick Start Guide

Prerequisites:
- macOS/Linux with either `mamba` or `conda` on PATH, or Windows WSL 
Steps:
1) Make scripts executable once:
    - `chmod +x setup.sh run.sh px.sh meta.sh python.sh`
2) Setup/reset the environment
    - `./setup.sh --help` shows utility flags:
    - `--debug`: implies `--reset`, clears `./data/` and `./debug/`, sets `config.json.debug=true`
    - `--reset`: clears `./data/` and `./debug/`; reinstalls deps if needed
    - `--restart`: deletes and recreates `./.venv` (fresh env)
3) Get an API token from NOAA and pass it to the shell
    - run `export NOAA_TOKEN='your_token_here' ` in your shell after activating
