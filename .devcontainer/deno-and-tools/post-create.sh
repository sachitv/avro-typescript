#!/bin/bash
set -e

sudo mkdir -p /home/vscode/.local/state /home/vscode/.codex
sudo chown -R vscode:vscode /home/vscode/.local /home/vscode/.codex
