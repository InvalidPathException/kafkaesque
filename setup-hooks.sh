#!/bin/bash
git config core.hooksPath .githooks
echo "Git hooks configured successfully!"
echo "To bypass the hook (not recommended), use: git commit --no-verify"