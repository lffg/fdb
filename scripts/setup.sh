#!/usr/bin/env bash

if ! command -v rustup &> /dev/null; then
    echo "rustup is not installed, please install it at:"
    echo 'https://www.rust-lang.org/tools/install'
    exit 1
fi

if rustup toolchain list | grep nightly > /dev/null; then
    echo "Nightly toolchain already installed."
else
    echo "Nightly toolchain is not installed."
    echo "Will install it using \`rustup toolchain install nightly\`..."
    rustup toolchain install nightly
fi
