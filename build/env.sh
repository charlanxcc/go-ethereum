#!/bin/sh

set -e

if [ ! -f "build/env.sh" ]; then
    echo "$0 must be run from the root of the repository."
    exit 2
fi

# Download etcd if it doesn't exist yet. Need to figure out release or tags later.
if [ ! -d "$PWD"/vendor/github.com/coreos/etcd ]; then
    [ -d "$PWD"/vendor/github.com/coreos ] || mkdir -p "$PWD"/vendor/github.com/coreos;
    git clone https://github.com/coreos/etcd $PWD/vendor/github.com/coreos/etcd
    if [ ! $? = 0 ]; then
        echo "Failed to get etcd.";
        exit 2
    fi
fi

# Create fake Go workspace if it doesn't exist yet.
workspace="$PWD/build/_workspace"
root="$PWD"
ethdir="$workspace/src/github.com/ethereum"
if [ ! -L "$ethdir/go-ethereum" ]; then
    mkdir -p "$ethdir"
    cd "$ethdir"
    ln -s ../../../../../. go-ethereum
    cd "$root"
fi

# Set up the environment to use the workspace.
GOPATH="$workspace"
export GOPATH

# Run the command inside the workspace.
cd "$ethdir/go-ethereum"
PWD="$ethdir/go-ethereum"

# Launch the arguments with the configured environment.
exec "$@"
