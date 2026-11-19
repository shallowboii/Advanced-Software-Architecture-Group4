#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
classes=$(mktemp -d)
trap 'rm -rf "$classes"' EXIT
if [ -n "${JAVA_HOME:-}" ]; then
  compiler="$JAVA_HOME/bin/javac"
  runtime="$JAVA_HOME/bin/java"
else
  compiler=javac
  runtime=java
fi
"$compiler" --release 21 -d "$classes" "$root/src/main/java/dk/sdu/beerfactory/Domain.java" "$root/src/test/java/dk/sdu/beerfactory/DomainCheck.java"
"$runtime" -cp "$classes" dk.sdu.beerfactory.DomainCheck
