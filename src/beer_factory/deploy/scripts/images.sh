#!/bin/sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
image_prefix=${IMAGE_PREFIX:-docker.io/beer-factory}
image_tag=${IMAGE_TAG:-dev}
minikube_profile=${MINIKUBE_PROFILE:-beer-factory}
machines="mashing lautering boiling whirlpooling filtering fermentation_maturation"

usage() {
  echo "Usage: sh deploy/scripts/images.sh {build|load-minikube|save [archive.tar]}" >&2
  echo "Options: IMAGE_PREFIX, IMAGE_TAG, MINIKUBE_PROFILE, PLATFORM (build only)" >&2
  exit 1
}

[ "$#" -ge 1 ] || usage
action=$1
shift

case "$action" in
  build)
    [ "$#" -eq 0 ] || usage
    for machine in $machines; do
      image_name=$(printf '%s' "$machine" | tr '_' '-')
      set -- docker build -f "$project_dir/$machine/Dockerfile" \
        -t "$image_prefix/$image_name:$image_tag"
      if [ -n "${PLATFORM:-}" ]; then
        set -- "$@" --platform "$PLATFORM"
      fi
      "$@" "$project_dir"
    done
    ;;
  load-minikube)
    [ "$#" -eq 0 ] || usage
    for machine in $machines; do
      image_name=$(printf '%s' "$machine" | tr '_' '-')
      minikube -p "$minikube_profile" image load "$image_prefix/$image_name:$image_tag"
    done
    ;;
  save)
    [ "$#" -le 1 ] || usage
    archive=${1:-/tmp/beer-factory-images.tar}
    set --
    for machine in $machines; do
      image_name=$(printf '%s' "$machine" | tr '_' '-')
      set -- "$@" "$image_prefix/$image_name:$image_tag"
    done
    docker image save -o "$archive" "$@"
    echo "Saved machine images to $archive"
    ;;
  *) usage ;;
esac
