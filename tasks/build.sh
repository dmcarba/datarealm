#!/bin/sh

set -eu
cd source-code
echo "Calling Maven ..."
./tasks/generate-settings.sh
cd ./sequencer

mvn verify