#!/bin/bash

# Same defaults as config.sh, with the year-2013 YAML selected by default.
export CONFIG_PATH="${CONFIG_PATH:-examples/minhash_simple/fineweb_012_year_2013.yaml}"

source "$(dirname "${BASH_SOURCE[0]}")/config.sh"
