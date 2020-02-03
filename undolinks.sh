#!/bin/bash

find $1 -type l |parallel 'cp --remove-destination $(readlink -f {}) {}'
