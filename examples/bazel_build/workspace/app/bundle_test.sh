#!/bin/sh
grep -q core app/bundle.out && grep -q util app/bundle.out
