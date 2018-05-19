#!/bin/bash
set -e
dotnet restore build.proj
dotnet fake build $@