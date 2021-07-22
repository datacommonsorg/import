#!/bin/bash

# This is the same test that pre-submit (via Cloud Build) runs.
mvn com.github.os72:protoc-jar-maven-plugin:run compile com.coveo:fmt-maven-plugin:check test
