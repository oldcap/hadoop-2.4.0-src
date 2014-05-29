#!/bin/bash

/data/apache-maven-3.2.1/bin/mvn -T 4 compile
/data/apache-maven-3.2.1/bin/mvn -T 4 package -DskipTests
