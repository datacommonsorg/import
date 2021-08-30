# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Build stage
FROM maven:3.6.0-jdk-11-slim AS build
WORKDIR /home
COPY . /home
RUN mvn -f /home/pom.xml clean package

# Package stage
FROM openjdk:11-jre-slim
COPY --from=build /home/server/target/datacommons-server-*.jar /usr/local/lib/server.jar
EXPOSE 8080
ENTRYPOINT ["java","-jar","/usr/local/lib/server.jar"]