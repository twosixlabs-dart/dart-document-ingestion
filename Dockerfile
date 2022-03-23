FROM twosixlabsdart/java-fatjar-runner:latest

LABEL maintainer="michael.reynolds@twosixtech.com"

ENV SCALA_VERSION '2.12'

ENV JAVA_OPTS "-Xms3g -Xmx18g -XX:+UseG1GC"
ENV PROGRAM_ARGS ''

COPY ./target/scala-$SCALA_VERSION/*assembly*.jar $APP_DIR

COPY scripts/run.sh /opt/app/run.sh

RUN chmod -R 755 /opt/app

ENTRYPOINT ["/opt/app/run.sh"]
