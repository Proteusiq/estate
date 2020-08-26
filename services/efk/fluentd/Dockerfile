FROM fluent/fluentd:v1.6-debian-1
USER root
RUN ["fluent-gem", "install", "fluent-plugin-multi-format-parser"]
RUN ["gem", "install", "fluent-plugin-elasticsearch", "--no-document", "--version", "3.5.2"]
USER fluent
