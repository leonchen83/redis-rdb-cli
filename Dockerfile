FROM maven:3.8-openjdk-11-slim AS builder

WORKDIR /app
COPY . /app
RUN mvn clean install -Dmaven.test.skip=true

FROM bellsoft/liberica-openjdk-alpine-musl:11
COPY --from=builder /app/target/redis-rdb-cli-release.zip /tmp/redis-rdb-cli-release.zip
WORKDIR /app
# because of the cli has set shebang
RUN unzip -o /tmp/redis-rdb-cli-release.zip \
        && apk add --no-cache bash \
        && rm -f /tmp/redis-rdb-cli-release.zip \
        && ln -s /app/redis-rdb-cli/bin/rct /usr/local/bin/rct \
        && ln -s /app/redis-rdb-cli/bin/rmt /usr/local/bin/rmt \
        && ln -s /app/redis-rdb-cli/bin/rst /usr/local/bin/rst \
        && ln -s /app/redis-rdb-cli/bin/ret /usr/local/bin/ret \
        && ln -s /app/redis-rdb-cli/bin/rdt /usr/local/bin/rdt \
        && ln -s /app/redis-rdb-cli/bin/rcut /usr/local/bin/rcut \
        && ln -s /app/redis-rdb-cli/bin/rcut /usr/local/bin/rmonitor

WORKDIR /app/redis-rdb-cli