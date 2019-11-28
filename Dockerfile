FROM maven:3 AS builder

WORKDIR /app
COPY . /app
RUN mvn clean install -Dmaven.test.skip=true

FROM taobeier/openjdk
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
        && ln -s /app/redis-rdb-cli/bin/rdt /usr/local/bin/rdt

WORKDIR /app/redis-rdb-cli