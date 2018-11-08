FROM maven:3 AS builder

WORKDIR /app
COPY . /app
RUN mvn package

FROM taobeier/openjdk
COPY --from=builder /app/target/redis-rdb-cli-release.zip /tmp/redis-rdb-cli-release.zip
WORKDIR /app
# because of the cli has set shebang
RUN unzip -o /tmp/redis-rdb-cli-release.zip \
        && apk add --no-cache bash \
        && rm -f /tmp/redis-rdb-cli-release.zip

WORKDIR /app/redis-rdb-cli/bin/
