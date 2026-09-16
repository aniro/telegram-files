# syntax=docker/dockerfile:1.7

FROM eclipse-temurin:23-jdk AS api-builder

WORKDIR /src/api

COPY VERSION /src/VERSION
COPY api/gradlew api/gradlew.bat api/build.gradle api/settings.gradle ./
COPY api/gradle ./gradle
RUN chmod +x gradlew
COPY api/src ./src
RUN ./gradlew shadowJar --no-daemon && \
    mkdir -p /docker-artifacts && \
    cp build/libs/telegram-files.jar /docker-artifacts/api.jar && \
    jdeps --print-module-deps --ignore-missing-deps /docker-artifacts/api.jar > /docker-artifacts/dependencies.txt

FROM node:22-alpine AS web-builder

WORKDIR /src/web

COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web ./
ENV NEXT_PUBLIC_API_URL=/api \
    NEXT_PUBLIC_WS_URL=/ws \
    NEXT_TELEMETRY_DISABLED=1 \
    SKIP_ENV_VALIDATION=1
RUN npm run build

FROM eclipse-temurin:23-jdk-alpine AS runtime-builder

WORKDIR /custom-jre

COPY --from=api-builder /docker-artifacts/dependencies.txt .
RUN --mount=type=cache,target=/var/cache/apk \
    apk add --update-cache binutils && \
    jlink \
        --add-modules $(cat dependencies.txt) \
        --output jre \
        --strip-debug \
        --no-man-pages \
        --no-header-files \
        --compress=2 && \
    apk del binutils

FROM alpine:3.18.12 AS final

WORKDIR /app

ARG TARGETARCH
ENV JAVA_HOME=/jre \
    PATH="/jre/bin:$PATH" \
    LANG=C.UTF-8 \
    NGINX_PORT=80

RUN --mount=type=cache,target=/var/cache/apk \
    addgroup -S tf && \
    adduser -S -G tf tf && \
    apk add --update-cache nginx wget curl unzip tini su-exec gettext openssl3 libstdc++ gcompat libc6-compat && \
    rm -rf /tmp/* /var/tmp/* && \
    touch /run/nginx.pid && \
    chown -R tf:tf /app /etc/nginx /var/lib/nginx /var/log/nginx /run/nginx.pid && \
    printf '#!/bin/sh\njava -Djava.library.path=/app/tdlib -cp /app/api.jar telegram.files.Maintain "$@"\n' > /usr/bin/tfm && \
    chmod +x /usr/bin/tfm

COPY --from=runtime-builder --chown=tf:tf /custom-jre/jre /jre
COPY --from=api-builder --chown=tf:tf /docker-artifacts/api.jar /app/api.jar
COPY --from=web-builder --chown=tf:tf /src/web/out/ /app/web/

COPY --chown=tf:tf ./tdlib/linux_$TARGETARCH /app/tdlib
COPY --chown=tf:tf ./entrypoint.sh .
COPY --chown=tf:tf ./nginx.conf.template /etc/nginx/nginx.conf.template

EXPOSE $NGINX_PORT

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["/bin/sh", "./entrypoint.sh"]
