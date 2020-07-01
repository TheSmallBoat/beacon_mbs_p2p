FROM alpine:3.8

LABEL maintainer="Abe.Chua <abecai@gmail.com>" \
    description="Beacon MQTT Broker Service Over P2P Network"

ENV VERSION=2020.03.20

RUN set -x && \
    addgroup -S -g 1883 beacon 2>/dev/null && \
    adduser -S -u 1883 -D -H -h /var/empty -s /sbin/nologin -G beacon -g beacon beacon 2>/dev/null && \
    mkdir /beacon && \
    chown -R beacon:beacon /beacon

WORKDIR /beacon
# Set up the entry point script and default command

COPY docker-entrypoint.sh /
COPY mbs_p2p_linux ./

EXPOSE 1883
EXPOSE 16666
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["./mbs_p2p_linux", "-p=16666", "-m=1883","172.17.0.2:16666"]