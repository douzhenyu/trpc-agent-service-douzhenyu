FROM alpine:3.22.1

RUN apk add --no-cache git git-daemon

COPY deploy /work/deploy

RUN git config --global user.name "Kubernetes Smoke" \
    && git config --global user.email "smoke@example.invalid" \
    && git -C /work init --initial-branch=main \
    && git -C /work add deploy \
    && git -C /work commit -m "smoke chart" \
    && mkdir -p /srv/git \
    && git clone --bare /work /srv/git/platform.git

EXPOSE 9418

ENTRYPOINT ["git", "daemon", "--reuseaddr", "--export-all", "--base-path=/srv/git", "--verbose", "/srv/git"]
