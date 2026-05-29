ARG ALPINE_VERSION=3.22

FROM alpine:${ALPINE_VERSION} AS jwker-downloader

ARG TARGETARCH
ARG JWKER_VERSION=v0.2.1
ARG JWKER_LINUX_AMD64_SHA256=f37866a81509a5021378035df31a767c60a49a71e91379aef1fed621bc3e024d
ARG JWKER_LINUX_ARM64_SHA256=7482cf4c5f3be63c8cca5ec00a720cf0f5bdd0a34f607c558e68ebbebed82e95

# jwker v0.2.1 has no release attestation, so pin the reviewed asset digests here.
RUN apk add --no-cache curl tar && \
    case "${TARGETARCH}" in \
        amd64) JWKER_ARCH="x86_64"; JWKER_SHA256="${JWKER_LINUX_AMD64_SHA256}" ;; \
        arm64) JWKER_ARCH="arm64"; JWKER_SHA256="${JWKER_LINUX_ARM64_SHA256}" ;; \
        *) echo "Unsupported architecture for jwker: ${TARGETARCH}" >&2; exit 1 ;; \
    esac && \
    JWKER_ASSET="jwker_Linux_${JWKER_ARCH}.tar.gz" && \
    curl -fsSLo /tmp/jwker_checksums.txt "https://github.com/jphastings/jwker/releases/download/${JWKER_VERSION}/checksums.txt" && \
    curl -fsSLo "/tmp/${JWKER_ASSET}" "https://github.com/jphastings/jwker/releases/download/${JWKER_VERSION}/${JWKER_ASSET}" && \
    awk -v asset="${JWKER_ASSET}" '$2 == asset { print; found=1 } END { exit found ? 0 : 1 }' /tmp/jwker_checksums.txt > /tmp/jwker_checksum.txt && \
    (cd /tmp && sha256sum -c jwker_checksum.txt) && \
    printf '%s  %s\n' "${JWKER_SHA256}" "/tmp/${JWKER_ASSET}" | sha256sum -c - && \
    mkdir -p /out && \
    tar xzf "/tmp/${JWKER_ASSET}" -C /out jwker && \
    chmod +x /out/jwker

FROM alpine:${ALPINE_VERSION} AS kubectl-downloader

ARG TARGETARCH
ARG KUBECTL_VERSION=v1.31.0

# kubectl publishes checksum, signature, and certificate sidecars.
RUN apk add --no-cache curl cosign && \
    KUBECTL_BASE_URL="https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${TARGETARCH}" && \
    mkdir -p /out && \
    curl -fsSLo /out/kubectl "${KUBECTL_BASE_URL}/kubectl" && \
    curl -fsSLo /tmp/kubectl.sha256 "${KUBECTL_BASE_URL}/kubectl.sha256" && \
    printf '%s  %s\n' "$(cat /tmp/kubectl.sha256)" /out/kubectl | sha256sum -c - && \
    curl -fsSLo /tmp/kubectl.sig "${KUBECTL_BASE_URL}/kubectl.sig" && \
    curl -fsSLo /tmp/kubectl.cert "${KUBECTL_BASE_URL}/kubectl.cert" && \
    cosign verify-blob /out/kubectl \
        --signature /tmp/kubectl.sig \
        --certificate /tmp/kubectl.cert \
        --certificate-identity krel-staging@k8s-releng-prod.iam.gserviceaccount.com \
        --certificate-oidc-issuer https://accounts.google.com && \
    chmod +x /out/kubectl

FROM openbao/openbao:2.5.4

# Install runtime dependencies and copy verified tools from downloader stages.
RUN apk add --no-cache openssl uuidgen helm curl bash jq

COPY --from=jwker-downloader /out/jwker /usr/local/bin/jwker
COPY --from=kubectl-downloader /out/kubectl /usr/local/bin/kubectl

WORKDIR /app

COPY --chmod=775 migrations/ /app/migrations/
COPY --chmod=775 addons/ /app/addons/
COPY --chmod=775 entrypoint.sh /app/

CMD ["/app/entrypoint.sh"]
