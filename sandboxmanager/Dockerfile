# Packages a binary built on the host; see deploy.sh.
FROM gcr.io/distroless/static-debian12:nonroot
COPY bin/sandbox-manager /sandbox-manager
EXPOSE 8080
ENTRYPOINT ["/sandbox-manager"]
