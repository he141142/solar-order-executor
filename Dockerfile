# Binary được build trước ở bước CI (hoặc lệnh trong README) để Docker không cần quyền đọc module private.
FROM gcr.io/distroless/static-debian12:nonroot
# --chmod: artifact của GitHub Actions (và build trên Windows) làm mất quyền thực thi của binary
# -> không có dòng này container báo "exec: /executor: permission denied".
COPY --chmod=0755 dist/executor /executor
ENTRYPOINT ["/executor"]
