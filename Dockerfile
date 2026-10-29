# Binary được build trước ở bước CI (hoặc lệnh trong README) để Docker không cần quyền đọc module private.
FROM gcr.io/distroless/static-debian12:nonroot
COPY dist/executor /executor
ENTRYPOINT ["/executor"]
