# Tiny static server (busybox httpd) — builds fast, no base-image pull beyond busybox.
FROM busybox
COPY index.html /www/index.html
EXPOSE 8080
CMD ["httpd", "-f", "-p", "8080", "-h", "/www"]
