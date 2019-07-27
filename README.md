An s3 proxy for sia.

# Running

Replace `<sia_password>` with your sia api password.

## Docker

The `--network=host` parameter is optional, but likely required for most
configurations.

```
docker run --network=host -e SIA_PASSWORD=<sia_password> tdickman/sia-s3-proxy
```

## Local

```
pipenv shell
pipenv install
SIA_PASSWORD=<sia_password> python3 s3_proxy/main.py
```

# Environtment Variables

| Key          | Default   | Description                                   |
|--------------|-----------|-----------------------------------------------|
| HOST         | localhost |                                               |
| PORT         | 10001     |                                               |
| ROOT         | s3        | Subdirectory to store everything under in sia |
| SIA_HOST     | localhost |                                               |
| SIA_PORT     | 9980      |                                               |
| SIA_PASSWORD |           |                                               |
| CACHE_DIR    | ./        | Where to save md5 cache                       |

# Notes

* Currently s3-proxy returns immediately after uploading a file
  to sia, but the file is not immediately available for download (until the
  file has fully uploaded). In the future we may add a local cache to prevent
  this from occuring.
