#/bin/bash
rm -f 000003.log  CURRENT  LOCK  LOG  MANIFEST-000002
replicant daemon --foreground --listen 127.0.0.1 --listen-port 1992
