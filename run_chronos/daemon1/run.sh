#/bin/bash
rm -f *.log *.sst  CURRENT  LOCK  LOG  MANIFEST-000002
replicant daemon --foreground --listen 127.0.0.1 --listen-port 1992
