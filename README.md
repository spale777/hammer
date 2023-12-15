# Hammer

Lightweight Spacemesh POST mover,

Uses `inotify` to watch for new Spacemesh POST bins and then fires off `rsync` to move
them to their final destination.

It can do many in parallel, one at a time, one per source, or one per destination.

### Forked from https://github.com/lmacken/plow
