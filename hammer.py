#!/usr/bin/env python3
"""
The Hammer.

An efficient Spacemash bin mover.

Author: Luke Macken <phorex@protonmail.com>
SPDX-License-Identifier: GPL-3.0-or-later
"""
import sys
import glob
import shutil
import random
import urllib.request
import asyncio
import aionotify
import os
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Local bin sources
# For wildcards:
#   SOURCES = glob.glob('/mnt/*')
SOURCES = []

# Rsync destinations
# Examples: ["/mnt/HDD1", "192.168.1.10::hdd1"]
DESTS = []

# By default file must be at least 4 GiB in size in order to be considered valid
# Change this value if you have files of a different size
FILE_SIZE = 4 * 1024**4

# Shuffle bin destinations. Useful when using many smeshers to decrease the odds
# of them copying to the same drive simultaneously.
SHUFFLE = True 

# Rsync bandwidth limiting
BWLIMIT = None

# Optionally set the I/O scheduling class and priority
IONICE = None  # "-c 3" for "idle"

# Only send 1 bin at a time, regardless of source/dest. 
ONE_AT_A_TIME = False

# Each bin source can have a lock, so we don't send more than one file from
# that origin at any given time.
ONE_PER_DRIVE = False

# Short & long sleep durations upon various error conditions
SLEEP_FOR = 60 * 3
SLEEP_FOR_LONG = 60 * 20

RSYNC_CMD = "rsync"

if SHUFFLE:
    random.shuffle(DESTS)


# Rsync parameters. For FAT/NTFS you may need to remove --preallocate
if BWLIMIT:
    RSYNC_FLAGS = f"--remove-source-files --preallocate --whole-file --bwlimit={BWLIMIT}"
else:
    RSYNC_FLAGS = "--remove-source-files --preallocate --whole-file"

if IONICE:
    RSYNC_CMD = f"ionice {IONICE} {RSYNC_CMD}"

LOCK = asyncio.Lock()  # Global ONE_AT_A_TIME lock
SRC_LOCKS = defaultdict(asyncio.Lock)  # ONE_PER_DRIVE locks


async def binfinder(paths, bin_queue, loop):
    for path in paths:
        for bin in Path(path).glob("**/*.bin"):
            if os.path.getsize(bin) >= FILE_SIZE:
                await bin_queue.put(bin)
    await binwatcher(paths, bin_queue, loop)


async def binwatcher(paths, bin_queue, loop):
    watcher = aionotify.Watcher()
    for path in paths:
        if not Path(path).exists():
            print(f'! Path does not exist: {path}')
            continue
        print('watching', path)
        watcher.watch(
            alias=path,
            path=path,
            flags=aionotify.Flags.CLOSE_WRITE,
        )
    await watcher.setup(loop)
    while True:
        event = await watcher.get_event()
        if event.name.endswith(".bin"):
            bin_path = Path(event.alias) / event.name
            await bin_queue.put(bin_path)


async def hammer(dest, bin_queue, loop):
    print(f"🔨 hammering to {dest}")
    while True:
        try:
            bin = await bin_queue.get()
            cmd = f"{RSYNC_CMD} {RSYNC_FLAGS} {bin} {dest}"

            # For local copies, we can check if there is enough space.
            dest_path = Path(dest)
            if dest_path.exists():
                # Make sure it's actually a mount, and not our root filesystem.
                if not dest_path.is_mount():
                    print(f"Smesher destination {dest_path} is not mounted. Trying again later.")
                    await bin_queue.put(bin)
                    await asyncio.sleep(SLEEP_FOR)
                    continue

                bin_size = bin.stat().st_size
                dest_free = shutil.disk_usage(dest).free
                if dest_free < bin_size:
                    print(f"Smesher {dest} is full")
                    await bin_queue.put(bin)
                    # Just quit the worker entirely for this destination.
                    break

            # One at a time, system-wide lock
            if ONE_AT_A_TIME:
                await LOCK.acquire()

            # Only send one bin from each SSD at a time
            if ONE_PER_DRIVE:
                await SRC_LOCKS[bin.parent].acquire()

            try:
                print(f"🔨 {bin} ➡️  {dest}")

                # Send a quick test copy to make sure we can write, or fail early.
                test_cmd = f"rsync /etc/hostname {dest}"
                proc = await asyncio.create_subprocess_shell(
                    test_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await proc.communicate()
                if proc.returncode != 0:
                    print(f"⁉️  {test_cmd!r} exited with {proc.returncode}")
                    await bin_queue.put(bin)
                    break

                # Now rsync the real bin
                proc = await asyncio.create_subprocess_shell(
                    cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                start = datetime.now()
                stdout, stderr = await proc.communicate()
                finish = datetime.now()
            finally:
                if ONE_PER_DRIVE:
                    SRC_LOCKS[bin.parent].release()
                if ONE_AT_A_TIME:
                    LOCK.release()

            if proc.returncode == 0:
                print(f"🏁 {cmd} ({finish - start})")
            elif proc.returncode == 10:  # Error in socket I/O
                # Retry later.
                print(f"⁉️ {cmd!r} exited with {proc.returncode} (error in socket I/O)")
                await bin_queue.put(bin)
                await asyncio.sleep(SLEEP_FOR_LONG)
            elif proc.returncode in (11, 23):  # Error in file I/O
                # Most likely a full drive.
                print(f"⁉️ {cmd!r} exited with {proc.returncode} (error in file I/O)")
                await bin_queue.put(bin)
                print(f"{dest} hammer exiting")
                break
            else:
                print(f"⁉️ {cmd!r} exited with {proc.returncode}")
                await asyncio.sleep(SLEEP_FOR)
                await bin_queue.put(bin)
                print(f"{dest} hammer exiting")
                break
            if stdout:
                output = stdout.decode().strip()
                if output:
                    print(f"{stdout.decode()}")
            if stderr:
                print(f"⁉️ {stderr.decode()}")
        except Exception as e:
            print(f"! {e}")


async def main(paths, loop):
    bin_queue = asyncio.Queue()
    futures = []

    # Add bins to queue
    futures.append(binfinder(paths, bin_queue, loop))

    # Fire up a worker for each hammer
    for dest in DESTS:
        futures.append(hammer(dest, bin_queue, loop))

    print('🔨 Hammer running...')
    await asyncio.gather(*futures)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main(SOURCES, loop))
    except KeyboardInterrupt:
        pass
