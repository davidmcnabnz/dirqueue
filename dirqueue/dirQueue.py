#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright © 2020 Taylor C. Richberger
# This code is released under the license described in the LICENSE file

# This is a sample simple async generator that tries its best to recursively watch a directory.
import sys, os

#import flexx.event
from pathlib import Path
from typing import Generator, AsyncGenerator, Union
import fcntl
import json

import datetime, time
import shutil
import traceback
import threading

import asyncio
from concurrent.futures import ThreadPoolExecutor

USE_UVLOOP = 1

if USE_UVLOOP:
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except:
        traceback.print_exc(file=sys.stderr)
        sys.stderr.write("UNABLE TO USE uvloop")

import aiofiles
from asyncinotify import Inotify, Event, Mask

# this import seems to fail
from .threadSafeQueue import ThreadsafeQueue

DEBUG = False

# ensure singletons, only one instance per directory in this process
REGISTRY = {}

class Debug:
    start = 0
    catchup = 0
    discard = 0
    claim = 0
    count = 1
    delete = 0
    lock = 0
    notify = 0
    pump = 0
    put = 0
    requeue = 0
    scan = 0
    test = 0
    transfer = 0
    default = 1

THREAD_POOL = ThreadPoolExecutor(max_workers=4096)


class DirQueueThreadRunnerMixin(object):
    """
    Convenience mixin which provides a threadpool for delegating blocking function calls
    """
    _loop = None

    async def runInThread(self, func, *args, **kw):
        """
        Run a blocking call in an external thread and return its result.

        If it happens to be actually a coroutine function, then await it here and now without
        the overhead of a thread pool

        This is effectively just a wrapper that makes blocking functions look and feel like async ones.

        :param func: blocking function to run
        :param args: tuple of arguments to pass to the function
        :param kw: keywords if any
        :return: whatever the blocking function returns
        """
        if not self._loop:
            self._loop = asyncio.get_running_loop()

        # need the wrapper in order to provide keyword parameters support, because
        # run_in_executor() only accepts unnamed parameters
        def runIt():
            runItResult = func(*args, **kw)
            return runItResult

        # run the given function
        if asyncio.iscoroutinefunction(func):
            # somehow this got called with an awaitable
            result = await func(*args, **kw)
        else:
            # genuinely a blocker
            result = await self._loop.run_in_executor(THREAD_POOL, runIt)

        # all done, one way or the other
        return result


class DirQueueItem(DirQueueThreadRunnerMixin):
    """
    Every fetched DirQueueItem object has one of three destinations:
        - destruction
        - re-queueing
        - relocation to another queue
    """
    def __init__(self, loop, consumerId:str, dirPath:Union[Path, str], name:str, lockfile, meta:dict=None, semaphore:asyncio.Semaphore=None):
        """
        This object must only be instantiated if we have a valid lock.

        Note that it is executed within a separate thread, due to a bit of file I/O happening.
        However, the constructor of course makes no async calls elsewhere.

        :param loop: this is the loop in which the DirQueue object was instantiated. This needs
        to be passed to us here, because of the possibility that another thread may instantiate
        one of these objects
        :param consumerId:
        :param dirPath: full posix pathname of directory being used for queue storage
        :param name: unique name prefix of this entry
        :param lockfile: an open and locked lockfile
        """
        # add our DirQueue's fingerprint to the metadata, requiring we
        # save the metadata
        self._loop = loop

        self.consumerId = consumerId
        if isinstance(dirPath, Path):
            dirPath = dirPath.as_posix()
        self.dirPath = dirPath
        self.name = name
        self.lockfile = lockfile
        self.st_dev = os.stat(self.dirPath).st_dev

        prefixPath = self.prefixPath = dirPath + '/' + name
        self.lockPath = prefixPath + '.lock'
        self.metaPath = prefixPath + ".meta"
        # generate payload paths dynamically based on prefixPath and meta['payloads']

        if meta is None:
            meta = json.load(open(self.metaPath))
            meta = getNewMeta(**meta)
        self.meta = meta

        # indicate if this item has been properly disposed of
        self.requeued = False
        self.discarded = False
        self.transferred = False

        ctrl = self.meta['_ctrl']
        self.alreadyFetched = ctrl['fetched']
        ctrl['fetched'] = True
        if self.consumerId not in ctrl['handledBy']:
            ctrl['handledBy'].append(self.consumerId)
        with open(self.metaPath, "w") as f:
            json.dump(self.meta, f)

        self.semaphore = semaphore

    def __del__(self):
        """
        By default, garbage-collection of this object automatically results in deletion
        of all the files in the spool
        :return:
        """
        if self.requeued or self.transferred:
            return
        if not self.discarded:
            print("item dropped")

        self._cleanup_blocking()

    def _cleanup_blocking(self):
        if DEBUG:
            pdebug('delete', "Deleting {path} {name}".format(
                path=self.dirPath,
                name=self.name,
            ))

        # no longer need this because deleting the file automatically releases the lock
#        try:
#            fcntl.flock(self.f, fcntl.LOCK_UN)
#            self.f.close()
#        except Exception as e:
#           pdebug("close lockfile failed: %s" % str(e))

        for path in (self.metaPath, self.lockPath):
            if DEBUG:
                pdebug("delete", "item: delete %s" % path)
            if os.path.exists(path):
                try:
                    os.unlink(path)
                    if DEBUG:
                        pdebug("delete", "item: deleted %s" % path)
                except Exception as e:
                    if DEBUG:
                        pdebug("delete", "item: delete %s failed: %s" % (path, str(e)))

        self._deletePayloads()

    def _deletePayloads(self):
        for payloadName in self.meta['payloads']:
            payloadPath = self._getPayloadPath(payloadName)
            if os.path.exists(payloadPath):
                try:
                    os.unlink(payloadPath)
                except:
                    pass

    def __repr__(self):
        return "<DirQueueItem:{path}:{name}>".format(
            path=self.dirPath,
            name=self.name,
        )

    async def forgetHandling(self):
        self.meta['handledBy'] = []

    async def discard(self):
        #await asyncio.to_thread(self._discard_blocking)
        await self.runInThread(self._discard_blocking)
        if self.semaphore is not None:
            self.semaphore.release()

    async def requeue(self):
        #await asyncio.to_thread(self._requeue_blocking)
        await self.runInThread(self._requeue_blocking)
        if self.semaphore is not None:
            self.semaphore.release()

    async def transfer(self, dirPath):
        """
        Transfer this item to another directory.

        Requirements:
            - directory exists
            - we can read/write
            - directory is on same partition

        :param dirPath:
        :return:
        """
        #await asyncio.to_thread(self._transfer_blocking, dirPath)
        await self.runInThread(self._transfer_blocking, dirPath)
        if self.semaphore is not None:
            self.semaphore.release()

    def dump(self):
        print("DirQueueItem:")
        print("  name: %s" % self.name)
        print("  meta: %s" % str(self.meta))
        print("  alreadyFetched: %s" % self.alreadyFetched)
        if os.path.isfile(self.payloadPath):
            print("  payload at %s" % self.payloadPath)
        else:
            print("  no payload")

    def _discard_blocking(self):
        if DEBUG:
            pdebug("discard", "discard:%s:%s" % (self.dirPath, self.name))
        self.discarded = True
        self._cleanup_blocking()

    def _getPayloadPath(self, payloadName, prefixPath=None):
        if prefixPath is None:
            prefixPath = self.prefixPath
        payloadPath = prefixPath + '.' + payloadName + '.payload'
        return payloadPath

    def _getNewPathPrefix(self, newDir=None):
        stub, serial = self.name.rsplit('_', 1)
        newSerial = int(serial) + 1
        newName = '%s_%s' % (stub, newSerial)

        newPathPrefix = os.path.join(newDir or self.dirPath, newName)
        return newPathPrefix

    def _requeue_blocking(self):
        """
        Stop handling this item, and enable other queue consumers to receive it
        :return:
        """
        newPathPrefix = self._getNewPathPrefix()
        newPathLock = newPathPrefix + ".lock"
        newPathMeta = newPathPrefix + ".meta"

        if DEBUG:
            pdebug("requeue", "%s:%s" % (self.name, self.dirPath))

        # record this consumer has a past handler so we don't double-handle
        ctrl = self.meta['_ctrl']
        #if self.consumerId not in ctrl['handledBy']:
        #    ctrl['handledBy'].append(self.consumerId)
        ctrl['fetched'] = False

        # save the metadata back under new prefix
        try:
            # remove old metadata
            os.unlink(self.metaPath)
        except:
            pass
        with open(newPathMeta, "w") as f:
            #metaRaw = json.dumps(self.meta)
            #f.write(metaRaw)
            json.dump(self.meta, f)
            f.close()

        # now rename zero or more payloads
        self._movePayloads(self.prefixPath, newPathPrefix)

        # destroy the old lock file
        del self.lockfile
        os.unlink(self.lockPath)

        # create new lock file
        open(newPathLock, "w").close()

        # and protect against gc wiping the files
        self.requeued = True

    def _transfer_blocking(self, dirPath):
        """
        For moving to another directory, which is hopefully being watched
        :return:
        """
        if isinstance(dirPath, Path):
            dirPath = dirPath.as_posix()

        if DEBUG:
            pdebug("transfer", "%s -> %s:%s" % (self.dirPath, dirPath, self.name))

        if os.stat(dirPath).st_dev != self.st_dev:
            raise RuntimeError("Cannot transfer from %s to %s: different filesystems" % (
                self.dirPath,
                dirPath,
            ))

        newPathPrefix = self._getNewPathPrefix(dirPath)
        newPathLock = newPathPrefix + ".lock"
        newPathMeta = newPathPrefix + ".meta"

        # move payload, if any
        self._movePayloads(self.prefixPath, newPathPrefix)

        # wipe out old metadata, render out current metadata to new directory
        ctrl = self.meta['_ctrl']
        ctrl['xferFrom'].append(self.dirPath)
        ctrl.pop('transfer', None)
        ctrl['fetched'] = False
        os.unlink(self.metaPath)
        with open(newPathMeta, "w") as f:
            json.dump(self.meta, f)

        # wipe out lock in old dir, create new one in new dir
        os.unlink(self.lockPath)
        del self.lockfile
        open(newPathLock, "w").close()

        self.transferred = True

    def _movePayloads(self, prefixFrom, prefixTo):
        for payloadName in self.meta['payloads']:
            pathOld = self._getPayloadPath(payloadName, prefixFrom)
            pathNew = self._getPayloadPath(payloadName, prefixTo)
            os.rename(pathOld, pathNew)
class DirQueue(DirQueueThreadRunnerMixin):
    """
    Implement a Queue-like component based on storing spool files in a single
    filesystem directory. Designed to be safe for multi-process usage

    The standard usage is:
        - permit one or more publishers, and one or more consumers
        - these can be within same event loop, or different loop in same or other process
        - consumers are competing to get entries
        - spool entries to consist of 3 physical files:
            - payload (optional, can also be passed as a readable file object)
            - metadata (mandatory, must be a JSON-able dict-like object)
            - lock (zero length, used to trigger the watcher)

    Workflow:
        - publisher:
            - calls .put() to add the (payload, metadata) entry to the queue
            - unique filename prefix is created, based on datatime-pid
            - payload, metadata and lock filenames are generated based on prefix
            - payload and metadata files are written out and files closed
                - inotify watcher ignores these
            - lock file is created
                - closing it triggers the watchers
        - consumer:
            - startup:
                - before creating watchers, first scan directory
                - for each lock file entry:
                    - try to lock
                    - if lock succeeds:
                        - yield up entry to .get() caller
                    - else:
                        - skip it
                - create watcher, listening in dir for CLOSE_WRITE events
            - running:
                - for each CLOSE_WRITE event:
                    - if not lock file
                        - continue
                    - try to lock it
                    - if lock succeeds:
                        - yield up entry to .get() caller
                    - else:
                        - skip it
    Responsibility of .get() caller:
        - when it's safe to destroy the entry, move or delete its files

    Risks:
        - consumer task crashing without relinquishing lock
            - yield up entries as objects whose __del__ methods clean up
    """
    def __new__(cls, dirPath, *args, **kw):
        if isinstance(dirPath, Path):
            dirPath = dirPath.as_posix()
        elif not isinstance(dirPath, str):
            raise TypeError("dirPath %s is not a str or Path object" % dirPath)

        if dirPath in REGISTRY:
            # existing listener operating on this directory within this process
            return REGISTRY[dirPath]
        else:
            obj = super().__new__(cls)
            REGISTRY[dirPath] = obj
            return obj

    def __init__(self, dirPath:Union[Path, str], allowDoubleHandling=True, maxsize=1000):
        """
        Create a singleton directory queue manager
        :param dirPath: the absolute posix pathname of the directory to monitor
        :param allowDoubleHandling: whether it's permissible for the same queue entry to be
        handled twice by this object
        :param maxsize:
        """
        # first, must save a ref to the loop, to satisfy the parent's runInThread() util
        try:
            self._loop = asyncio.get_running_loop()
        except:
            raise RuntimeError("Cannot construct outside running loop")

        # validate/save the monitored directory path as a string
        if isinstance(dirPath, Path):
            dirPath = dirPath.as_posix()
        elif not isinstance(dirPath, str):
            raise TypeError("dirPath %s is not a str or Path object" % dirPath)
        self.dirPath = dirPath
        self.allowDoubleHandling = allowDoubleHandling

        # set up synchronisation attributes
        self._isRunning = False
        self._q = ThreadsafeQueue(maxsize=maxsize)
        self._qNames = ThreadsafeQueue() # for name prefixes in the monitored directory, before locks
        self._isStarted = False
        self._isStarted_lock = asyncio.Lock()
        self._consumerId = str(abs(hash((self, os.getpid()))))
        self._semLimit = asyncio.Semaphore(1000)

        # prevent race conditions with initial directory reads
        self._recentItems = set() # tuples (name, dir, mtime)

    async def get(self) -> DirQueueItem:
        """
        Like asyncio.Queue.get, but returns a DirPathItem object.

        The very first caller to this method inherits the duty of firing up the event
        pump and scanning for historical entries.

        :return:
        """
        if not self._isStarted:
            await self._start()

        # try to lock it, because otherwise some other process may have it
        while self._isRunning:
            itemName = await self._qNames.get()

            # apply semaphore limit
            await self._semLimit.acquire()

            # try to lock it and wrap it
            item = await self.runInThread(self._claim_blocking, itemName)
            if item is None:
                # Failed to claim it, which should only ever happen if something else
                # has already locked it
                continue

            # all good - we've got an item with a live lock
            return item

    async def put(self, /, name=None, meta=None, payloads=None, **payloadKw):
        #await asyncio.to_thread(self._put_blocking, meta, payload)
        await self.runInThread(self._put_blocking, name=name, meta=meta, payloads=payloads, **payloadKw)

    def _put_blocking(self, /, name=None, meta=None, payloads=None, **payloadKw):

        allPayloads = {}
        if payloads is None:
            pass
        elif isinstance(payloads, dict):
            allPayloads.update(payloads)
        else:
            raise TypeError("payloads must be dict, or omitted")
        allPayloads.update(payloadKw)

        basePath = os.path.join(self.dirPath, _getPrefix(name))

        if meta is None:
            meta = getNewMeta()
        elif isinstance(meta, dict):
            meta = getNewMeta(**meta)
        else:
            raise TypeError("Invalid metadata: %s" % type(meta))

        try:
            # now write in zero or more payloads
            for payloadName, payload in allPayloads.items():
                payloadPath = basePath + "." + payloadName + ".payload"
                if isinstance(payload, str):
                    mode = "w"
                elif isinstance(payload, bytes):
                    mode = "wb"
                elif hasattr(payload, 'read'):
                    mode = 'wb' if 'b' in payload.mode else 'w'
                else:
                    raise TypeError("%s:%s: payload must be str, bytes or readable file object" % (
                        self.dirPath,
                        payloadName,
                    ))

                with open(payloadPath, mode) as f:
                    if hasattr(payload, 'read'):
                        try:
                            shutil.copyfileobj(payload, f)
                        except:
                            pass
                        try:
                            payload.close()
                        except:
                            pass
                    else:
                        f.write(payload)
                    f.close()

                meta['payloads'].append(payloadName)

            # all payloads done, so can now finalise the metadata
            with open(basePath+".meta", "w") as f:
                #f.write(metaStr)
                json.dump(meta, f)


            # finally, create the lock
            lockPath = basePath + ".lock"
            with open(lockPath, "w") as f:
                f.close()
        except:
            traceback.print_exc()
            pdebug("put", "failed")

    async def _start(self):
        """
        The first task to call .get() must initialise this object,
        set up the watcher, catch up with old events
        :return:
        """
        # firstly get the watcher up, so it will accumulate a backlog which
        # covers all new events that happen while we're initialising
        if DEBUG:
            pdebug("start", "started")

        # bail if a catchup already in progress
        async with self._isStarted_lock:
            if self._isStarted:
                if DEBUG:
                    pdebug("start", "already done???")
                return

            # indicated we're started so we don't attract any more volunteers
            self._isStarted = True
            self._isRunning = True

            # we are the one to do the catchup, so firstly, set up the
            # watcher for right now
            self._notify = Inotify()
            self._watcher = self._notify.add_watch(
                self.dirPath,
                Mask.CLOSE_WRITE,
            )

            # now start up the pump which fetches lock file write-close events,
            # and adds new items to queue
            # save it because we may need to kill it later
            self._task_pump = asyncio.create_task(self._pump())

            if DEBUG:
                pdebug("start", "done")


    async def _pump(self):
        """
        Feed queue entry names into an internal async queue. Firstly, the
        ones which were present when we started up, then afterwards, the ones
        coming in from inotify events. Ensure there are no gaps, and no double-handling.
        :return: Never
        """
        try:
            await self._pump_()
        except asyncio.CancelledError:
            print("cancelled:%s" % self.dirPath)
        except BaseException:
            traceback.print_exc(file=sys.stderr)
            return

    async def _pump_(self):
        # process any items that pre-date the watcher
        # NOTE - there will be some duplicates, of items both being read by the notify
        # watcher, and read from a directory scan.
        # We avoid these with the _recentItems[_lock] devices

        #locks = await asyncio.to_thread(self._scanOldLocks_blocking)
        if DEBUG:
            pdebug("pump", "scanning for pre-existing queue items")
        preExistingLockNames = await self.runInThread(self._scanOldLocks_blocking)
        if DEBUG:
            pdebug("pump", "  found {n} pre-existing".format(
                n=len(preExistingLockNames),
            ))

        for lockName in preExistingLockNames:
            # add to the raw 'lock names feed'
            await self._qNames.put(lockName)

            # ensure we don't later double up in the inotify feed
            self._recentItems.add(lockName)

        if DEBUG:
            pdebug("pump", "now scanning inotify feed for new item appearances")

        watchingForOverlaps = True

        then = time.time()

        async for ev in self._notify:
            # first disqualify inotify events we're not interested in
            if ev.mask & Mask.ISDIR:
                # directory-related
                continue
            if ev.mask & Mask.CLOSE_NOWRITE:
                # closing without write - indicates nobody has finished writing a lockfile
                continue
            fileNameObj = ev.name
            if not fileNameObj:
                # not sure what that is
                # FIXME: check for inotify queue overruns
                if DEBUG:
                    pdebug("notify", "%s: strange event: %s" % (self.dirPath, str(ev)))
                continue

            # ok, a normal file got closed for write
            fileName = fileNameObj.name

            # we're only interested in .lock
            if not fileName.endswith(".lock"):
                continue

            # now we have a unique queue item name
            itemName = fileName.rsplit(".lock", 1)[0]

            if DEBUG:
                pdebug("notify", "%s:%s" % (self.dirPath, itemName))

            # qualification done - now, make sure our historical scan didn't cover this
            if watchingForOverlaps:

                if itemName in self._recentItems:
                    # this was handled in the initial directory scan
                    if DEBUG:
                        pdebug("notify", "%s:%s: ditch recent" % (self.dirPath, itemName))
                    continue
                else:
                    if DEBUG:
                        pdebug("notify", "%s:%s: new" % (self.dirPath, itemName))
                    self._recentItems.add(itemName)
                    if time.time() - then > 3:
                        # waited enough time for the overlaps to clear
                        # we haven't already seen this in the pre-existing scan
                        # and have expired the monitoring period
                        # all good now, we can stop monitoring overlaps
                        if DEBUG:
                            pdebug("notify", "%s:%s: ok, overlap window expired" % (self.dirPath, itemName))
                        watchingForOverlaps = False
                        self._recentItems = None # no longer needed

            # ok, we now have a newly-added item we haven't yet handled
            await self._qNames.put(itemName)

    def _scanOldLocks_blocking(self):
        locks = []
        for entry in os.scandir(self.dirPath):
            filename = entry.name
            if filename.endswith(".lock"):
                locks.append(filename.rsplit(".lock", 1)[0])
        locks.sort()
        return locks

    def _claim_blocking(self, itemName:str) -> Union[DirQueueItem, None]:
        """
        Given the name of a queue item present in the scanned directory, attempt
        to first lock it, then wrap it in a DirQueueItem object for processing by client

        :param itemName: string name of item's prefix
        :return: DirQueueItem if we were able to lock it and wrap it, otherwise None
        """
        try:
            lockPath = os.path.join(self.dirPath, itemName+".lock")
            if DEBUG:
                pdebug("claim", "try to lock {path}".format(
                    path=lockPath,
                ))

            # now we attempt the lock, which if successful, will mutex the entry for us
            lockfile = open(lockPath)
            try:
                # not entirely delighted with potential event loop impact on this call.
                # TODO: check if thread delegation improves througput
                fcntl.flock(lockfile.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

            except BlockingIOError:
                # Lock attempt failed, so someone else has it.
                # No further interest to us.
                if DEBUG:
                    pdebug("claim", "can't lock {path}".format(
                        path=lockPath,
                    ))
                lockfile.close()
                return None

            # lock succeeded, so can now wrap
            if DEBUG:
                pdebug("claim", "got lock {path}".format(
                    path=lockPath,
                ))

            # attempt to read metadata
            metaPath = os.path.join(self.dirPath, itemName+".meta")
            with open(metaPath) as f:
                try:
                    if 1:
                        metaRaw = None
                        meta = json.load(f)
                    else:
                        metaRaw = f.read()
                        meta = json.loads(metaRaw)
                        meta = getNewMeta(**meta)
                except Exception as e:
                    excStr = traceback.format_exc()
                    if DEBUG:
                        pdebug("claim", "Failed to read metadata from: %s: %s" % (
                            metaPath, repr(metaRaw)),
                        )
                    failPath = os.path.join(self.dirPath, itemName+".meta.fail")
                    try:
                        with open(failPath, "w") as f:
                            f.write(excStr)
                    except:
                        pass

            # bail if we've seen this and are not permitting double handling
            if (not self.allowDoubleHandling) and (self.consumerId in meta['_ctrl']['handledBy']):
                # we've already handled this
                return None

            # now wrap it - good to go
            # loop, consumerId:str, dirPath:Union[Path, str], name:str, lockfile, meta:dict=None
            qitem = DirQueueItem(
                self._loop,
                self._consumerId,
                self.dirPath,
                itemName,
                lockfile,
                meta,
                self._semLimit,
            )
            if DEBUG:
                pdebug("claim", "wrapped {path} as DirQueueItem".format(
                    path=itemName,
                ))
            return qitem

        except Exception as e:
            traceback.print_exc()
            if DEBUG:
                pdebug("claim", "skip {item}".format(
                    item=itemName,
                ))
            return None

    def __repr__(self):
        return "<DirQueue:%s>" % self.dirPath

# -----------------------------------------------------------
# support functions

def pdebug(chan, msg):
    flag = getattr(Debug, chan, None)
    if flag is None:
        print("No channel for %s" % chan)
        flag = Debug.default
    if not flag:
        return

    if not msg.endswith('\n'):
        msg += '\n'
    s = '%s:%s:%s' % (time.time_ns(), chan.upper(), msg)
    sys.stderr.write(s)
    sys.stderr.flush()


def _getPrefix(name=None):
    """
    Procure a human-readable filename prefix to help with investigations
    :return:
    """
    ns = time.time_ns()
    s, ns = divmod(ns, 1000000000)
    dt = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

    # notes on prefix format:datetime_nanoseconds_0
    prefix = dt + "_%d_%d_%s0" % (ns, os.getpid(), (name+'_') if name else '')
    return prefix

def getNewMeta(**kw):
    """
    Returns a dict containing all user keywords, but also guaranteeing the
    presence of a '_ctrl' key containing all needed housekeeping items

    :param kw:
    :return: dict with guaranteed presence of _ctrl
    """
    newMeta = {
        '_ctrl': {
            'handledBy': [],
            'fetched': False,
            'xferFrom': [],
        },
        'payloads': [],  # payloads by name
    }
    newMeta.update(kw)
    return newMeta


# ---------------------------------------------------------------
# START OF TEST CODE

import random
import base64

class DirQueueTest(object):
    """
    Stage and run a high-pressure test of the DirQueue system
    """
    def __init__(
            self, /,
            nQueues=3,
            nWorkersPerQueue=2,
            nItems=10,
            itemSize=1024,
            qMaxItems=1024,
            countDownModulus=1000,
            countUpModulus=1000,
            **kw):

        self.nQueues = nQueues
        self.nWorkersPerQueue = nWorkersPerQueue
        self.nItems = nItems
        self.itemSize = itemSize
        self.qMaxItems = qMaxItems
        self.countDownModulus = countDownModulus
        self.countUpModulus = countUpModulus

        self.kw = kw

        self.nGets = 0

    def run(self):
        try:
            result = asyncio.run(self.arun())
        except:
            traceback.print_exc()
            print("Something went wrong")

    async def arun(self):
        self.evStart = asyncio.Event()
        self.evDone = asyncio.Event()
        self.lockCountDown = asyncio.Lock()
        self.lockCountUp = asyncio.Lock()

        await self.setup()
        print("arun: wait for completion")
        await self.evDone.wait()
        print("arun: all done")

    async def setup(self):
        baseDir = "/tmp/dirQueueTest"
        if os.path.exists(baseDir):
            shutil.rmtree(baseDir)
        os.makedirs(baseDir)
        self.queues = []

        for i in range(self.nQueues):
            qDir = "{base}/{i}".format(base=baseDir, i=i)
            os.makedirs(qDir)
            q = DirQueue(qDir, maxsize=self.qMaxItems)
            #await q.catchup()
            self.queues.append(q)

        for i in range(self.nQueues):
            for j in range(self.nWorkersPerQueue):
                asyncio.create_task(self.worker(i, j))

        r = open("/dev/random", "rb")

        qFirst = self.queues[0]

        print("Test: Creating random payloads")
        payloads = []
        for i in range(self.nItems):
            payloadb = r.read(self.itemSize)
            payload = base64.encodebytes(payloadb).decode('utf-8')
            payloads.append((i, payload))
            if i % 1000 == 0:
                print("   %d of %d" % (i, self.nItems))
        print("Test: Payloads created, now injecting to first queue")

        async def putWorker():
            while payloads:
                try:
                    i, payload = payloads.pop()

                    # here, we're writing payload to a temp file, so we can put a readable
                    # file object to the queue
                    filename = "/tmp/%s" % i
                    with open(filename, "w") as f:
                        f.write(payload)
                    f = open(filename)
                    meta = {
                        'itemNum': i,
                    }
                    payloadDict = {
                        'payload%d' % i: f,
                    }
                    await qFirst.put(name='item_%d' % i, meta=meta, **payloadDict)

                    try:
                        os.unlink(filename)
                    except:
                        pass

                    if i % 1000 == 0:
                        print("   %d of %d" % (i, self.nItems))
                except:
                    return

        tasks = []
        for i in range(100):
            task = asyncio.create_task(putWorker())
            tasks.append(task)
        await asyncio.wait(tasks)

        print("Test: all payloads now injected, start the processing workers")
        self.tsStart = time.time()
        self.evStart.set()

        print("Test: setup completed")

    async def worker(self, qNum, workerNum):
        try:
            await self.worker_(qNum, workerNum)
        except BaseException as e:
            if not self.evDone.is_set():
                traceback.print_exc()
                print("worker:q=%d:n=%d: failed: %s" % (qNum, workerNum, str(e)))
        #print("worker:q=%d:n=%d: exited" % (qNum, workerNum))


    async def worker_(self, qNum, workerNum):
        """
        Work a single queue, reading items and randomly moving them
        :param qNum:
        :param workerNum:
        :return:
        """
        q = self.queues[qNum]

        await self.evStart.wait()

        def dbg(msg):
            print("worker:q=%s:n=%d:%s" % (qNum, workerNum, msg))

        isFirst = qNum == 0
        isLast = qNum == len(self.queues) - 1

        if isFirst:
            dirPrev = None
        else:
            dirPrev = self.queues[qNum - 1].dirPath

        if isLast:
            dirNext = None
        else:
            dirNext = self.queues[qNum+1].dirPath

        dbg("starting")

        while True:
            item = await q.get()
            await self.countUp()

#            dbg("got item %d" % item.meta['itemNum'])
            item.meta['_ctrl']['handledBy'] = []
            #await asyncio.sleep(random.random() * 0.2)

            x = random.random()
            if isFirst:
                if x < 0.4:
                    # keep here
                    pdebug("test", "requeue:first:%s:%s" % (item.dirPath, item.name))
                    await item.requeue()
                else:
                    if isLast:
                        # ultimate disposal
                        pdebug("test", "discard:only:%s:%s" % (item.dirPath, item.name))
                        await item.discard()
                        await self.countDown()
                    else:
                        pdebug("test", "transfer:first:%s -> %s:%s" % (item.dirPath, dirNext, item.name))
                        await item.transfer(dirNext)
            elif isLast:
                if x < 0.2:
                    pdebug("test", "transfer:last:%s -> %s:%s" % (item.dirPath, dirPrev, item.name))
                    await item.transfer(dirPrev)
                elif x < 0.4:
                    pdebug("test", "requeue:last:%s:%s" % (item.dirPath, item.name))
                    await item.requeue()
                else:
                    # ultimate disposal
                    pdebug("test", "discard:last:%s:%s" % (item.dirPath, item.name))
                    await item.discard()
                    await self.countDown()
            else:
                if x < 0.2:
                    pdebug("test", "transfer:middle:%s -> %s:%s" % (item.dirPath, dirPrev, item.name))
                    await item.transfer(dirPrev)
                elif x < 0.4:
                    pdebug("test", "requeue:middle:%s:%s" % (item.dirPath, item.name))
                    await item.requeue()
                else:
                    pdebug("test", "transfer:middle:%s -> %s:%s" % (item.dirPath, dirNext, item.name))
                    await item.transfer(dirNext)

    async def countUp(self):
        async with self.lockCountUp:
            self.nGets += 1
            if self.nGets % self.countUpModulus == 0:
                pdebug("count", "up:nGets=%d (%d/s)" % (self.nGets, self.nGets/(time.time()-self.tsStart)))

    async def countDown(self):
        async with self.lockCountDown:
            self.nItems -= 1
            if self.nItems % self.countDownModulus == 0:
                pdebug("count", "down:nItems=%d" % self.nItems)
            if self.nItems <= 0:
                pdebug("count", "down:all done")
                self.evDone.set()

def main():
    tester = DirQueueTest(
        nQueues=3,
        nWorkersPerQueue=1,
        nItems=10,
        itemSize=1024,
        qMaxItems=1024,
        countUpModulus=1000,
        countDownModulus=100,
    )
    tester.run()

if __name__ == '__main__':
    main()

