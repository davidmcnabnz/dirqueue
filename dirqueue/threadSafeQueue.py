#!/usr/bin/env python3
"""
Implement a wrapper to asyncio.Queue that works regardless of what event
loop its .get() and .put() methods are invoked from
"""
import time
import threading
import asyncio

class ThreadsafeQueue(asyncio.Queue):
    """
    A wrapper of asyncio.Queue that allows its .get and .put methods to be safely
    called/awaited within the same event loop, or another event loop
    """
    def __init__(self, maxsize=0, *, loop=None):
        # deliberately crash out if not being executed within a live event loop
        if loop is None:
            loop = asyncio.get_running_loop()

        # ok to proceed
        super().__init__(maxsize=maxsize)
        self._loop = loop
        self._inst_thread = threading.current_thread()

    async def put(self, item):
        if threading.current_thread() == self._inst_thread:
            await super().put(item)
        else:
            # this seems strange -- called like a blocking function, but doesn't block the calling loop
            f = asyncio.run_coroutine_threadsafe(super().put(item), self._loop)

    async def get(self):
        if threading.current_thread() == self._inst_thread:
            return await (super().get())
        else:
            # this seems strange -- called like a blocking function, but doesn't block the calling loop
            f = asyncio.run_coroutine_threadsafe(super().get(), self._loop)
            result = f.result()
            return result

_registry = {
    'queue': None,
    'qIn': None,
    'qOut': None,
}

async def thread1_amain():
    qOut = ThreadsafeQueue()
    qIn = ThreadsafeQueue()
    _registry.update(
        qOut=qOut,
        qIn=qIn,
        eating=True,
    )

    async def ticker():
        n = 0
        while _registry['eating']:
            print("thread1:ticker:n=%d" % n)
            n += 1
            await asyncio.sleep(0.1)

    async def eat():
        while True:
            print("thread1:eat: fetching")
            item = await qIn.get()
            print("thread1:eat: got %s" % item)
            if item.endswith(":quit"):
                print("thread1:eat: got quit, bail")
                _registry['eating'] = False
                return

    #asyncio.create_task(ticker())
    asyncio.create_task(eat())

    for thing in ["foo", "bar", "baz", "quit"]:
        print("thread1: putting %s" % thing)
        await qOut.put(thing)
        print("thread1: put %s done" % thing)
        await asyncio.sleep(1)
    while _registry['eating']:
        print("thread1: waiting for eating() to finish")
        await asyncio.sleep(5)
    print("thread1: our eating() task has finished")

async def thread2_amain():
    while None in (_registry['qOut'], _registry['qIn']):
        print("thread2: Waiting for thread1's queues to get created")
        await asyncio.sleep(1)

    # reversed order, from our perspective
    qIn = _registry['qOut']
    qOut = _registry['qIn']

    async def ticker():
        n = 0
        while _registry['eating']:
            print("thread2:ticker:n=%d" % n)
            n += 1
            await asyncio.sleep(0.1)
    #asyncio.create_task(ticker())

    print("thread2: got both queues, now see if they work")
    while True:
        print("thread2: fetching from thread1's queue")
        item = await qIn.get()
        print("thread2: got %s" % item)
        await asyncio.sleep(2)
        reply = "pong:%s" % item

        print("thread2: putting reply %s to thread1 queue" % reply)
        await qOut.put(reply)
        print("thread2: reply sent")

        if item == "quit":
            break

    print("thread2: sent quit response, so we're done")
    await asyncio.sleep(1)
    pass

def thread1_test():
    asyncio.run(thread1_amain())
    pass

def thread2_test():
    asyncio.run(thread2_amain())
    pass

def runThreadSafeQueueDemo():
    print("main: create threads")
    thread1 = threading.Thread(target=thread1_test)
    thread2 = threading.Thread(target=thread2_test)

    print("main: launch thread2")
    thread2.start()
    print("main: run thread1")
    thread1.run()
    print("main: thread1 done")

def main():
    runThreadSafeQueueDemo()

if __name__ == '__main__':
    main()

