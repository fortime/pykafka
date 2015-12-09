"""
Author: Keith Bourgoin, Emmett Butler
"""
__license__ = """
Copyright 2015 Parse.ly, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
__all__ = ["ResponseFuture", "Handler", "ThreadingHandler", "RequestHandler"]
import logging
import threading
import Queue

from collections import namedtuple
from .exceptions import HandlerStoppedException

log = logging.getLogger(__name__)
_STOP = object()

class ResponseFuture(object):
    """A response which may have a value at some point."""

    def __init__(self, handler):
        """
        :type handler: :class:`pykafka.handlers.Handler`
        """
        self.handler = handler
        self.error = False
        self._ready = handler.Event()

    def set_response(self, response):
        """Set response data and trigger get method."""
        self.response = response
        self._ready.set()

    def set_error(self, error):
        """Set error and trigger get method."""
        self.error = error
        self._ready.set()

    def get(self, response_cls=None, timeout=None):
        """Block until data is ready and return.

        Raises an exception if there was an error.
        """
        self._ready.wait(timeout)
        if self.error:
            raise self.error
        if response_cls:
            return response_cls(self.response)
        else:
            return self.response

    @property
    def ready(self):
        return self._ready.is_set()


class Handler(object):
    """Base class for Handler classes"""
    def spawn(self, target, *args, **kwargs):
        """Create the worker that will process the work to be handled"""
        raise NotImplementedError


class ThreadingHandler(Handler):
    """A handler. that uses a :class:`threading.Thread` to perform its work"""
    QueueEmptyError = Queue.Empty
    Queue = Queue.Queue
    Event = threading.Event
    Lock = threading.Lock

    def spawn(self, target, *args, **kwargs):
        t = threading.Thread(target=target, *args, **kwargs)
        t.daemon = True
        t.start()
        return t


class RequestHandler(object):
    """Uses a Handler instance to dispatch requests."""

    Task = namedtuple('Task', ['request', 'future'])

    def __init__(self, handler, connection):
        """
        :type handler: :class:`pykafka.handlers.Handler`
        :type connection: :class:`pykafka.connection.BrokerConnection`
        """
        self.handler = handler
        self.connection = connection
        self._requests = handler.Queue()
        self.ending = False
        self.ended = handler.Event()
        self.t = None
        self.log = log

    def request(self, request, has_response=True):
        """Construct a new request

        :type request: :class:`pykafka.protocol.Request`
        :param has_response: Whether this request will return a response
        :returns: :class:`pykafka.handlers.ResponseFuture`
        """
        if self.ending:
            raise HandlerStoppedException('This handler has stopped.')

        future = None
        if has_response:
            future = ResponseFuture(self.handler)

        task = self.Task(request, future)
        self._requests.put(task)

        # check if this handler has stopped.
        if self.ending and future and not future.ready:
            raise HandlerStoppedException('This handler has stopped after putting task.')
        return future

    def start(self):
        """Start the request processor."""
        self.t = self._start_thread()

    def stop(self):
        """Stop the request processor."""
        if self.ending:
            return

        self.ending = True
        self._requests.join()
        self.ended.set()
        # tell worker thread to stop.
        self._requests.put(_STOP)
        if self.t:
            self.t.join()
            self.t = None

    def _flush(self):
        while True:
            # it will raise an exception finally.
            try:
                task = self._requests.get(block=False)
            except Exception:
                break
            if task is _STOP:
                self._requests.task_done()
                continue
            task.set_error(HandlerStoppedException('This handler has stopped'))
            self._requests.task_done()

    def _start_thread(self):
        """Run the request processor"""
        def worker():
            future = None
            try:
                while not self.ended.is_set():
                    task = self._requests.get()
                    if task is _STOP:
                        self._requests.task_done()
                        break
                    future = task.future
                    try:
                        self.connection.request(task.request)
                        if task.future:
                            res = self.connection.response()
                            task.future.set_response(res)
                    except Exception as e:
                        if task.future:
                            task.future.set_error(e)
                    finally:
                        self._requests.task_done()
            except Exception as exc:
                log.warn('Exception happened in worker thread of pykafka handler.', exc_info=True)
                if future and not future.ready:
                    future.set_error(exc)
            finally:
                self.ended.set()
                self.ending = True
                # clear all self._requests
                self._flush()
                log.info('Worker thread of pykafka handler exits')
        return self.handler.spawn(worker)
