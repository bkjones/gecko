#/usr/bin/env python

"""
Abstracting IOloop/AMQP operations used by gecko proper. This is
based on pika's tornado demo here:

https://github.com/pika/pika/blob/master/examples/demo_tornado.py

"""


import multiprocessing
import os
import pika
from pika.adapters.tornado_connection import TornadoConnection
import tornado
import logging
from pyrabbit.api import Server

logging.basicConfig(level=logging.DEBUG)

__author__ = 'Brian K. Jones'
__email__ = 'bkjones@gmail.com'
__since__ = '05/25/2011'

class PoolManager(object):
    """
    Takes in the AMQP config, which includes the info needed to connect and
    consume from the broker, as well as the min/max number of consumer
    threads to run, spawn/kill threshholds, etc.

    It doesn't connect or consume itself. It passes that on to the workers.
    This object's responsibility is limited to thread management and providing
    a singular interface with with to communicate w/ threads & get information
    about them.

    To get queue depths & anything else it needs to determine when/if to
    spawn/kill threads, it uses the HTTP interface of the RabbitMQ
    management plugin, so that's an external dependency.
    """
    def __init__(self, config):
        self._config = config['AMQP']['AutoScaling']
        mgmt_cfg = config['AMQP']['Management']
        self.min_workers = self._config['min_workers']
        self.max_workers= self._config['max_workers']
        self.mps_max = self._config['mps_max']
        self.max_err = self._config['max_err']
        self.spawn_depth = self._config['spawn_depth']
        self.requeue_on_err = self._config['requeue_on_err']
        self.broker = Server("%s:%s" % (mgmt_cfg['host'], mgmt_cfg['port']),
                             mgmt_cfg['user'], mgmt_cfg['password'])

        # Passed to workers
        self.worker_config = config['AMQP']

        # worker instances.
        self.workers = []
        self.worker_class = PikaClient

    def spawn_workers(self):
        """Starts a new worker, and registers it in self.workers.

        """
        try:
            new_worker = self.worker_class(self._config['AMQP'])
            new_worker.start()
        except Exception:
            raise
        else:
            self.workers.append(new_worker)
            logging.info("New worker added (%s workers)", len(self.workers))
            return True

    def reap_workers(self):
        """Loops over the workers and removes any that have died.

        """
        for worker in self.workers:
            if worker.is_alive():
                continue
            else:
                logging.info("Removing dead worker: %s", worker.name)
                self.workers.pop(self.workers.index(worker))
                logging.info("%s workers running", len(self.workers))

    def poll_depth(self, vhost, queue):
        """Asks RabbitMQ about queue depths. Used to determine if we need to
        spawn more workers. Returns an integer.

        """
        depth = self.broker.get_queue_depth(vhost, queue)
        return depth

    def start(self):
        """This is called by the end user's processor module to kick off
        the fireworks. It's basically this class's 'main()'.

        """

        worker = PikaClient(self.worker_config)
        logging.info("Starting a new worker")
        worker.start()
        logging.info("Worker started")

class PikaClient(multiprocessing.Process):

    def __init__(self, config):
        super(PikaClient, self).__init__()
        self.config = config
        # Default values
        self.connected = False
        self.connecting = False
        self.connection = None
        self.channel = None
        self.ioloop = tornado.ioloop.IOLoop.instance()
        # A place for us to keep messages sent to us by Rabbitmq
        self.messages = list()

        # A place for us to put pending messages while we're waiting to connect
        self.pending = list()
        self.handlers = set()

    def register_handler(self, handler):
        """
        handler is an instance of some class that processes messages.

        """
        logging.debug("register_handler called...")
        self.handlers.add(handler)

    def run(self):
        """
        Start consuming messages, dispatching them to any registered handlers.

        """
        for i in range(3):
            if os.fork() == 0:
                self.ioloop = tornado.ioloop.IOLoop.instance()
                logging.debug("Starting ioloop inside consume()")
                self.connect()
                self.ioloop.start()

    def dispatch(self, channel, method, header, body):
        logging.debug("CONSUMER: Got message: %s", body)
        logging.debug("Handlers available: %s", self.handlers)
        for handler in self.handlers:
            logging.debug("Routing to handler: %s", handler)
            handler.process(body)

    def connect(self):
        if self.connecting:
            logging.debug('PikaClient: Already connecting to RabbitMQ')
            return
        logging.debug('PikaClient: Connecting to RabbitMQ')
        self.connecting = True

        credentials = pika.PlainCredentials('guest', 'guest')
        param = pika.ConnectionParameters(credentials=credentials,
                                          **self.config['Connection'])
        logging.debug("Pika params: %s" % param)
        try:
            TornadoConnection(param, on_open_callback=self.on_connected)
        except Exception as out:
            logging.error("There was a connection problem: %s", out)


    def on_connected(self, connection):
        logging.debug('PikaClient: Connected to RabbitMQ')
        self.connected = True
        self.connection = connection
        self.connection.add_on_close_callback(self.on_closed)
        logging.debug("Setting self.channel now...")
        self.connection.channel(self.on_channel_open)


    def on_channel_open(self, channel):
        logging.debug('PikaClient: Channel Open, Declaring Exchange')
        self.channel = channel
        self.channel.exchange_declare(callback=self.on_exchange_declared,
                                      **self.config['Exchange'])

    def on_exchange_declared(self, frame):
        logging.debug('PikaClient: Exchange Declared, Declaring Queue')
        self.channel.queue_declare(callback=self.on_queue_declared,
                                   **self.config['Queue'])

    def on_queue_declared(self, frame):
        logging.debug('PikaClient: Queue Declared, Binding Queue')
        self.channel.queue_bind(exchange=self.config['Exchange']['exchange'],
                                queue=self.config['Queue']['queue'],
                                routing_key=self.config['Binding']['routing_key'],
                                callback=self.on_queue_bound)

    def on_queue_bound(self, frame):
        logging.debug('PikaClient: Queue Bound, Issuing Basic Consume!')
        self.channel.basic_consume(consumer_callback=self.dispatch,
                                   queue=self.config['Queue']['queue'],
                                   no_ack=True)

    def on_basic_cancel(self, frame):
        logging.debug('PikaClient: Basic Cancel Ok')
        self.connection.close()

    def on_closed(self, connection):
        # We've closed our pika connection so stop
        tornado.ioloop.IOLoop.instance().stop()
