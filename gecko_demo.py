#!/usr/bin/env python2.7
"""
The intent here is to describe the usage model. This is gecko's
Hello World. A model to use in building your own amqp processors
using gecko.

"""
from pikaclient import PikaClient
import logging
import yaml

logging.basicConfig(level=logging.DEBUG)

class Processor(object):
    def __init__(self):
        """
        At some point this would take a config with resource configurations,
        like database connection setup and stuff.

        """
        pass

    def process(self, msg):
        """
        This demo uses one method for all parts of all messages.

        """
        print "HANDLER: received a msg: %s" % msg


if __name__ == "__main__":
    with open('test.yaml', 'r') as stream:
        config = yaml.load(stream)


    # create a consumer and processor.
    consumer = PikaClient(config['AMQP'])
    processor = Processor()

    # tell the consumer about the processor. A design decision: we could've
    # passed the processor to consumer's init, but this allows us to
    # register multiple handlers if we want to.
    consumer.register_handler(processor)

    # FIRE!
    consumer.run()



