# -*- coding: utf-8 -*-

from tornado import ioloop, gen
import sqs
import time
import hashlib
import os

service = sqs.SQSService(os.getenv('AWS_PUBLIC'),
    os.getenv('AWS_PRIVATE'),
    os.getenv('AWS_ACCOUNT'),
    os.getenv('AWS_SERVER'))

h = hashlib.sha1()
for value in os.uname():
    h.update(value)
h.update('%.6f' % time.time())
test_hash = 'test-%s' % h.hexdigest()

@gen.engine
def test_suite():
    r = yield gen.Task(service.create_queue, '%s-a' % test_hash)
    r = yield gen.Task(service.create_queue, '%s-b' % test_hash)
    r = yield gen.Task(service.list_queues, test_hash)
    assert len(r['queues']) == 2
    assert [q.split('/')[-1] for q in sorted(r['queues'])] == ['%s-a' % test_hash,
                                                               '%s-b' % test_hash]
    r = yield gen.Task(service.delete_queue, '%s-b' % test_hash)
    r = yield gen.Task(service.list_queues, test_hash)
    r = yield gen.Task(service.get_queue_attributes,
                         '%s-a' % test_hash, ['VisibilityTimeout',
                                              'ApproximateNumberOfMessages'])
    timeout = int(r['values']['VisibilityTimeout']) + 1
    r = yield gen.Task(service.set_queue_attributes,
                         '%s-a' % test_hash, 'VisibilityTimeout', timeout)
    r = yield gen.Task(service.get_queue_attributes,
                         '%s-a' % test_hash, ['VisibilityTimeout',
                                              'ApproximateNumberOfMessages'])
    assert int(r['values']['VisibilityTimeout']) == timeout
    message = "._-~ ''X{}[]()ö"
    r = yield gen.Task(service.send_message, '%s-a' % test_hash, message, 0)
    r = yield gen.Task(service.receive_message, '%s-a' % test_hash)

    if r:
        assert r['body'] == message
        #r = yield gen.Task(service.change_message_visibility,
        #                   '%s-a' % test_hash, r['receipt_handle'], 43200)
        r = yield gen.Task(service.delete_message, '%s-a' % test_hash, r['receipt_handle'])

    #print yield gen.Task(service.add_permission,
    #                     '%s-a' % test_hash,'test', '512768672994', 'SendMessage')
    #print yield gen.Task(service.remove_permission, '%s-a' % test_hash, 'test')
    yield gen.Task(service.delete_queue, '%s-a' % test_hash)
    ioloop.IOLoop.instance().stop()

test_suite()

try:
    ioloop.IOLoop.instance().start()
except:
    ioloop.IOLoop.instance().stop()
