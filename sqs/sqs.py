#
# Copyright(c) 2012, BitTorrent Inc.
# All rights reserved.
#

import hmac
import hashlib
import base64
import functools
import urllib
import logging

from datetime import datetime
from tornado import httpclient
from tornado.escape import url_escape
from urllib import quote
from lxml import etree

###########
# Helpers #
###########

def get_xml_node_value(xml, node_name):
    result = xml.xpath("//*[local-name() = '%s']/text()" % node_name)
    if len(result) >= 1:
        return result[0].strip()
    return None

def get_xml_node_values(xml, node_name):
    results = xml.xpath("//*[local-name() = '%s']/text()" % node_name)
    return [r.strip() for r in results]

def cb(*args):
    pass

##############
# Exceptions #
##############

class APIMismatchException (Exception):
    def __init__(self, version):
        message = 'action not defined in SQS API: %s' % version
        super(APIMismatchException, self).__init__(message)

class ActionErrorException(Exception):
    def __init__(self, error):
        message = 'SQS action returned error 400: %s' % error
        super(ActionErrorException, self).__init__(message)

###########
# Service #
###########

class SQSService:
    """Amazon SQS service object"""

    VERSIONS = ['2011-10-01', '2009-02-01', '2008-01-01']
    ENDPOINTS = ['sqs.us-east-1.amazonaws.com',
                 'sqs.us-east-1.amazonaws.com',
                 'sqs.eu-west-1.amazonaws.com',
                 'sqs.us-west-1.amazonaws.com',
                 'sqs.us-west-2.amazonaws.com',
                 'sqs.sa-east-1.amazonaws.com',
                 'sqs.ap-northeast-1.amazonaws.com',
                 'sqs.ap-southeast-1.amazonaws.com',
                 'queue.amazonaws.com']

    class AwsMultipleParameterContainer(dict):
        """Build a parameters list as required by Amazon"""
        def __setitem__(self, key, value):
            if isinstance(value, basestring):
                value = [value]
            for i in range(1, len(value) + 1):
                dict.__setitem__(self, '%s.%d' % (key, i), value[i - 1])

    def __init__(self, access_key, secret_id, account_num, endpoint, version=VERSIONS[0]):
        self._access_key = access_key
        self._secret_id = secret_id
        self._account_num = account_num
        if version not in self.VERSIONS:
            raise Exception('unknown SQS API version provided: %s' % version)
        self._version = version
        if endpoint not in self.ENDPOINTS:
            raise Exception('unknown SQS region endpoint provided: %s' % endpoint)
        self._endpoint = endpoint
        self.http = httpclient.AsyncHTTPClient()

    def _sign(self, message):
        """Sign an AWS request"""
        signed_hash = hmac.new(key=self._secret_id, msg=message, digestmod=hashlib.sha256)
        return base64.b64encode(signed_hash.digest()).decode()

    def _url_encode(self, params):
        return '&'.join(['%s=%s' % (quote(str(k), safe="~"),
                                    quote(str(params[k]), safe="~"))
                                    for k in sorted(params.keys())])

    def _sign_action(self, method, path, params):
        string = '%s\n%s\n%s\n' % (method, self._endpoint, path)
        return self._sign(string + self._url_encode(params))

    def _call(self, command, callback, params={}, name=None):
        """Make a call to SQS"""

        now = datetime.utcnow()
        signstamp = now.strftime('%a, %d %b %Y %H:%M:%S GMT')
        timestamp = now.strftime('%Y-%m-%dT%H:%M:%SZ')
        url = 'https://%s' % self._endpoint
        method = 'POST'

        complement = ''
        if name:
            if self._version in self.VERSIONS[0:2]:
                complement += '/%s' % self._account_num
            complement += '/%s' % name
        url += complement

        params = dict(v for v in params.iteritems() if v[1] is not None)
        params['Action'] = command
        params['Version'] = self._version
        params['AWSAccessKeyId'] = self._access_key
        params['Timestamp'] = timestamp
        params['SignatureVersion'] = 2
        params['SignatureMethod'] = 'HmacSHA256'
        path = complement if len(complement) > 0 else '/'
        params['Signature'] = self._sign_action(method, path, params)

        headers = { 'Content-Type': 'application/x-www-form-urlencoded',
                    'Date': signstamp,
                    'X-Amzn-Authorization':
                        'AWS3-HTTPS AWSAccessKeyId=%s, '
                        'Algorithm=HMACSHA256, '
                        'Signature=%s' % (self._access_key,
                                          self._sign(signstamp)) }

        def callback_(response):
            if response.code == 200 and not response.error:
                callback(etree.XML(response.body))
            elif response.code == 400:
                raise ActionErrorException(response.body)
            else:
                raise response.error

        self.http.fetch(url, callback_, headers=headers, method=method,
                        body=self._url_encode(params))

    def create_queue(self, queue_name, attributes={}, callback=cb):
        params = { 'QueueName': queue_name }

        i = 1
        for k in attributes:
            prefix = 'Attribute.%d' % i
            params['%s.Name'  % prefix] = k
            params['%s.Value' % prefix] = attributes[k]
            i += 1

        def callback_(xml):
            callback({ 'queue_url':  get_xml_node_value(xml, 'QueueUrl'),
                       'request_id': get_xml_node_value(xml, 'RequestId') })

        result = self._call('CreateQueue', callback_, params)

    def list_queues(self, queue_name_prefix, callback=cb):
        params = { 'QueueNamePrefix': queue_name_prefix }

        def callback_(xml):
            callback({ 'queues':     get_xml_node_values(xml, 'QueueUrl'),
                       'request_id': get_xml_node_value (xml, 'RequestId') })

        self._call('ListQueues', callback_, params)

    def delete_queue(self, queue_name, callback=cb):
        def callback_(xml):
            callback({ 'request_id': get_xml_node_value(xml, 'RequestId') })

        self._call('DeleteQueue', callback_, name=queue_name)

    def get_queue_attributes(self, queue_name, attributes=[], callback=cb):
        multiple = self.AwsMultipleParameterContainer()
        multiple['AttributeName'] = attributes

        def callback_(xml):
            callback({ 'values': dict(zip(get_xml_node_values(xml, 'Name'),
                                          get_xml_node_values(xml, 'Value'))),
                       'request_id':      get_xml_node_value (xml, 'RequestId') })

        params = dict(multiple)
        self._call('GetQueueAttributes', callback_, params, queue_name)

    def set_queue_attributes(self, queue_name, attribute_name, attribute_value, callback=cb):
        params = { 'Attribute.Name': attribute_name, 'Attribute.Value': attribute_value }

        def callback_(xml):
            callback({ 'request_id': get_xml_node_value(xml, 'RequestId') })

        self._call('SetQueueAttributes', callback_, params, queue_name)

    def send_message(self, queue_name, message_body, delay_seconds, callback=cb):
        params = { 'MessageBody': base64.b64encode(message_body),
                   'DelaySeconds': delay_seconds }

        def callback_(xml):
            callback({ 'md5_of_message_body': get_xml_node_value(xml, 'MD5OfMessageBody'),
                       'message_id':          get_xml_node_value(xml, 'MessageId'),
                       'request_id':          get_xml_node_value(xml, 'RequestId') })

        self._call('SendMessage', callback_, params, queue_name)

    def receive_message(self, queue_name, max_number_of_messages=None,
                        visibility_timeout=None, attributes=[], callback=cb):
        single = { 'MaxNumberOfMessages': max_number_of_messages,
                   'VisibilityTimeout': visibility_timeout }

        multiple = self.AwsMultipleParameterContainer()
        multiple['Attribute'] = attributes
        params = dict(single, **multiple)

        def callback_(xml):
            response = { 'message_id':     get_xml_node_value(xml, 'MessageId'),
                         'receipt_handle': get_xml_node_value(xml, 'ReceiptHandle'),
                         'md5_of_body':    get_xml_node_value(xml, 'MD5OfBody'),
                         'body':           get_xml_node_value(xml, 'Body'),
                         'request_id':     get_xml_node_value(xml, 'RequestId') }

            if response['message_id']:
                response['body'] = base64.b64decode(response['body'])
                callback(response)
            else:
                callback(None)

        self._call('ReceiveMessage', callback_, params, queue_name)

    def delete_message(self, queue_name, receipt_handle, callback=cb):
        params = { 'ReceiptHandle': receipt_handle }

        def callback_(xml):
            callback({ 'request_id': get_xml_node_value(xml, 'RequestId') })

        self._call('DeleteMessage', callback_, params, queue_name)

    def add_permission(self, queue_name, label, aws_account_id, action_name, callback=cb):
        if self._version == self.VERSIONS[2]:
            raise APIMismatchException(self._version)

        single = { 'Label': label }
        multiple = self.AwsMultipleParameterContainer()
        multiple['AWSAccountId'] = aws_account_id
        multiple['ActionName'] = action_name
        params = dict(single, **multiple)

        def callback_(xml):
            callback({ 'request_id': get_xml_node_value(xml, 'RequestId') })

        self._call('AddPermission', callback_, params, queue_name)

    def remove_permission(self, queue_name, label, callback=cb):
        if self._version == self.VERSIONS[2]:
            raise APIMismatchException(self._version)

        params = { 'Label': label }

        def callback_(xml):
            callback({ 'request_id': get_xml_node_value(xml, 'RequestId') })

        self._call('RemovePermission', callback_, params, queue_name)

    def change_message_visibility(self, queue_name, receipt_handle, visibility_timeout, callback=cb):
        if self._version == self.VERSIONS[2]:
            raise APIMismatchException(self._version)

        params = { 'ReceiptHandle': receipt_handle, 'VisibilityTimeout': visibility_timeout }

        def callback_(xml):
            callback({ 'request_id': get_xml_node_value(xml, 'RequestId') })

        self._call('ChangeMessageVisibility', callback_, params, queue_name)

    def get_queue_url(self, queue_name, queue_owner_aws_account_id=None, callback=cb):
        if self._version == self.VERSIONS[1:3]:
            raise APIMismatchException(self._version)

        params = { 'QueueName': queue_name,
                   'QueueOwnerQWSAccountId': queue_owner_aws_account_id }

        def callback_(xml):
            callback({ 'request_id': get_xml_node_value(xml, 'RequestId'),
                       'queue_url':  get_xml_node_value(xml, 'QueueUrl') })

        self._call('GetQueueUrl', callback_, params)

    def send_message_batch(self, queue_name, messages, callback=cb):
        if self._version == self.VERSIONS[1:3]:
            raise APIMismatchException(self._version)

        params = {}
        for i in range(len(metestssages)):
            prefix = 'SendMessageBatchRequestEntry.%d' % (i + 1)
            params.update(
                zip(('%s.Id' % prefix,
                     '%s.MessageBody' % prefix,
                     '%s.DelaySeconds' % prefix),
                    message[i])
            )

        # FIXME: retrun a list of results rather than the XML result
        self._call('SendMessageBatch', callback, params, queue_name)

    def delete_message_batch(self, queue_name, messages, callback=cb):
        if self._version == self.VERSIONS[1:3]:
            raise APIMismatchException(self._version)

        params = {}
        for i in range(len(messages)):
            prefix = 'DeleteMessageBatchRequestEntry.%d' % (i + 1)
            params.update(
                zip(('%s.Id' % prefix, '%s.ReceiptHandle' % prefix),
                    message[i])
            )

        # FIXME: retrun a list of results rather than the XML result
        self._call('DeleteMessageBatch', callback, params, queue_name)

    def change_message_visibility_batch(self, queue_name, messages, callback=cb):
        if self._version == self.VERSIONS[1:3]:
            raise APIMismatchException(self._version)

        params = {}
        for i in range(len(messages)):
            prefix = 'ChangeMessageVisibilityBatchRequestEntry.%d' % (i + 1)
            params.update(
                zip(('%s.Id' % prefix,
                     '%s.ReceiptHandle' % prefix,
                     '%s.VisibilityTimeout' % prefix),
                    message[i])
            )

        # FIXME: retrun a list of results rather than the XML result
        self._call('ChangeMessageVisibilityBatch', callback, params, queue_name)
