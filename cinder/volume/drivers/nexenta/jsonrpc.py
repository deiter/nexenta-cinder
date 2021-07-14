# Copyright 2021 Nexenta by DDN, Inc. All rights reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import hashlib
import json

from eventlet import greenthread
from oslo_log import log as logging
import requests
import six
from six.moves import html_parser

from cinder import coordination
from cinder import exception
from cinder.i18n import _

LOG = logging.getLogger(__name__)

DEFAULT_REST_PORT = 8457
HTTPS = 'https'
HTTP = 'http'
AUTO = 'auto'
NFS = 'nfs'
ISCSI = 'iscsi'


class NmsException(exception.VolumeDriverException):

    def __init__(self, data=None, **kwargs):
        defaults = {
            'name': 'Nexenta Error',
            'code': 'EPROTO',
            'source': 'Nexenta Cinder Driver',
            'message': 'Unknown Error'
        }
        if isinstance(data, dict):
            for key in defaults:
                if key in kwargs:
                    continue
                if key in data:
                    kwargs[key] = data[key]
                else:
                    kwargs[key] = defaults[key]
        elif isinstance(data, six.string_types):
            if 'message' not in kwargs:
                kwargs['message'] = data
        for key in defaults:
            if key not in kwargs:
                kwargs[key] = defaults[key]
        message = ('%(message)s (source: %(source)s, '
                   'name: %(name)s, code: %(code)s)') % kwargs
        del kwargs['message']
        self.code = kwargs['code']
        super(NmsException, self).__init__(message, **kwargs)


class NmsParser(html_parser.HTMLParser, object):

    def __init__(self):
        super(NmsParser, self).__init__()
        self.data = []

    def handle_data(self, data):
        data = data.strip()
        data = data.replace('\n', ' ')
        if data:
            self.data.append(data)

    @property
    def text(self):
        return ' '.join(self.data)


class NmsRequest(object):

    def __init__(self, nms, method):
        self.nms = nms
        self.method = method

    @coordination.synchronized('{self.nms.proxy.lock}')
    def __call__(self, *args):
        timeout = self.nms.proxy.timeout
        attempts = self.nms.proxy.retries + 1
        if self.nms.proxy.auto:
            attempts *= 2
        payload = {
            self.nms.kind: self.nms.name,
            'method': self.method,
            'params': args
        }
        LOG.debug('NMS request start: post %(url)s %(payload)s',
                  {'url': self.nms.proxy.url, 'payload': payload})
        data = json.dumps(payload)
        message = None
        for attempt in range(attempts):
            if message:
                self.nms.proxy.delay(attempt, message)
                LOG.warning('NMS request retry %(attempt)s: post %(url)s '
                            '%(payload)s, reason: %(message)s ',
                            {'attempt': attempt, 'url': self.nms.proxy.url,
                             'payload': payload, 'message': message})
            try:
                response = self.nms.proxy.session.post(self.nms.proxy.url,
                                                       data=data,
                                                       timeout=timeout)
            except requests.exceptions.Timeout as error:
                message = error
                continue
            except requests.exceptions.SSLError as error:
                if self.nms.proxy.auto:
                    self.nms.proxy.scheme = 'http'
                    message = '%(error)s, failover to %(url)s' % {
                        'error': error,
                        'url': self.nms.proxy.url
                    }
                else:
                    message = '%(error)s, please check SSL options' % {
                        'error': error
                    }
                continue
            except requests.exceptions.ConnectionError as error:
                if self.nms.proxy.auto and self.nms.proxy.scheme == 'http':
                    self.nms.proxy.scheme = 'https'
                    message = '%(error)s, failover to %(url)s' % {
                        'error': error,
                        'url': self.nms.proxy.url
                    }
                else:
                    message = error
                continue

            LOG.debug('NMS request done: post %(url)s %(payload)s, '
                      'response status: %(status)s, response reason: '
                      '%(reason)s, response time: %(time)s seconds, '
                      'response content: %(content)s',
                      {'url': self.nms.proxy.url,
                       'payload': payload,
                       'status': response.status_code,
                       'reason': response.reason,
                       'time': response.elapsed.total_seconds(),
                       'content': response.content})

            if not response.content:
                message = 'no content %(status)s %(reason)s' % {
                    'status': response.status_code,
                    'reason': response.reason
                }
                continue

            try:
                content = response.json()
            except (TypeError, ValueError) as error:
                text = response.content
                try:
                    parser = NmsParser()
                    parser.feed(response.content)
                    text = parser.text
                except html_parser.HTMLParseError:
                    pass
                message = 'failed to parse JSON %(text)s: %(error)s' % {
                    'text': text,
                    'error': error
                }
                continue

            if not (response.ok and isinstance(content, dict)):
                message = '%(status)s %(reason)s %(content)s' % {
                    'status': response.status_code,
                    'reason': response.reason,
                    'content': content
                }
                continue

            if 'error' in content and content['error'] is not None:
                error = content['error']
                if isinstance(error, dict) and 'message' in error:
                    message = error['message']
                else:
                    message = error
                if self.check_error(message):
                    continue

            if 'result' in content and content['result'] is not None:
                result = content['result']
            else:
                result = None

            LOG.debug('NMS request result for %(payload)s: %(result)s',
                      {'payload': payload, 'result': result})

            return result
        raise NmsException(code='EAGAIN', message=message)

    def check_error(self, message):
        source = self.nms.proxy.url
        if 'already exists' in message:
            code = 'EEXIST'
        elif 'already configured' in message:
            code = 'EEXIST'
        elif 'has children' in message:
            code = 'EEXIST'
        elif 'in use' in message:
            code = 'EBUSY'
        elif 'does not exist' in message:
            code = 'ENOENT'
        elif 'not receive a reply' in message:
            code = 'EAGAIN'
        else:
            code = 'EPROTO'
        ignored = {
            'EEXIST': [
                'add_hostgroup_member',
                'add_lun_mapping_entry',
                'add_targetgroup_member',
                'clone',
                'create',
                'create_hostgroup',
                'create_lu',
                'create_snapshot',
                'create_target',
                'create_targetgroup',
                'create_with_props'
            ],
            'ENOENT': [
                'delete_lu',
                'destroy',
                'remove_lun_mapping_entry'
            ]
        }
        if code in ignored and self.method in ignored[code]:
            LOG.debug('Ignore %(kind)s %(name)s %(method)s error: %(error)s',
                      {'kind': self.nms.kind, 'name': self.nms.name,
                       'method': self.method, 'error': message})
            return False
        elif code == 'EAGAIN':
            return True
        raise NmsException(code=code, source=source, message=message)


class NmsObject(object):

    def __init__(self, proxy, name):
        self.kind = 'object'
        self.proxy = proxy
        self.name = name
        plugins = {
            'autosync_plugin': 'nms-autosync',
            'rrdaemon_plugin': 'nms-rrdaemon',
            'rsf_plugin': 'nms-rsf-cluster'
        }
        if name in plugins:
            self.kind = 'plugin'
            self.name = plugins[name]

    def __getattr__(self, method):
        return NmsRequest(self, method)


class NmsProxy(object):

    def __init__(self, proto, path, conf):
        self.path = path
        self.lock = 'nms'
        self.auto = False
        self.scheme = conf.nexenta_rest_protocol
        if self.scheme == AUTO:
            self.auto = True
            self.scheme = HTTP
        if conf.nexenta_rest_address:
            self.host = conf.nexenta_rest_address
        elif proto == ISCSI and conf.nexenta_host:
            self.host = conf.nexenta_host
        elif proto == NFS and conf.nas_host:
            self.host = conf.nas_host
        else:
            message = (_('NexentaStor Rest API address is not defined, '
                         'please check the Cinder configuration file for '
                         'NexentaStor backend and nexenta_rest_address, '
                         'nexenta_host or nas_host configuration options'))
            raise NmsException(code='EINVAL', message=message)
        if conf.nexenta_rest_port:
            self.port = conf.nexenta_rest_port
        else:
            self.port = DEFAULT_REST_PORT
        self.headers = {
            'Content-Type': 'application/json',
            'X-Requested-With': 'XMLHttpRequest'
        }
        self.retries = conf.nexenta_rest_retry_count
        self.backoff_factor = conf.nexenta_rest_backoff_factor
        self.timeout = (conf.nexenta_rest_connect_timeout,
                        conf.nexenta_rest_read_timeout)
        self.session = requests.Session()
        self.session.auth = (conf.nexenta_user, conf.nexenta_password)
        self.session.verify = conf.driver_ssl_cert_verify
        if self.session.verify and conf.driver_ssl_cert_path:
            self.session.verify = conf.driver_ssl_cert_path
        self.session.headers.update(self.headers)
        if not conf.driver_ssl_cert_verify:
            requests.packages.urllib3.disable_warnings()
        self.update_lock()

    def __getattr__(self, name):
        return NmsObject(self, name)

    def update_lock(self):
        signature = None
        license_info = {}
        try:
            license_info = self.appliance.get_license_info()
        except NmsException as error:
            LOG.error('Unable to get license information: %(error)s',
                      {'error': error})
        key = 'machine_sig'
        if isinstance(license_info, dict) and key in license_info:
            signature = license_info[key]
            LOG.debug('Host signature: %(signature)s',
                      {'signature': signature})
        plugins = {}
        try:
            plugins = self.plugin.get_names('')
        except NmsException as error:
            LOG.error('Unable to get installed plugins: %(error)s',
                      {'error': error})
        plugin = 'nms-rsf-cluster'
        if isinstance(plugins, list) and plugin in plugins:
            names = []
            try:
                names = self.rsf_plugin.get_names('')
            except NmsException as error:
                LOG.error('Unable to get HA cluster name: %(error)s',
                          {'error': error})
            if isinstance(names, list) and len(names) == 1:
                name = names.pop()
                conf = {}
                try:
                    conf = self.rsf_plugin.get_child_props(name, '')
                except NmsException as error:
                    LOG.error('Unable to get HA cluster %(name)s '
                              'configuration: %(error)s',
                              {'name': name, 'error': error})
                key = 'machinesigs'
                if isinstance(conf, dict) and key in conf:
                    data = conf[key]
                    nodes = {}
                    try:
                        nodes = json.loads(data)
                    except (TypeError, ValueError) as error:
                        LOG.error('Unable to parse HA cluster %(name)s '
                                  'configuration %(data)s: %(error)s',
                                  {'name': name, 'data': data,
                                   'error': error})
                    if nodes and isinstance(nodes, dict):
                        signatures = nodes.values()
                        if signature and signature in signatures:
                            signature = ':'.join(sorted(signatures))
                            LOG.debug('HA cluster %(name)s hosts '
                                      'signatures: %(signatures)s',
                                      {'name': name,
                                       'signatures': signatures})
                        else:
                            LOG.debug('HA cluster %(name)s configuration '
                                      '%(conf)s does not contain signature '
                                      'for configured host %(host)s',
                                      {'name': name, 'conf': conf,
                                       'host': self.host})
                    else:
                        LOG.debug('HA cluster %(name)s configuration %(conf)s '
                                  'does not contain valid hosts signatures',
                                  {'name': name, 'conf': conf})
                else:
                    LOG.debug('Hosts signatures unavailable for HA cluster '
                              '%(name)s and cluster configuration %(conf)s',
                              {'name': name, 'conf': conf})
            else:
                LOG.debug('HA cluster plugin %(plugin)s is not configured',
                          {'plugin': plugin})
        else:
            LOG.debug('HA cluster plugin %(plugin)s is not installed',
                      {'plugin': plugin})
        if not signature:
            signature = self.host
        lock = '%s:%s' % (signature, self.path)
        if isinstance(lock, six.text_type):
            lock = lock.encode('utf-8')
        self.lock = hashlib.md5(lock).hexdigest()
        LOG.debug('NMS coordination lock: %(lock)s',
                  {'lock': self.lock})

    @property
    def url(self):
        return '%s://%s:%s/rest/nms' % (self.scheme, self.host, self.port)

    def delay(self, attempt, reason):
        if self.retries > 0:
            attempt %= self.retries
            if attempt == 0:
                attempt = self.retries
        interval = float(self.backoff_factor * (2 ** (attempt - 1)))
        LOG.debug('Waiting for %(interval)s seconds, reason: %(reason)s',
                  {'interval': interval, 'reason': reason})
        greenthread.sleep(interval)
