# Copyright 2016 Nexenta Systems, Inc.
# All Rights Reserved.
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
"""
Unit tests for OpenStack Cinder volume driver
"""

from mock import call
from mock import Mock
from mock import patch
from six.moves import urllib

from cinder import context
from cinder import db
from cinder import exception
from cinder import test
from cinder.tests.unit.fake_volume import fake_volume_obj
from cinder.volume import configuration as conf
from cinder.volume.drivers.nexenta.ns5 import jsonrpc
from cinder.volume.drivers.nexenta.ns5 import nfs


class TestNexentaNfsDriver(test.TestCase):
    TEST_SHARE = 'host1:/pool/share'
    TEST_SHARE2_OPTIONS = '-o intr'
    TEST_FILE_NAME = 'test.txt'
    TEST_SHARES_CONFIG_FILE = '/etc/cinder/nexenta-shares.conf'
    TEST_SNAPSHOT_NAME = 'snapshot1'
    TEST_VOLUME_NAME = 'volume1'
    TEST_VOLUME_NAME2 = 'volume2'

    TEST_VOLUME = fake_volume_obj(None, **{
        'name': TEST_VOLUME_NAME,
        'id': '1',
        'size': 1,
        'status': 'available',
        'provider_location': TEST_SHARE
    })

    TEST_VOLUME2 = fake_volume_obj(None, **{
        'name': TEST_VOLUME_NAME2,
        'size': 2,
        'id': '2',
        'status': 'in-use'
    })

    TEST_SNAPSHOT = {
        'name': TEST_SNAPSHOT_NAME,
        'volume_name': TEST_VOLUME_NAME,
        'volume_size': 1,
        'volume_id': '1'
    }

    TEST_SHARE_SVC = 'svc:/network/nfs/server:default'

    def setUp(self):
        super(TestNexentaNfsDriver, self).setUp()
        self.ctxt = context.get_admin_context()
        self.cfg = Mock(spec=conf.Configuration)
        self.cfg.nexenta_dataset_description = ''
        self.cfg.nexenta_mount_point_base = '$state_path/mnt'
        self.cfg.nexenta_sparsed_volumes = True
        self.cfg.nexenta_dataset_compression = 'on'
        self.cfg.nexenta_dataset_dedup = 'off'
        self.cfg.nfs_mount_point_base = '/mnt/test'
        self.cfg.nfs_mount_attempts = 3
        self.cfg.nas_mount_options = 'vers=4'
        self.cfg.reserved_percentage = 20
        self.cfg.nexenta_use_https = False
        self.cfg.driver_ssl_cert_verify = False
        self.cfg.nexenta_rest_port = 0
        self.cfg.nexenta_user = 'user'
        self.cfg.nexenta_password = 'pass'
        self.cfg.max_over_subscription_ratio = 20.0
        self.cfg.nas_host = '1.1.1.2'
        self.cfg.nexenta_rest_address = '1.1.1.1'
        self.cfg.nas_share_path = 'pool/share'
        self.cfg.nfs_mount_options = None
        self.nef_mock = Mock()
        self.mock_object(jsonrpc, 'NexentaJSONProxy',
                         lambda *_, **__: self.nef_mock)
        self.drv = nfs.NexentaNfsDriver(configuration=self.cfg)
        self.drv.db = db
        self.drv.do_setup(self.ctxt)

    def _create_volume_db_entry(self):
        vol = {
            'id': '1',
            'size': 1,
            'status': 'available',
            'provider_location': self.TEST_SHARE
        }
        return db.volume_create(self.ctxt, vol)['id']

    def test_check_for_setup_error_raise(self):
        def get_side_effect(*args, **kwargs):
            if 'storage/pools/' in args[0]:
                return ''
            elif 'nas/nfs?filesystem=' in args[0]:
                return {'data': [{'shareState': 'offline'}]}
        self.nef_mock.get.side_effect = get_side_effect
        self.assertRaises(
            exception.NexentaException,
            lambda: self.drv.check_for_setup_error())

    def test_check_for_setup_error(self):
        def get_side_effect(*args, **kwargs):
            if 'storage/pools/' in args[0]:
                return ''
            elif 'nas/nfs?filesystem=' in args[0]:
                return {'data': [{'shareState': 'online'}]}
        self.nef_mock.get.side_effect = get_side_effect
        self.assertIsNone(self.drv.check_for_setup_error())

    @patch('cinder.volume.drivers.nexenta.ns5.nfs.'
           'NexentaNfsDriver._mount_volume')
    def test_initialize_connection(self, _mount_volume):
        data = {
            'export': self.TEST_VOLUME['provider_location'], 'name': 'volume'}
        self.assertEqual({
            'driver_volume_type': self.drv.driver_volume_type,
            'data': data,
            'mount_point_base': self.drv.nfs_mount_point_base
        }, self.drv.initialize_connection(self.TEST_VOLUME, None))
        url = 'hpr/activate'
        data = {'datasetName': '/'.join(
            [self.drv.share, self.TEST_VOLUME['name']])}
        self.nef_mock.post.assert_called_with(url, data)

    @patch('cinder.volume.drivers.nexenta.ns5.nfs.'
           'NexentaNfsDriver._mount_volume')
    @patch('cinder.volume.drivers.nexenta.ns5.nfs.'
           'NexentaNfsDriver._unmount_volume')
    @patch('cinder.volume.drivers.nexenta.ns5.nfs.'
           'NexentaNfsDriver.local_path')
    @patch('cinder.volume.drivers.nexenta.ns5.nfs.'
           'NexentaNfsDriver._create_regular_file')
    @patch('cinder.volume.drivers.nexenta.ns5.nfs.'
           'NexentaNfsDriver._create_sparsed_file')
    @patch('cinder.volume.drivers.nexenta.ns5.nfs.'
           'NexentaNfsDriver._ensure_share_mounted')
    @patch('cinder.volume.drivers.nexenta.ns5.nfs.'
           'NexentaNfsDriver._share_folder')
    def test_do_create_volume(self, share, ensure, sparsed, local,
                              regular, _unmount_volume, _mount_volume):
        local.return_value = 'path'
        ensure.return_value = True
        share.return_value = True
        self.nef_mock.get.return_value = 'on'
        self.drv._do_create_volume(self.TEST_VOLUME)

        url = 'storage/filesystems'
        data = {
            'path': 'pool/share/volume-1',
            'compressionMode': 'on',
        }
        self.nef_mock.post.assert_called_with(url, data)

    @patch('cinder.volume.drivers.nexenta.ns5.nfs.'
           'NexentaNfsDriver._unmount_volume')
    @patch('cinder.volume.drivers.nexenta.ns5.nfs.'
           'NexentaNfsDriver._ensure_share_mounted')
    def test_delete_volume(self, ensure, unmount):
        self._create_volume_db_entry()
        self.nef_mock.get.return_value = {'data': ['volume']}
        self.drv.delete_volume(self.TEST_VOLUME)
        path = '/'.join(['pool/share', self.TEST_VOLUME['name']])
        self.nef_mock.delete.assert_called_with(
            'storage/filesystems/%s?force=true&snapshots=true' % (
                urllib.parse.quote_plus(path)))

    def test_create_snapshot(self):
        self._create_volume_db_entry()
        self.drv.create_snapshot(self.TEST_SNAPSHOT)
        vol_path = '/'.join(['pool/share', self.TEST_VOLUME['name']])
        url = 'storage/snapshots'
        data = {'path': '%s@snapshot1' % vol_path}
        self.nef_mock.post.assert_called_with(url, data)

    def test_delete_snapshot(self):
        self._create_volume_db_entry()
        self.nef_mock.get.return_value = {'data': ['volume']}
        self.drv.delete_snapshot(self.TEST_SNAPSHOT)

        vol_path = '/'.join(['pool/share', self.TEST_VOLUME['name']])
        snap_path = urllib.parse.quote_plus(('%s@snapshot1' % vol_path))
        url = 'storage/snapshots/%s?defer=true' % snap_path
        self.nef_mock.delete.assert_called_with(url)

    @patch('cinder.volume.drivers.nexenta.ns5.nfs.'
           'NexentaNfsDriver.extend_volume')
    @patch('cinder.volume.drivers.nexenta.ns5.nfs.'
           'NexentaNfsDriver.local_path')
    @patch('cinder.volume.drivers.nexenta.ns5.nfs.'
           'NexentaNfsDriver._share_folder')
    def test_create_volume_from_snapshot(self, share, path, extend):
        self._create_volume_db_entry()
        path = '/'.join(['pool/share', self.TEST_VOLUME2['name']])
        data = {'targetPath': path}
        self.drv.create_volume_from_snapshot(
            self.TEST_VOLUME2, self.TEST_SNAPSHOT)
        source_vol_path = '/'.join(['pool/share', self.TEST_VOLUME['name']])
        snap_path = '%s@snapshot1' % urllib.parse.quote_plus(source_vol_path)
        url_1 = 'storage/snapshots/%s/clone' % snap_path
        url_2 = 'storage/filesystems/%s/unmount' % urllib.parse.quote_plus(
            path)
        url_3 = 'storage/filesystems/%s/mount' % urllib.parse.quote_plus(path)
        calls = [call(url_1, data), call(url_2), call(url_3)]
        self.nef_mock.post.assert_has_calls(calls)

        # make sure the volume get extended!
        extend.assert_called_once_with(self.TEST_VOLUME2, 2)

    @patch('cinder.volume.drivers.nexenta.ns5.nfs.'
           'NexentaNfsDriver._unmount_volume')
    @patch('cinder.volume.drivers.nexenta.ns5.nfs.'
           'NexentaNfsDriver._mount_volume')
    @patch('cinder.volume.drivers.nexenta.ns5.nfs.'
           'NexentaNfsDriver.local_path')
    @patch('oslo_concurrency.processutils.execute')
    def test_extend_volume_sparsed(
            self, _execute, path, mount, unmount):
        self._create_volume_db_entry()
        path.return_value = 'path'

        self.drv.extend_volume(self.TEST_VOLUME, 2)

        _execute.assert_called_with(
            'truncate', '-s', '2G',
            'path',
            root_helper='sudo cinder-rootwrap /etc/cinder/rootwrap.conf',
            run_as_root=True)

    @patch('cinder.volume.drivers.nexenta.ns5.nfs.'
           'NexentaNfsDriver._unmount_volume')
    @patch('cinder.volume.drivers.nexenta.ns5.nfs.'
           'NexentaNfsDriver._mount_volume')
    @patch('cinder.volume.drivers.nexenta.ns5.nfs.'
           'NexentaNfsDriver.local_path')
    @patch('oslo_concurrency.processutils.execute')
    def test_extend_volume_nonsparsed(
            self, _execute, path, mount, unmount):
        self._create_volume_db_entry()
        path.return_value = 'path'
        with patch.object(self.drv, 'sparsed_volumes', False):
            self.drv.extend_volume(self.TEST_VOLUME, 2)

            _execute.assert_called_with(
                'dd', 'if=/dev/zero', 'seek=1024',
                'of=path',
                'bs=1M', 'count=1024',
                root_helper='sudo cinder-rootwrap /etc/cinder/rootwrap.conf',
                run_as_root=True)

    def test_get_capacity_info(self):
        self.nef_mock.get.return_value = {
            'bytesAvailable': 1000,
            'bytesUsed': 100}
        self.assertEqual(
            (1100, 1000, 100), self.drv._get_capacity_info('pool/share'))
