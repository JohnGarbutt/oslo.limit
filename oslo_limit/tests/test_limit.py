# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""
test_limit
----------------------------------
Tests for `limit` module.
"""

import mock
import uuid

from openstack.identity.v3 import registered_limit
from oslotest import base

from oslo_limit import exception
from oslo_limit import limit


@mock.patch.object(limit, "_get_keystone_connection", new=mock.MagicMock)
class TestEnforcer(base.BaseTestCase):

    def setUp(self):
        super(TestEnforcer, self).setUp()

    def _get_usage_for_project(self, project_id, resource_names):
        return {name: 1 for name in resource_names}

    def test_required_parameters(self):
        enforcer = limit.Enforcer(self._get_usage_for_project)
        self.assertEqual(self._get_usage_for_project, enforcer.usage_callback)

    def test_usage_callback_must_be_callable(self):
        invalid_callback_types = [uuid.uuid4().hex, 5, 5.1]

        for invalid_callback in invalid_callback_types:
            self.assertRaises(
                ValueError,
                limit.Enforcer,
                invalid_callback
            )

    def test_deltas_must_be_a_dictionary(self):
        project_id = uuid.uuid4().hex
        invalid_delta_types = [uuid.uuid4().hex, 5, 5.1, True, False, []]
        enforcer = limit.Enforcer(self._get_usage_for_project)

        for invalid_delta in invalid_delta_types:
            self.assertRaises(
                ValueError,
                enforcer.enforce,
                project_id,
                invalid_delta
            )

    def test_project_id_must_be_a_string(self):
        enforcer = limit.Enforcer(self._get_usage_for_project)
        invalid_delta_types = [{}, 5, 5.1, True, False, [], None, ""]
        for invalid_project_id in invalid_delta_types:
            self.assertRaises(
                ValueError,
                enforcer.enforce,
                invalid_project_id)

    @mock.patch.object(limit._EnforcerUtils,
                       "get_limit")
    @mock.patch.object(limit._EnforcerUtils,
                       "get_all_registered_limit_resources")
    def test_enforce(self, mock_get_all, mock_limit):
        def fake_callback(proj_id, resources):
            return {
                "a": 5,
                "b": 10,
                "c": 0,
            }

        enforcer = limit.Enforcer(fake_callback)

        project_id = uuid.uuid4().hex
        enforcer.enforce(project_id)

        e = self.assertRaises(ValueError,
                              enforcer.enforce,
                              project_id, {"a": 8})
        self.assertEqual(str(e), "unexpected resource a in deltas")

        mock_get_all.return_value = ["a", "b", "c"]
        mock_limit.return_value = 10
        enforcer.enforce(project_id, {"a": 5})

        e = self.assertRaises(exception.ClaimExceedsLimit,
                              enforcer.enforce,
                              project_id, {"a": 6})
        self.assertEqual(str(e),
                         "5 a have been used. Claiming 6 a would exceed "
                         "the current limit of 10")

        mock_get_all.return_value = ["a", "b", "c", "d"]
        e = self.assertRaises(ValueError,
                              enforcer.enforce,
                              project_id, {"a": 5})
        self.assertEqual(str(e),
                         "missing resource counts for d")


class TestEnforcerUtils(base.BaseTestCase):
    def test_get_endpoint(self):
        mock_conn = mock.MagicMock()
        mock_conn.get_endpoint.return_value = "fake"

        utils = limit._EnforcerUtils(mock_conn)

        self.assertEqual("fake", utils.endpoint)
        mock_conn.get_endpoint.assert_called_once_with(None)

    def test_get_all_registered_limit_resources(self):
        mock_conn = mock.MagicMock()
        utils = limit._EnforcerUtils(mock_conn)

        mock_conn.registered_limits.return_value = []

        names = utils.get_all_registered_limit_resources()
        self.assertEqual([], names)

        foo = registered_limit.RegisteredLimit()
        foo.resource_name = "foo"
        mock_conn.registered_limits.return_value = [foo]

        names = utils.get_all_registered_limit_resources()
        self.assertEqual(["foo"], names)
