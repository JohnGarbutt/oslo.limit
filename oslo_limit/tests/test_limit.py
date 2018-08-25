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

from oslo_config import cfg
from oslo_config import fixture as config_fixture
from oslotest import base

from oslo_limit import exception
from oslo_limit import limit
from oslo_limit import opts

CONF = cfg.CONF


class TestClaim(base.BaseTestCase):

    def test_required_parameters(self):
        resource_name = uuid.uuid4().hex
        quantity = 1

        claim = limit.Claim(resource_name, quantity)

        self.assertEqual(resource_name, claim.resource_name)
        self.assertEqual(quantity, claim.quantity)

    def test_resource_name_must_be_a_string(self):
        quantity = 1
        invalid_resource_name_types = [
            True, False, [uuid.uuid4().hex], {'key': 'value'}, 1, 1.2
        ]

        for invalid_resource_name in invalid_resource_name_types:
            self.assertRaises(
                ValueError,
                limit.Claim,
                invalid_resource_name,
                quantity
            )

    def test_quantity_must_be_an_integer(self):
        resource_name = uuid.uuid4().hex
        invalid_quantity_types = ['five', 5.5, [5], {5: 5}]

        for invalid_quantity in invalid_quantity_types:
            self.assertRaises(
                ValueError,
                limit.Claim,
                resource_name,
                invalid_quantity
            )


class TestEnforcer(base.BaseTestCase):

    def setUp(self):
        super(TestEnforcer, self).setUp()
        self.project_id = uuid.uuid4().hex
        self.quantity = 10
        self.claim = limit.Claim('cores', self.quantity)

        self.config_fixture = self.useFixture(config_fixture.Config(CONF))
        self.config_fixture.config(
            group='oslo_limit',
            auth_type='password')
        opts.register_opts(CONF)
        self.config_fixture.config(
            group='oslo_limit',
            auth_url='http://www.fake_url')

        limit._SDK_CONNECTION = mock.MagicMock()

    def _get_usage_for_project(self, project_id, claims):
        return {'cores': 8}

    def _get_multi_usage_for_project(self, project_id, claims):
        return {'cores': 8, 'memory': 512}

    def test_required_parameters(self):
        callback = self._get_usage_for_project
        enforcer = limit.Enforcer(self.claim, self.project_id, callback)

        self.assertEqual([self.claim], enforcer.claims)
        self.assertEqual(self.project_id, enforcer.project_id)
        self.assertEqual(self._get_usage_for_project, enforcer.callback)

    def test_optional_parameters(self):
        callback = self._get_usage_for_project
        enforcer = limit.Enforcer(self.claim, self.project_id, callback,
                                  verify=False)

        self.assertEqual([self.claim], enforcer.claims)
        self.assertEqual(self.project_id, enforcer.project_id)
        self.assertEqual(self._get_usage_for_project, enforcer.callback)
        self.assertFalse(enforcer.verify)

    def test_callback_must_be_callable(self):
        invalid_callback_types = [uuid.uuid4().hex, 5, 5.1]

        for invalid_callback in invalid_callback_types:
            self.assertRaises(
                ValueError,
                limit.Enforcer,
                self.claim,
                self.project_id,
                invalid_callback
            )

    def test_verify_must_be_boolean(self):
        invalid_verify_types = [uuid.uuid4().hex, 5, 5.1]

        for invalid_verify in invalid_verify_types:
            self.assertRaises(
                ValueError,
                limit.Enforcer,
                self.claim,
                self.project_id,
                self._get_usage_for_project,
                verify=invalid_verify
            )

    def test_claim_must_be_an_instance_of_project_claim(self):
        invalid_claim_types = [uuid.uuid4().hex, 5, 5.1, True, False, [], {}]

        for invalid_claim in invalid_claim_types:
            self.assertRaises(
                ValueError,
                limit.Enforcer,
                invalid_claim,
                self.project_id,
                self._get_usage_for_project,
            )

    def _create_generator(self, limit_list):
        class FakeLimit(object):
            def __init__(self, resource_limit=None, default_limit=None):
                self.resource_limit = resource_limit
                self.default_limit = default_limit

        return (FakeLimit(n.get('resource_limit'),
                          n.get('default_limit')) for n in limit_list)

    def test_call_enforce_success(self):
        callback = self._get_usage_for_project
        enforcer = limit.Enforcer(self.claim, self.project_id, callback)
        enforcer._connection.limits.return_value = self._create_generator(
            [{'resource_limit': 20}])

        # 20(limit) > 10(quantity) + 8(usage), so enforce success.
        enforcer.enforce()

    def test_call_enforce_fail(self):
        callback = self._get_usage_for_project
        enforcer = limit.Enforcer(self.claim, self.project_id, callback)
        enforcer._connection.limits.return_value = self._create_generator(
            [{'resource_limit': 10}])
        # 10(limit) < 10(quantity) + 8(usage), enforce fail.
        self.assertRaises(exception.ClaimExceedsLimit, enforcer.enforce)

    def test_call_enforce_with_registered_limit_success(self):
        callback = self._get_usage_for_project
        enforcer = limit.Enforcer(self.claim, self.project_id, callback)
        enforcer._connection.limits.return_value = self._create_generator([])
        enforcer._connection.registered_limits.return_value = (
            self._create_generator([{'default_limit': 20}]))
        # 20(registered_limit) > 10(quantity) + 8(usage), enforce success.
        enforcer.enforce()

    def test_call_enforce_with_registered_limit_fail(self):
        callback = self._get_usage_for_project
        enforcer = limit.Enforcer(self.claim, self.project_id, callback)
        enforcer._connection.limits.return_value = self._create_generator([])
        enforcer._connection.registered_limits.return_value = (
            self._create_generator([{'default_limit': 15}]))
        # 15(registered_limit) < 10(quantity) + 8(usage), enforce fail.
        self.assertRaises(exception.ClaimExceedsLimit, enforcer.enforce)

    def test_call_enforce_with_no_limit(self):
        callback = self._get_usage_for_project
        enforcer = limit.Enforcer(self.claim, self.project_id, callback)
        enforcer._connection.limits.return_value = self._create_generator([])
        enforcer._connection.registered_limits.return_value = (
            self._create_generator([]))
        self.assertRaises(exception.LimitNotFound, enforcer.enforce)

    def test_enforce_multi_claim(self):
        claim1 = limit.Claim('cores', 10)
        claim2 = limit.Claim('memory', 1024)
        callback = self._get_multi_usage_for_project
        enforcer = limit.Enforcer([claim1, claim2], self.project_id, callback)
        enforcer._connection.limits.return_value = self._create_generator(
            [{'resource_limit': 20}, {'resource_limit': 2048}])
        # 20(limit) > 10(quantity) + 8(usage),
        # 2048(limit) > 1024(quantity) + 512(usage), so enforce success.
        enforcer.enforce()

    def test_enforce_multi_claim_fail(self):
        claim1 = limit.Claim('cores', 10)
        claim2 = limit.Claim('memory', 1024)
        callback = self._get_multi_usage_for_project
        enforcer = limit.Enforcer([claim1, claim2], self.project_id, callback)
        enforcer._connection.limits.return_value = self._create_generator(
            [{'resource_limit': 20}, {'resource_limit': 1024}])
        # 20(limit) > 10(quantity) + 8(usage), but
        # 1024(limit) < 1024(quantity) + 512(usage), so enforce fail.
        self.assertRaises(exception.ClaimExceedsLimit, enforcer.enforce)
