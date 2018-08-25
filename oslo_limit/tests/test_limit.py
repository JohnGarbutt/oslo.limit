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


class TestProjectClaim(base.BaseTestCase):

    def test_required_parameters(self):
        project_id = uuid.uuid4().hex

        claim = limit.ProjectClaim(project_id)
        self.assertEqual(project_id, claim.project_id)

    def test_add_resource_to_claim(self):
        resource_name = uuid.uuid4().hex
        project_id = uuid.uuid4().hex
        quantity = 10

        claim = limit.ProjectClaim(project_id)
        claim.add_resource(resource_name, quantity)

        self.assertEqual(project_id, claim.project_id)
        self.assertIn(resource_name, claim.claims.keys())
        self.assertEqual(quantity, claim.claims[resource_name])

    def test_add_multiple_resource_to_claim(self):
        project_id = uuid.uuid4().hex

        claim = limit.ProjectClaim(project_id)
        claim.add_resource(uuid.uuid4().hex, 10)
        claim.add_resource(uuid.uuid4().hex, 5)

        self.assertEqual(project_id, claim.project_id)
        self.assertTrue(len(claim.claims) == 2)

    def test_resource_name_must_be_a_string(self):
        project_id = uuid.uuid4().hex
        invalid_resource_name_types = [
            True, False, [uuid.uuid4().hex], {'key': 'value'}, 1, 1.2
        ]

        claim = limit.ProjectClaim(project_id)
        for invalid_resource_name in invalid_resource_name_types:
            self.assertRaises(
                ValueError,
                claim.add_resource,
                invalid_resource_name,
                10
            )

    def test_project_id_must_be_a_string(self):
        invalid_project_id_types = [
            True, False, [uuid.uuid4().hex], {'key': 'value'}, 1, 1.2
        ]

        for invalid_project_id in invalid_project_id_types:
            self.assertRaises(
                ValueError,
                limit.ProjectClaim,
                invalid_project_id
            )

    def test_quantity_must_be_an_integer(self):
        project_id = uuid.uuid4().hex
        invalid_quantity_types = ['five', 5.5, [5], {5: 5}]

        claim = limit.ProjectClaim(project_id)
        for invalid_quantity in invalid_quantity_types:
            self.assertRaises(
                ValueError,
                claim.add_resource,
                uuid.uuid4().hex,
                invalid_quantity
            )


class TestEnforcer(base.BaseTestCase):

    def setUp(self):
        super(TestEnforcer, self).setUp()
        self.resource_name = uuid.uuid4().hex
        self.project_id = uuid.uuid4().hex
        self.quantity = 10
        self.claim = limit.ProjectClaim(self.project_id)
        self.claim.add_resource(self.resource_name, self.quantity)

        self.config_fixture = self.useFixture(config_fixture.Config(CONF))
        self.config_fixture.config(
            group='oslo_limit',
            auth_type='password')
        opts.register_opts(CONF)
        self.config_fixture.config(
            group='oslo_limit',
            auth_url='http://www.fake_url')

        limit._SDK_CONNECTION = mock.MagicMock()

    def _get_usage_for_project(self, project_id):
        return 8

    def test_required_parameters(self):
        enforcer = limit.Enforcer(self.claim)

        self.assertEqual(self.claim, enforcer.claim)
        self.assertIsNone(enforcer.callback)
        self.assertTrue(enforcer.verify)

    def test_optional_parameters(self):
        callback = self._get_usage_for_project
        enforcer = limit.Enforcer(self.claim, callback=callback, verify=True)

        self.assertEqual(self.claim, enforcer.claim)
        self.assertEqual(self._get_usage_for_project, enforcer.callback)
        self.assertTrue(enforcer.verify)

    def test_callback_must_be_callable(self):
        invalid_callback_types = [uuid.uuid4().hex, 5, 5.1]

        for invalid_callback in invalid_callback_types:
            self.assertRaises(
                ValueError,
                limit.Enforcer,
                self.claim,
                callback=invalid_callback
            )

    def test_verify_must_be_boolean(self):
        invalid_verify_types = [uuid.uuid4().hex, 5, 5.1]

        for invalid_verify in invalid_verify_types:
            self.assertRaises(
                ValueError,
                limit.Enforcer,
                self.claim,
                callback=self._get_usage_for_project,
                verify=invalid_verify
            )

    def test_claim_must_be_an_instance_of_project_claim(self):
        invalid_claim_types = [uuid.uuid4().hex, 5, 5.1, True, False, [], {}]

        for invalid_claim in invalid_claim_types:
            self.assertRaises(
                ValueError,
                limit.Enforcer,
                invalid_claim,
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
        enforcer = limit.Enforcer(self.claim, callback, verify=True)
        enforcer._connection.limits.return_value = self._create_generator(
            [{'resource_limit': 20}])
        with enforcer:
            # 20(limit) > 10(quantity) + 8(usage), so enforce success.
            pass

    def test_call_enforce_fail(self):
        callback = self._get_usage_for_project
        enforcer = limit.Enforcer(self.claim, callback=callback, verify=True)
        enforcer._connection.limits.return_value = self._create_generator(
            [{'resource_limit': 10}])
        # 10(limit) < 10(quantity) + 8(usage), enforce fail.
        self.assertRaises(exception.ClaimExceedsLimit, enforcer.__enter__)

    def test_call_enforce_with_registered_limit_success(self):
        callback = self._get_usage_for_project
        enforcer = limit.Enforcer(self.claim, callback=callback, verify=True)
        enforcer._connection.limits.return_value = self._create_generator([])
        enforcer._connection.registered_limits.return_value = (
            self._create_generator([{'default_limit': 20}]))
        # 20(registered_limit) > 10(quantity) + 8(usage), enforce success.
        with enforcer:
            pass

    def test_call_enforce_with_registered_limit_fail(self):
        callback = self._get_usage_for_project
        enforcer = limit.Enforcer(self.claim, callback, verify=True)
        enforcer._connection.limits.return_value = self._create_generator([])
        enforcer._connection.registered_limits.return_value = (
            self._create_generator([{'default_limit': 15}]))
        # 15(registered_limit) < 10(quantity) + 8(usage), enforce fail.
        self.assertRaises(exception.ClaimExceedsLimit, enforcer.__enter__)

    def test_call_enforce_with_no_limit(self):
        callback = self._get_usage_for_project
        enforcer = limit.Enforcer(self.claim, callback, verify=True)
        enforcer._connection.limits.return_value = self._create_generator([])
        enforcer._connection.registered_limits.return_value = (
            self._create_generator([]))
        self.assertRaises(exception.LimitNotFound, enforcer.__enter__)

    def test_call_verify_times_if_true(self):
        callback = self._get_usage_for_project
        enforcer = limit.Enforcer(self.claim, callback, verify=True)
        enforcer._connection.limits.return_value = self._create_generator(
            [{'resource_limit': 10}])
        enforcer._verify = mock.MagicMock()

        with enforcer:
            pass
        # no error raises during enforcing, so _verify will be called 2 times,
        # one is in __enter__, one is in __exit__
        self.assertEqual(2, enforcer._verify.call_count)

    def test_call_verify_times_if_false(self):
        callback = self._get_usage_for_project
        enforcer = limit.Enforcer(self.claim, callback, verify=False)
        enforcer._connection.limits.return_value = self._create_generator(
            [{'resource_limit': 10}])
        enforcer._verify = mock.MagicMock()

        with enforcer:
            pass
        # no error raises during enforcing, but the verify is False, so _verify
        # will be called 1 times in __enter__.
        self.assertEqual(1, enforcer._verify.call_count)

    def test_call_verify_times_if_raising_error(self):
        callback = self._get_usage_for_project
        enforcer = limit.Enforcer(self.claim, callback, verify=True)
        enforcer._connection.limits.return_value = self._create_generator(
            [{'resource_limit': 10}])
        enforcer._verify = mock.MagicMock()

        class FakeException(Exception):
            def __init__(self):
                msg = "fake exception for test."
                super(FakeException, self).__init__(msg)

        try:
            with enforcer:
                raise FakeException
        except FakeException:
            expect_verify_call_count = 1

        # error raises during enforcing, so _verify will be called 1 times in
        # __enter__
        self.assertEqual(expect_verify_call_count,
                         enforcer._verify.call_count)
