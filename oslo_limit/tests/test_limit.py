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

import uuid

from oslotest import base

from oslo_limit import limit


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
