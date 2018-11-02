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

from keystoneauth1 import loading as ksa_loading
from openstack import connection
from oslo_config import cfg
from oslo_log import log
import six

from oslo_limit import exception
from oslo_limit import opts


LOG = log.getLogger(__name__)
CONF = cfg.CONF
_SDK_CONNECTION = None

opts.register_opts(CONF)


def _get_keystone_connection():
    global _SDK_CONNECTION

    if not _SDK_CONNECTION:
        try:
            auth = ksa_loading.load_auth_from_conf_options(
                CONF, group='oslo_limit')
            session = ksa_loading.load_session_from_conf_options(
                CONF, group='oslo_limit', auth=auth)
            _SDK_CONNECTION = connection.Connection(session=session).identity
        except Exception as e:
            msg = "Can't initialise SDK session, reason: %s" % e
            LOG.error(msg)
            raise exception.SessionInitError(e)

    return _SDK_CONNECTION


class ProjectClaim(object):

    def __init__(self, project_id):
        """An object representing a claim of resources against a project.

        :param project_id: The ID of the project claiming the resources.
        :type project_id: string

        """

        if not isinstance(project_id, six.string_types):
            msg = 'project_id must be a string type.'
            raise ValueError(msg)

        self.claims = {}
        self.project_id = project_id

    def add_resource(self, resource_name, quantity):
        """Add a resource type and quantity to a claim.

        :param resource_name: A string representing the resource to claim.
        :type resource_name: string
        :param quantity: The number of resources being claimed.
        :type quantity: integer

        """

        if not isinstance(resource_name, six.string_types):
            msg = 'resource_name must be a string type.'
            raise ValueError(msg)

        if quantity and not isinstance(quantity, int):
            msg = 'quantity must be an integer.'
            raise ValueError(msg)

        self.claims[resource_name] = quantity


class Enforcer(object):

    def __init__(self, claim, callback=None, verify=True):
        """Context manager for checking usage against resource claims.

        :param claim: An object containing information about the claim.
        :type claim: ``oslo_limit.limit.ProjectClaim``
        :param callback: A callable function that accepts a project_id string
                         as a parameter and calculates the current usage of a
                         resource.
        :type callable function:
        :param verify: Boolean denoting whether or not to verify the new usage
                       after executing a claim. This can be useful for handling
                       race conditions between clients claiming resources.
        :type verify: boolean

        """

        if not isinstance(claim, ProjectClaim):
            msg = 'claim must be an instance of oslo_limit.limit.ProjectClaim.'
            raise ValueError(msg)
        if callback and not callable(callback):
            msg = 'callback must be a callable function.'
            raise ValueError(msg)
        if verify and not isinstance(verify, bool):
            msg = 'verify must be a boolean value.'
            raise ValueError(msg)

        self.claim = claim
        self.callback = callback
        self.verify = verify
        self._connection = _get_keystone_connection()
        self._service_id, self._region_id = self._get_service_and_region()
        self._resource_limit = None

    def __enter__(self):
        if not self._resource_limit:
            self._resource_limit = self._get_resource_limit()
        self._verify(self._resource_limit)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_val:
            if self.verify:
                self._verify(self._resource_limit)

    # FixMe(wxy): enable caching function. See bug 1790894
    def _get_resource_limit(self):
        """Get the claimed resource's limit.

        Return registered limit instead if the project list does not exist.
        """
        limit = self._connection.limits(
            service_id=self._service_id,
            region_id=self._region_id,
            resource_name=self.claim.resource_name,
            project_id=self.claim.project_id)
        try:
            project_limit = next(limit)
            return project_limit.resource_limit
        except StopIteration:
            limit = self._connection.registered_limits(
                service_id=self._service_id,
                region_id=self._region_id,
                resource_name=self.claim.resource_name)
            try:
                registered_limit = next(limit)
                return registered_limit.default_limit
            except StopIteration:
                raise exception.LimitNotFound(self.claim.resource_name)

    # FixMe(wxy): enable caching function. See bug 1790894
    def _get_service_and_region(self):
        """Get service id and region id of the service."""

        endpoint_id = CONF.oslo_limit.endpoint_id
        endpoint = self._connection.get_endpoint(endpoint_id)
        return endpoint.service_id, endpoint.region_id

    def _verify(self, limit):
        resource_names = self.claim.claims.keys()
        usage = self.callback(self.claim.project_id, resource_names)
        quantity = self.claim.quantity
        if usage + quantity > limit:
            raise exception.ClaimExceedsLimit(usage, quantity, limit,
                                              self.claim.resource_name)
