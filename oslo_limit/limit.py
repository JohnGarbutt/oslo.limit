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

from keystoneauth1 import exceptions as ksa_exception
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


class _FlatEnforcer(object):

    def __init__(self, usage_callback):
        """An object for checking usage against resource limits and requests.

        :param usage_callback: A callable function that accepts a project_id
                               string as a parameter and calculates the current
                               usage of a resource.
        :type callable function:
        """
        if not callable(usage_callback):
            msg = 'usage_callback must be a callable function.'
            raise ValueError(msg)
        self.usage_callback = usage_callback

        keystone_connection = _get_keystone_connection()
        self.utils = _EnforcerUtils(keystone_connection)

    def enforce(self, project_id, deltas=None):
        """Check resource usage against limits

        If deltas are specified, add deltas to exiting usage.
        Note we fail for any resource usage over the limit, not just
        the ones specified in the deltas.

        :param project_id: The project to check usage and enforce limits
                           against.
        :type project_id: string
        :param deltas: An dictionary containing resource names as keys and
                       requests resource quantities as values.
        :type deltas: dictionary or None
        """
        if not project_id or not isinstance(project_id, six.string_types):
            msg = 'project_id must be a non-empty string.'
            raise ValueError(msg)
        if deltas is not None and not isinstance(deltas, dict):
            msg = 'deltas must be a dictionary.'
            raise ValueError(msg)

        resource_names = self.utils.get_all_registered_limit_resources()
        # sort names for predictable failure order
        resource_names = sorted(list(resource_names))

        if deltas is None:
            deltas = {}

        for resource in deltas.keys():
            if resource not in resource_names:
                msg = "unexpected resource %s in deltas" % resource
                raise ValueError(msg)

        all_deltas = {}
        for resource_name in resource_names:
            all_deltas[resource_name] = deltas.get(resource_name, 0)

        counts = self.usage_callback(project_id, resource_names)

        overs = []
        for resource_name in resource_names:
            delta = all_deltas[resource_name]
            if resource_name not in counts:
                msg = "missing resource counts for %s" % resource_name
                raise ValueError(msg)
            count = counts.get(resource_name, 0)
            limit = self.utils.get_limit(project_id, resource_name)

            if int(count) + int(delta) > int(limit):
                overs.append((count, delta, limit, resource_name))

        if len(overs) > 0:
            LOG.debug("hit limits for project %s %s", project_id, overs)
            usage, delta, limit, resource_name = overs[0]
            raise exception.ClaimExceedsLimit(usage, delta, limit,
                                              resource_name)


# TODO(johngarbutt) we should load this based on keystone settings
Enforcer = _FlatEnforcer


def _get_keystone_connection():
    global _SDK_CONNECTION

    if not _SDK_CONNECTION:
        try:
            auth = ksa_loading.load_auth_from_conf_options(
                CONF, group='oslo_limit')
            session = ksa_loading.load_session_from_conf_options(
                CONF, group='oslo_limit', auth=auth)
            _SDK_CONNECTION = connection.Connection(session=session).identity
        except (ksa_exception.NoMatchingPlugin,
                ksa_exception.MissingRequiredOptions,
                ksa_exception.MissingAuthPlugin,
                ksa_exception.DiscoveryFailure,
                ksa_exception.BadRequest,
                ksa_exception.Unauthorized) as e:
            msg = "Can't initialise OpenStackSDK session, reason: %s" % e
            LOG.error(msg)
            raise exception.SessionInitError(e)

    return _SDK_CONNECTION


class _EnforcerUtils(object):
    def __init__(self, keystone_connection):
        self.connection = keystone_connection

        # cache endpoint info
        endpoint_id = CONF.oslo_limit.endpoint_id
        self.endpoint = self.connection.get_endpoint(endpoint_id)
        if not self.endpoint:
            raise ValueError("can't find endpoint for %s" % endpoint_id)

    def get_all_registered_limit_resources(self):
        reg_limits = self.connection.registered_limits(
            service_id=self.endpoint.service_id,
            region_id=self.endpoint.region_id)
        return [limit.resource_name for limit in reg_limits]

    def get_limit(self, project_id, resource_name):
        # TODO(johngarbutt): might need to cache here
        project_limit = self._get_project_limit(project_id, resource_name)
        if project_limit:
            return project_limit.resource_limit
        registered_limit = self._get_registered_limit(resource_name)
        if registered_limit:
            return registered_limit.default_limit

        raise exception.LimitNotFound(resource_name)

    def _get_project_limit(self, project_id, resource_name):
        limit = self.connection.limits(
            service_id=self.endpoint.service_id,
            region_id=self.endpoint.region_id,
            resource_name=resource_name,
            project_id=project_id)
        try:
            return next(limit)
        except StopIteration:
            return None

    def _get_registered_limit(self, resource_name):
        reg_limit = self.connection.registered_limits(
            service_id=self.endpoint.service_id,
            region_id=self.endpoint.region_id,
            resource_name=resource_name)
        try:
            return next(reg_limit)
        except StopIteration:
            return None
