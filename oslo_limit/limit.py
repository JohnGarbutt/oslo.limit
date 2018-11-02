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


class Enforcer(object):

    def __init__(self, callback=None):
        """For checking usage against resource claims.

        :param callback: A callable function that accepts a project_id string
                         as a parameter and calculates the current usage of a
                         resource.
        :type callable function:
        """

        if callback and not callable(callback):
            msg = 'callback must be a callable function.'
            raise ValueError(msg)

        self.callback = callback
        self._connection = _get_keystone_connection()
        self._service_id, self._region_id = self._get_service_and_region()

    # FixMe(wxy): enable caching function. See bug 1790894
    def _get_service_and_region(self):
        """Get service id and region id of the service."""
        endpoint_id = CONF.oslo_limit.endpoint_id
        endpoint = self._connection.get_endpoint(endpoint_id)
        return endpoint.service_id, endpoint.region_id

    # FixMe(wxy): enable caching function. See bug 1790894
    def _get_resource_limit(self, project_id, resource_name):
        """Get the claimed resource's limit.

        Return registered limit instead if the project list does not exist.
        """
        limit = self._connection.limits(
            service_id=self._service_id,
            region_id=self._region_id,
            resource_name=resource_name,
            project_id=project_id)
        try:
            project_limit = next(limit)
            return project_limit.resource_limit
        except StopIteration:
            limit = self._connection.registered_limits(
                service_id=self._service_id,
                region_id=self._region_id,
                resource_name=resource_name)
            try:
                registered_limit = next(limit)
                return registered_limit.default_limit
            except StopIteration:
                raise exception.LimitNotFound(self.claim.resource_name)

    def _get_all_limits(self, project_id, resource_names):
        limits = {}
        for resource_name in resource_names:
            limits[resource_name] = self._get_resource_limit(project_id,
                                                             resource_names)
        return limits

    def check_limits(self, context, project_id, extra_resources):
        """Exception if currently over any of the specified limits.

        :param extra_resources: a dict that contains the resources
            that need to be checked and how many extra resources are
            required e.g.:
               {'instances': 1, 'resources:VCPU': 2, 'resources:MEMORY_MB': 3}
            Or if you just want to check if already over limit:
               {'instances': 0, 'resources:VCPU': 0, 'resources:MEMORY_MB': 0}
        """
        # TODO(johngarbutt) clearly need to be more defensive here!
        resource_names = extra_resources.keys()

        limits = self._get_all_limits(project_id, resource_names)
        usages = self.callback(context, project_id, resource_names)

        for resource_name in resource_names:
            limit = int(limits[resource_name])
            usage = int(usages[resource_name])
            extra = int(extra_resources.get(resource_name, 0))

            # TODO(johngarbutt) clearly should return all problems,
            # not just the first one! Also note extra may be zero here.
            if usage + extra > limit:
                raise exception.ClaimExceedsLimit(usage, extra, limit,
                                                  resource_name)

        # TODO(johngarbutt) Seems odd not to return this, but I don't need it
        return limits, usages

    def verify_claim(self, project_id, extra_resources):
        """Get a context manager to double check claim."""
        return EnforcerContext(self, project_id, extra_resources)


# NOTE: not sure we need this in the first version...
class EnforcerContext(object):

    def __init__(self, enforcer, project_id, extra_resources):
        """Context manager for checking usage against resource claims.

        It will verify the new usage after executing a claim. This can be
        useful for handling race conditions between clients claiming resources.
        It assumes the resource usage counting will now include the
        extra_resources that were originally requested.
        """
        self._enforcer = enforcer
        self._project_id = project_id
        self._extra_resources = extra_resources
        self.limits = None
        self.usages = None

    def __enter__(self):
        # allow the user to check out what is being used and limited
        # although probably more useful in the error case, annoyingly
        self.limits, self.usages = self.enforcer.check_limits(
            self._project_id, self._extra_resources)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_val:
            resource_names = self._extra_resources.keys()
            extra_resources = {name: 0 for name in resource_names}
            self.enforcer.check_limits(self._project_id, extra_resources)
            # TODO(johngarbutt) should we have some rollback function that gets
            # called when we hit and error here?
