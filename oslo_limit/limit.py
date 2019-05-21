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


class EndpointEnforcerContext(object):
    def __init__(self, resource_callback):
        """Creates a set of limits utils specific to a given endpoint.

        :param resource_counter: this is a function that takes the following
        inputs: (project_id, list of resources to count)
        it returns a dict of resources and their counts
        """
        # Allow cache of endpoint specific context
        self._connection = _get_keystone_connection()
        self.endpoint_id = CONF.oslo_limit.endpoint_id
        endpoint = self._connection.get_endpoint(self.endpoint_id)
        self._service_id = endpoint.service_id
        self._region_id = endpoint.region_id
        # TODO(johngarbutt) select the correct enforcer
        self._enforcer = FlatEnforcer(self, resource_callback)

    def check_all_limits(self, project_id, deltas=None):
        """Check all limits for a given project.

        This involves using the resource_callback to count all resources
        relating to a given project. This is then checked against the
        current per project limits in keystone.

        Optionally additional resources can be added to the above counts
        """
        resource_names = self._get_all_registered_limits()

        if deltas is None:
            deltas = {}

        for resource in deltas.keys():
            if resource not in resource_names:
                raise ValueError("unexpected resource %s in deltas" % resource)

        all_deltas = {}
        for resource_name in resource_names:
            all_deltas[resource_name] = deltas.get(resource_name, 0)

        self._enforcer.check_limits(project_id, all_deltas)

    def _get_all_registered_limits(self):
        registered_limits = self._connection.registered_limits(
            service_id=self._service_id, region_id=self._region_id)
        return [limit.resource_name for limit in registered_limits]

    # FixMe(wxy): enable caching function. See bug 1790894
    def get_limit(self, project_id, resource_name):
        """Return the unified limits from a particular resource.

        :param resource_name: the name of the resource to return limits for,
                              this should coorespond to the name of the limit
                              in keystone.
        :returns: an integer representing the limit of the resource.
        :raises exception.LimitNotFound: in the event there is no corresponding
                                         limit within keystone.

        """
        project_limit = self._get_project_limit(project_id, resource_name)
        if project_limit:
            return project_limit.resource_limit
        registered_limit = self._get_registered_limit(resource_name)
        if registered_limit:
            return registered_limit.default_limit

        raise exception.LimitNotFound(resource_name)

    def _get_project_limit(self, project_id, resource_name):
        limit = self._connection.limits(
            service_id=self._service_id, region_id=self._region_id,
            resource_name=resource_name, project_id=project_id)
        try:
            return next(limit)
        except StopIteration:
            return None

    def _get_registered_limit(self, resource_name):
        reg_limit = self._connection.registered_limits(
            service_id=self._service_id, region_id=self._region_id,
            resource_name=resource_name)
        try:
            return next(reg_limit)
        except StopIteration:
            return None


class FlatEnforcer(object):
    def __init__(self, endpoint_context, resource_callback):
        self._endpoint_context = endpoint_context
        self._resource_callback = resource_callback

    def check_limits(self, project_id, resource_deltas):
        resource_names = sorted(list(resource_deltas.keys()))

        counts = self._resource_callback(project_id, resource_names)

        overs = []
        for resource_name in resource_names:
            delta = resource_deltas[resource_name]
            if resource_name not in counts:
                raise ValueError("no counts for %s" % resource_name)
            count = counts.get(resource_name, 0)
            limit = self._endpoint_context.get_limit(project_id, resource_name)

            if int(count) + int(delta) > int(limit):
                overs.append((count, delta, limit, resource_name))

        if len(overs) > 0:
            usage, delta, limit, resource_name = overs[0]
            raise exception.ClaimExceedsLimit(usage, delta, limit,
                                              resource_name)


class TreeEnforcer(object):
    """For any given project_id, counts resources in the whole tree."""
    pass


class Claim(object):

    def __init__(self, resource_name, quantity):
        """An object representing a claim of resources against a project.

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

        self.resource_name = resource_name
        self.quantity = quantity


class Enforcer(object):

    def __init__(self, claims, project_id, callback, verify=True):
        """Context manager for checking usage against resource claims.

        :param claims: An object containing information about the claim.
        :type claims: an instance of ``oslo.limit.limit.Claim`` or a list of
                     ``oslo_limit.limit.Claim`` instances.
        :param project_id: The ID of the project claiming the resources.
        :type project_id: string
        :param callback: A callable function that accepts a project_id string
                         as a parameter and calculates the current usage of a
                         resource.
        :type callable function:
        :param verify: Boolean denoting whether or not to verify the new usage
                       after executing a claim. This can be useful for handling
                       race conditions between clients claiming resources.
        :type verify: boolean

        """

        if not isinstance(claims, Claim):
            msg = (
                'claim must be an instance of oslo_limit.limit.Claim or a '
                'list of oslo.limit.limit.Claim instances.'
            )
            if not isinstance(claims, list):
                raise ValueError(msg)
            elif not claims:
                raise ValueError(msg)
            else:
                for claim in claims:
                    if not isinstance(claim, Claim):
                        raise ValueError(msg)
        if isinstance(claims, list):
            msg = (
                'claim must be an instance of oslo_limit.limit.Claim or a '
                'list of oslo.limit.limit.Claim instances.'
            )
            for claim in claims:
                if not isinstance(claim, Claim):
                    raise ValueError(msg)
        if not isinstance(project_id, six.string_types):
            msg = 'project_id must be a string type.'
            raise ValueError(msg)
        if not callable(callback):
            msg = 'callback must be a callable function.'
            raise ValueError(msg)
        if not isinstance(verify, bool):
            msg = 'verify must be a boolean value.'
            raise ValueError(msg)

        self.callback = callback
        if isinstance(claims, Claim):
            claims = [claims]
        self.claims = claims

        self.project_id = project_id
        self._connection = _get_keystone_connection()
        self._service_id, self._region_id = self._get_service_and_region()
        # TODO(wxy): Add verify function.
        self.verify = verify

    def __enter__(self):
        # TODO(lbragstad): Wire this up eventually. The idea for using a
        # context manager to implement enforcement was to make it easier for
        # service developers to "wrap" code that consumed resources in their
        # services and implement enforcement at the same time. The __enter__()
        # function was supposed to check initial limits and usage. The
        # __exit__() function was responsible for checking that the project was
        # still under it's limit, ultimately protecting against race conditions
        # between clients claiming resources on the same project. In theory,
        # this design works when the process creating the resources is also the
        # process performing the verification check for race conditions. If
        # that verification check is done by another process, then the
        # decoupling defeats the purpose of the context manager. There are
        # places in OpenStack where verification is done by a separate service,
        # and using a context manager in both places is awkward. Instead, we
        # should expose a public API to enforce the initial usage before
        # resources are created and another method the can verify the absense
        # of race conditions.
        #
        # In the future, we should be call the initial usage check from this
        # method and the verification check from the __exit__() to implement a
        # context manager for processes that can use it.
        pass

    def __exit__(self, *args):
        pass

    # FixMe(wxy): enable caching function. See bug 1790894
    def _get_service_and_region(self):
        """Get service ID and region ID of the service.

        :returns: a tuple containing the service ID and region ID of the
                  endpoint(e.g., (service_id, region_id)

        """
        endpoint_id = CONF.oslo_limit.endpoint_id
        endpoint = self._connection.get_endpoint(endpoint_id)
        return endpoint.service_id, endpoint.region_id

    def enforce(self):
        """Perform an enforcement check based on claims, limits, and usage.

        :raises exception.ClaimExceedsLimit: in the event the resources being
                                             claimed exceed the allow limit for
                                             that project.

        """
        # get usage for all resources for a specific project
        current_usage = self.callback(self.project_id, self.claims)

        for claim in self.claims:
            # get either the default limit or project limit
            limit = self._get_resource_limit(claim.resource_name)
            usage = current_usage.get(claim.resource_name, 0)
            if usage + claim.quantity > limit:
                raise exception.ClaimExceedsLimit(
                    usage, claim.quantity, limit, claim.resource_name
                )

    # FixMe(wxy): enable caching function. See bug 1790894
    def _get_resource_limit(self, resource_name):
        """Return the unified limits from a particular resource.

        :param resource_name: the name of the resource to return limits for,
                              this should coorespond to the name of the limit
                              in keystone.
        :returns: an integer representing the limit of the resource.
        :raises exception.LimitNotFound: in the event there is no corresponding
                                         limit within keystone.

        """
        project_limit = self._get_project_limit(resource_name)
        if project_limit:
            return project_limit.resource_limit
        registered_limit = self._get_registered_limit(resource_name)
        if registered_limit:
            return registered_limit.default_limit

        raise exception.LimitNotFound(resource_name)

    def _get_project_limit(self, resource_name):
        limit = self._connection.limits(
            service_id=self._service_id, region_id=self._region_id,
            resource_name=resource_name, project_id=self.project_id)
        try:
            return next(limit)
        except StopIteration:
            return None

    def _get_registered_limit(self, resource_name):
        reg_limit = self._connection.registered_limits(
            service_id=self._service_id, region_id=self._region_id,
            resource_name=resource_name)
        try:
            return next(reg_limit)
        except StopIteration:
            return None
