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

from oslo_limit._i18n import _


class ClaimExceedsLimit(Exception):
    def __init__(self, usage, quantity, limit, resource):
        msg = _("%(usage)s %(resource)s have been used. Claiming %(quantity)s "
                "%(resource)s would exceed the current limit of %(limit)s"
                ) % {'usage': usage,
                     'quantity': quantity,
                     'limit': limit,
                     'resource': resource}
        super(ClaimExceedsLimit, self).__init__(msg)


class LimitNotFound(Exception):
    def __init__(self, resource):
        msg = _("Can't find the limit for resource %(resource)s."
                ) % {'resource': resource}
        super(LimitNotFound, self).__init__(msg)


class SessionInitError(Exception):
    def __init__(self, reason):
        msg = _("Can't initialise OpenStackSDK session, reason: %(reason)s."
                ) % {'reason': reason}
        super(SessionInitError, self).__init__(msg)
