var assert = require('assert-plus');
var PUBLIC_STOR_PATH = /^\/([a-zA-Z][a-zA-Z0-9_\-\.@%]+)\/public(\/(.*)|)/;

module.exports = {
    authorize: authorize
};

/*
 * TODO add docs
 */
function authorize(opts) {
    assert.object(opts);
    assert.object(opts.mahi);
    assert.object(opts.context);
    assert.object(opts.context.principal);
    assert.string(opts.context.action);
    assert.object(opts.context.resource);
    assert.object(opts.context.conditions);

    assert.object(opts.context.principal.account);
    assert.optionalObject(opts.context.principal.user);

    assert.object(opts.context.resource.owner);
    assert.string(opts.context.resource.owner.uuid);
    assert.string(opts.context.resource.key);

    var mahi = opts.mahi;
    var principal = opts.context.principal;
    var method = opts.context.conditions.method;
    var resource = opts.context.resource;
    resource.roles = resource.roles || [];

    var owner = resource.owner;

    // If the caller is the account owner, allow access to all of the account's
    // stuff. If the caller is an operator, allow access.
    // Only do these checks if the caller is acting as an account (not a user).
    // TODO
    // superusers - users that can access anything under their parent account
    if (!principal.user) {
        if (owner.uuid === principal.account.uuid) {
            return (true);
        }

        if (principal.account.isOperator) {
            return (true);
        }
    }

    // Deny if the caller or owner is not approved for provisioning.
    // Operators will have been allowed access above regardless of approved
    // for provisioning status on their account or the owner of the resource.
    if (!principal.account.approved_for_provisioning ||
        !resource.owner.approved_for_provisioning) {

        return (false);
    }

    // Authorize if public GET.
    if (PUBLIC_STOR_PATH.test(resource.key) &&
        (method === 'GET' || method === 'HEAD' || method === 'OPTIONS')) {

        return (true);
    }

    // TODO anonymous users

    var ok = false;

    try {
        ok = mahi.authorize(opts.context);
    } catch (e) {
        if (e.restCode === 'RoleTagMismatch' || e.restCode === 'InactiveRole') {
            ok = false;
        } else {
            throw e;
        }
    }

    return (ok);
}
