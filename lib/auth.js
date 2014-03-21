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

    assert.string(opts.context.resource.owner);
    assert.string(opts.context.resource.key);

    var mahi = opts.mahi;
    var principal = opts.context.principal;
    var method = opts.context.conditions.method;
    var resource = opts.context.resource;
    resource.roles = resource.roles || [];

    var owner = resource.owner;

    // Authorize if public GET.
    if (PUBLIC_STOR_PATH.test(resource.key) &&
        (method === 'GET' || method === 'HEAD' || method === 'OPTIONS')) {

        return (true);
    }

    // If the caller is the account owner, allow access to all of the account's
    // stuff. If the caller is an operator, allow access.
    if (!principal.user) {
        if (owner === principal.account.uuid) {
            return (true);
        }

        if (principal.account.isOperator) {
            return (true);
        }
    }

    // TODO
    // superusers - users that can access anything under their parent account
    // anonymous users

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
