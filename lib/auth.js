var assert = require('assert-plus');
var PUBLIC_STOR_PATH = /^\/([a-zA-Z][a-zA-Z0-9_\-\.@%]+)\/public(\/(.*)|)/;
var ANONYMOUS_USER = 'anonymous';

module.exports = {
    authorize: authorize
};

/*
 * mahi: mahi client
 * context.principal: the object received from mahi.authenticate
 * context.action: auth action
 * context.resource.owner: the object received from calling mahi.getAccount
 *       with the resource owner's login
 * context.resource.key: resource key or path (used to check for PUBLIC gets)
 * context.resource.roles: resource role tags (UUIDs)
 * context.conditions: all additional context collected as part of the request,
 *      including activeRoles
 */
function authorize(opts) {
    assert.object(opts);
    assert.object(opts.mahi, 'mahi');
    assert.object(opts.context, 'context');
    assert.object(opts.context.principal, 'principal');
    assert.string(opts.context.action, 'action');
    assert.object(opts.context.resource, 'resource');
    assert.object(opts.context.conditions, 'conditions');

    assert.object(opts.context.principal.account, 'principal.account');
    assert.optionalObject(opts.context.principal.user, 'principal.user');

    assert.object(opts.context.resource.owner, 'resource.owner');
    assert.string(opts.context.resource.owner.account.uuid,
        'resource.owner.account.uuid');
    assert.arrayOfString(opts.context.resource.roles, 'resource.roles');
    assert.string(opts.context.resource.key, 'resource.key');

    assert.string(opts.context.conditions.method, 'conditions.method');
    assert.arrayOfString(opts.context.conditions.activeRoles,
        'condition.activeRoles'); // array of UUID

    var mahi = opts.mahi;
    var method = opts.context.conditions.method;
    var resource = opts.context.resource;
    resource.roles = resource.roles || [];

    // Authorize if public GET.
    if (PUBLIC_STOR_PATH.test(resource.key) &&
        (method === 'GET' || method === 'HEAD' || method === 'OPTIONS')) {

        return (true);
    }

    var error;

    try {
        mahi.authorize(opts.context);
    } catch (e) {
        error = e;
    }

    // If authorization fails as the authenticated user, check if the resource
    // owner has allowed anonymous access to the resource.
    if (error &&
        resource.owner.user &&
        resource.owner.user.login === ANONYMOUS_USER) {

        opts.context.principal = opts.context.resource.owner;
        opts.context.conditions.activeRoles =
            opts.context.principal.user.defaultRoles;

        try {
            mahi.authorize(opts.context);
        } catch (e) {
            throw error; // throw the error from when we attempted to authorize
                         // as the user himself, not as the anonymous user, as
                         // that feedback is more likely something the user can
                         // act upon and do something about, if possible
        }
    } else if (error) {
        throw error;
    }

    return (true);
}
