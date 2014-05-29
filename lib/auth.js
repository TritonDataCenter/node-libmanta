var assert = require('assert-plus');
var PUBLIC_STOR_PATH = /^\/([a-zA-Z0-9_\-\.@%]+)\/public(\/(.*)|)/;
var ANONYMOUS_USER = 'anonymous';

module.exports = {
    authorize: authorize,
    ANONYMOUS_USER: ANONYMOUS_USER
};

/*
 * Determine whether an authenticated user has access to the given resource
 * under the specified conditions.  If access is allowed, returns true.
 * Otherwise, throws an error describing the authentication problem.  This
 * function checks both whether the user explicitly has access as well as
 * whether access is allowed because the resource itself is marked public.
 *
 * Arguments include:
 *
 *     mahi             A mahiv2 client
 *     (object)
 *
 *     context          Describes the user, action, and conditions being
 *     (object)         authorized.
 *
 *          principal   The authenticated user that's attempting to perform the
 *          (object)    operation.  This may be a top-level account or a subuser
 *                      within an account.  For real services, this would
 *                      usually be the return value of mahi.authenticate().  For
 *                      internal tools or test suites that bypass real
 *                      authentication, use the value of mahi.getAccount() or
 *                      mahi.getUser().
 *
 *                      If the request is coming from an unauthenticated user
 *                      and the resource owner's account has an "anonymous"
 *                      subuser, then that subuser should be specified here.  If
 *                      the request is unauthenticated and the account has no
 *                      anonymous user, then { "roles": {}, "account": {} }
 *                      should be specified here.
 *
 *          action      The action the user is trying to perform.
 *          (string)
 *
 *          resource    The resource being accessed, with the following fields:
 *          (object)
 *
 *             owner            Object representing either the account that
 *             (object)         owns this resource or "anonymous" subuser for
 *                              the account that owns the resource.  Recall that
 *                              all resources are owned by accounts, not
 *                              subusers.  If the account allows for any
 *                              "public" resources (i.e., resources that are
 *                              accessible to both authenticated and
 *                              unauthenticated callers that may not otherwise
 *                              have explicit access), then it must have a
 *                              subuser called "anonymous".  In that case, the
 *                              owner specified here for *all* resources owned
 *                              by the account should be that anonymous user.
 *                              If the account does not allow any such public
 *                              resources (by having no anonymous user), then
 *                              the resource owner specified here is the account
 *                              itself.
 *
 *                              The value specified here is usually the result
 *                              of calling mahi.getUser() on the "anonymous"
 *                              user for the account that owns the resource.
 *                              (If the anonymous user does not exist for that
 *                              account, mahi.getUser() will still return the
 *                              account object, so you can still pass the return
 *                              value here even if you get a
 *                              UserDoesNotExistError from mahi.getUser().)
 *
 *             key              The internal resource path (i.e., with the
 *             (string)         user's login resolved to their user uuid).  This
 *                              is used to check for access to objects in
 *                              directories that are defined to be public (e.g.,
 *                              "/$userid/public/...").
 *
 *             roles            Role tags (uuids) associated with this resource.
 *             (array)
 *
 *          conditions  Additional context collected as part of this operation:
 *          (object)
 *
 *              activeRoles     List of role tags that are active for the
 *              (array)         current user for this operation.
 *
 *              owner           Represents the *account* that owns the resource.
 *              (object)        This differs from resource.owner, which may
 *                              actually specify the anonymous subuser (see
 *                              above).  It's useful to remember that
 *                              "conditions" are used to evaluate user-specified
 *                              access control rules, so "owner" exists in this
 *                              context to allow users to write rules based on
 *                              the account that owns the resource.  By
 *                              contrast, "owner" exists under "resource" for
 *                              the system-defined semantics relating to
 *                              a resource's owner.
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
    var action = opts.context.action;
    var resource = opts.context.resource;
    resource.roles = resource.roles || [];

    // Authorize if public GET.
    if (PUBLIC_STOR_PATH.test(resource.key) &&
        (action === 'getobject' ||
        action === 'getdirectory' ||
        action === 'mlogin')) {

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
