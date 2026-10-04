-- Queue workers legitimately operate across tenants (they own no tenant). Instead of
-- granting BYPASSRLS, which is all-or-nothing and easy to leak into a request path,
-- the queue tables become visible only to connections that declare the worker scope.
-- TaskQueue issues SET LOCAL app.worker_scope = 'true' inside its own transactions;
-- no HTTP handler can set it.
CREATE POLICY worker_tasks ON tasks
    USING (current_setting('app.worker_scope', true) = 'true')
    WITH CHECK (current_setting('app.worker_scope', true) = 'true');

CREATE POLICY worker_outbox ON outbox_events
    USING (current_setting('app.worker_scope', true) = 'true')
    WITH CHECK (current_setting('app.worker_scope', true) = 'true');
