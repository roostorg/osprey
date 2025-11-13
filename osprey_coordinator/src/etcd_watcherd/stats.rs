use crate::metrics::define_metrics;

define_metrics!(WatcherStats, [
    // Incremented when an etcd error occurs during the watcher loop.
    watch_etcd_errors => StaticCounter(),
    // Incremented when a watch fails to start.
    watcher_start_fails => StaticCounter(),
    // Incremented when a watcher successfully starts.
    watcher_start_success => StaticCounter(),
    // Incremented when a watcher has been swept.
    watchers_swept => StaticCounter(),
    // Incremented when an etcd watch request returns a result.
    watch_success => StaticCounter(),
    // Incremented when a watch times out.
    watch_timeout => StaticCounter(),
    // Incremented when a watch resets.
    watch_reset => StaticCounter(),
    // Incremented when a new subscription is created.
    subscriptions_created => StaticCounter(),
    // Incremented when a subscription is removed.
    subscriptions_removed => StaticCounter(),
    // Incremented when a subscription has been re-created.
    subscriptions_recreated => StaticCounter(),
    // Incremented when a subscription falls too far behind and gets reset.
    subscriptions_reset => StaticCounter(),
    // Incremented when a subscription has been notified of an event.
    subscriptions_notified => StaticCounter(),
]);
