use std::{collections::HashMap, fmt::Debug, future::Future, hash::Hash, sync::Arc};

use futures::{
    future::{BoxFuture, Shared},
    FutureExt,
};
use parking_lot::RwLock;

enum CachedFuture<T, E>
where
    T: Clone,
    E: Clone,
{
    Ready(T),
    Pending(Shared<BoxFuture<'static, Result<T, E>>>),
}

pub enum CachedFutureRef<'a, T> {
    Ready(&'a T),
    Pending,
}

#[derive(Clone)]
pub struct CachedFutures<K, T, E>
where
    K: Hash + Eq + Clone,
    T: Clone,
    E: Clone,
{
    cache: Arc<RwLock<HashMap<K, CachedFuture<T, E>>>>,
}

impl<K, T, E> Default for CachedFutures<K, T, E>
where
    K: Hash + Eq + Clone,
    T: Clone,
    E: Clone,
{
    fn default() -> Self {
        Self {
            cache: Default::default(),
        }
    }
}
impl<K, T, E> Debug for CachedFutures<K, T, E>
where
    K: Hash + Eq + Clone,
    T: Clone,
    E: Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedFutures")
            .field("cache_size", &self.cache.read().len())
            .finish()
    }
}

impl<K, T, E> CachedFutures<K, T, E>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    E: Clone + Send + Sync + 'static,
{
    /// Given a key, tries to re-use the successful result of `resolver`, otherwise, will execute resolver,
    /// and cache its result if the result is Ok().
    pub async fn get_or_cache_default<F, U>(&self, key: K, resolver: F) -> Result<T, E>
    where
        F: FnOnce(K) -> U,
        U: Future<Output = Result<T, E>> + Send + 'static,
    {
        // Check to see if we already have a cached value - fast path with the read lock!
        let maybe_pending_future = match self.cache.read().get(&key) {
            Some(CachedFuture::Ready(value)) => return Ok(value.clone()),
            Some(CachedFuture::Pending(future)) => Some(future.clone()),
            None => None,
        };

        let future = match maybe_pending_future {
            Some(future) => future,
            None => {
                // The future does not exist yet, so now we're going to acquire a write lock, so we can start resolution:
                let mut write_locked_cache = self.cache.write();

                // Repeat the read check first, a writer may have raced us and inserted the future:
                match write_locked_cache.get(&key) {
                    Some(CachedFuture::Ready(value)) => return Ok(value.clone()),
                    Some(CachedFuture::Pending(future)) => future.clone(),
                    None => {
                        let future = self.create_resolver_future(resolver, &key);
                        write_locked_cache.insert(key, CachedFuture::Pending(future.clone()));
                        future
                    }
                }
            }
        };

        future.await
    }

    /// Reduces over the cached futures, allowing one to introspect all of the resolved future values, or which futures are pending.
    pub fn fold<I, F>(&self, mut init: I, reducer: F) -> I
    where
        F: Fn(I, (&K, CachedFutureRef<'_, T>)) -> I,
    {
        let cache = self.cache.read();
        for (k, v) in cache.iter() {
            let future_ref = match v {
                CachedFuture::Ready(v) => CachedFutureRef::Ready(v),
                CachedFuture::Pending(_) => CachedFutureRef::Pending,
            };

            init = (reducer)(init, (k, future_ref));
        }

        init
    }

    /// Reduces over the ready cached futures, allowing one to introspect all of the resolved future values.
    pub fn fold_ready<I, F>(&self, init: I, reducer: F) -> I
    where
        F: Fn(I, (&K, &T)) -> I,
    {
        self.fold(init, |init, (k, v)| match v {
            CachedFutureRef::Ready(v) => (reducer)(init, (k, v)),
            CachedFutureRef::Pending => init,
        })
    }

    // drop a cached future
    pub fn drop(&self, key: &K) {
        self.cache.write().remove(key);
    }

    /// Iterates over all the *ready* cached futures, retaining ones that are match the predicate function, and returning the number
    /// of futures which have been disposed of (where the predicate function has returned false).
    pub fn retain_ready<F>(&self, mut f: F) -> usize
    where
        F: FnMut(&T) -> bool,
    {
        let mut cache = self.cache.write();
        let mut num_disposed = 0;
        cache.retain(|_, v| {
            match v {
                // Always retain pending entries, as they are in-flight.
                CachedFuture::Pending(_) => true,
                CachedFuture::Ready(r) => {
                    let should_retain = (f)(r);

                    if !should_retain {
                        num_disposed += 1;
                    }

                    should_retain
                }
            }
        });

        num_disposed
    }

    fn create_resolver_future<F, U>(
        &self,
        resolver: F,
        key: &K,
    ) -> Shared<BoxFuture<'static, Result<T, E>>>
    where
        F: FnOnce(K) -> U,
        U: Future<Output = Result<T, E>> + Send + 'static,
    {
        let resolver_future = (resolver)(key.clone());
        let cache = self.cache.clone();
        let key_clone = key.clone();
        async move {
            let result = resolver_future.await;
            match result {
                Ok(result) => {
                    // If the result was a success, we'll cache that result now.
                    cache
                        .write()
                        .insert(key_clone, CachedFuture::Ready(result.clone()));
                    Ok(result)
                }
                Err(error) => {
                    // We will not cache errors!
                    cache.write().remove(&key_clone);
                    Err(error)
                }
            }
        }
        .boxed()
        .shared()
    }
}
