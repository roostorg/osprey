use std::{
    borrow::Cow,
    collections::{hash_map::Entry, HashMap, HashSet},
    hash::Hash,
};

use fxhash::{FxHashMap, FxHashSet};
use parking_lot::RwLock;

use super::DynamicTagValue;

/// A string interner is responsible for taking a String, and turning it into a &'static str. It does so by leaking
/// the String using [`Box::leak`], and then caching that leaked string, so when the same string is provided, it'll
/// dispense an already interned leaked &'static str. There are two kinds of interners that can be used:
///  - [`Simple`]: Simply interns the provided string as is.
///  - [`Transformed`]: transforms the string and caches the transformed result.
pub struct StringInterner<T = Simple> {
    inner: RwLock<T>,
    max_capacity: usize,
}

/// A [`InternerImpl`] which memoizes the transformation to a &'static str. This is useful in places where we might
/// take a string and transform it for use as a metrics tag. The transformation function should be deterministic and
/// return the same outputs for the same inputs always.
pub struct Transformed {
    interned: FxHashMap<String, &'static str>,
    transformer: Box<dyn Fn(&str) -> String + Send + Sync>,
}

/// A [`InternerImpl`] which simply interns the string provided as is.
pub struct Simple {
    interned: FxHashSet<&'static str>,
}

/// The result of [`StringInterner::intern`].
#[derive(Debug, Eq, PartialEq, Hash, Clone, Copy)]
pub enum InternResult {
    /// The string interning was successful. This makes no distinguishment for if the string was already interned
    /// prior to this call, or if it was just interned now.
    Interned(&'static str),
    /// The interner cache is at capacity, and can no longer intern any new strings.
    AtCapacity,
}

impl DynamicTagValue for InternResult {
    fn as_static_str(&self) -> &'static str {
        match self {
            InternResult::Interned(s) => *s,
            InternResult::AtCapacity => "string_interner_at_capacity",
        }
    }
}

/// The trait that interners must implement.
pub trait InternerImpl {
    /// Try to get the interned string from the cache, returning None if the cache did not have the interned value.
    fn try_intern(&self, string: &str) -> Option<&'static str>;
    /// Interns the string and inserts it into the cache. If the string is already interned,
    /// it will not modify the cache.
    fn intern(&mut self, string: String) -> &'static str;
    /// How many strings is this interner currently interning.
    fn len(&self) -> usize;
    /// Is this interner currently interning zero strings?
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl InternerImpl for Transformed {
    fn len(&self) -> usize {
        self.interned.len()
    }

    fn try_intern(&self, string: &str) -> Option<&'static str> {
        self.interned.get(string).copied()
    }

    fn intern(&mut self, string: String) -> &'static str {
        match self.interned.entry(string) {
            Entry::Occupied(e) => e.get(),
            Entry::Vacant(v) => {
                let transformed = Box::leak((self.transformer)(v.key()).into_boxed_str());
                v.insert(transformed)
            }
        }
    }
}

impl InternerImpl for Simple {
    fn len(&self) -> usize {
        self.interned.len()
    }

    fn try_intern(&self, string: &str) -> Option<&'static str> {
        self.interned.get(string).copied()
    }

    fn intern(&mut self, string: String) -> &'static str {
        if let Some(s) = self.try_intern(&string) {
            return s;
        }
        let leaked = Box::leak(string.into_boxed_str());
        self.interned.insert(leaked);
        leaked
    }
}

impl StringInterner<Transformed> {
    /// Creates a new [`StringInterner`] that uses the [`Transformed`] interner implementation. For more details, see
    /// the docs for [`Transformed`].
    ///
    /// # Example:
    ///
    /// ```rust
    /// # use crate::metrics::{StringInterner, InternResult};
    /// let interner = StringInterner::with_transformer(512, |s| s.to_uppercase());
    /// assert_eq!(interner.intern("hello"), InternResult::Interned("HELLO"));
    /// ```
    pub fn with_transformer<F>(max_capacity: usize, transformer: F) -> Self
    where
        F: Fn(&str) -> String + Send + Sync + 'static,
    {
        Self {
            max_capacity,
            inner: RwLock::new(Transformed {
                interned: HashMap::with_capacity_and_hasher(max_capacity, Default::default()),
                transformer: Box::new(transformer),
            }),
        }
    }
}

impl StringInterner<Simple> {
    /// Creates a new [`StringInterner`] that uses the [`Simple`] interner implementation. For more details, see
    /// the docs for [`Simple`].
    ///
    /// # Example:
    ///
    /// ```rust
    /// # use crate::metrics::{StringInterner, InternResult};
    /// let interner = StringInterner::new(1);
    /// assert_eq!(interner.intern("hello"), InternResult::Interned("hello"));
    /// assert_eq!(interner.intern("world"), InternResult::AtCapacity);
    /// ```
    pub fn new(max_capacity: usize) -> Self {
        Self {
            max_capacity,
            inner: RwLock::new(Simple {
                interned: HashSet::with_capacity_and_hasher(max_capacity, Default::default()),
            }),
        }
    }
}

impl<T> StringInterner<T>
where
    T: InternerImpl,
{
    /// Interns the given string.
    ///
    /// This function takes a String or &str (basically something that can be turned into a Cow<str>). If you can
    /// provide an owned string, this is usually the best, as we won't have to clone the string when we're interning it,
    /// however, if you can't, a borrowed string is fine too, and you'll only pay the cost of cloning the string the
    /// first time its interned.
    pub fn intern<'a>(&self, string: impl Into<Cow<'a, str>>) -> InternResult {
        let string: Cow<'a, str> = string.into();

        {
            let read_only_interner = self.inner.read();
            if let Some(interned) = read_only_interner.try_intern(&string) {
                return InternResult::Interned(interned);
            }

            // If the interner is at capacity, bail early.
            if read_only_interner.len() >= self.max_capacity {
                return InternResult::AtCapacity;
            }
        }

        let mut interner = self.inner.write();
        // Check the length again, if we're full, bail before we try interning.
        if interner.len() >= self.max_capacity {
            return InternResult::AtCapacity;
        }

        InternResult::Interned(interner.intern(string.into_owned()))
    }
}

/// Cache of strings for types that have a [`ToString`] implementation.
#[derive(Debug)]
pub struct ToStringInterner<T> {
    inner: RwLock<FxHashMap<T, &'static str>>,
    max_capacity: usize,
}

impl<T: Hash + Eq + ToString> ToStringInterner<T> {
    /// Create a new interner with the given capacity.
    pub fn new(max_capacity: usize) -> Self {
        ToStringInterner {
            inner: RwLock::new(FxHashMap::with_capacity_and_hasher(
                max_capacity,
                Default::default(),
            )),
            max_capacity,
        }
    }

    /// Returns a static string for the given integer.
    pub fn intern(&self, item: T) -> InternResult {
        // Fast path: number already interned.
        {
            let map = self.inner.read();
            if let Some(s) = map.get(&item).copied() {
                return InternResult::Interned(s);
            }

            // If the interner is at capacity, bail and don't acquire write lock.
            if map.len() >= self.max_capacity {
                return InternResult::AtCapacity;
            }
        }

        let stringified_item = item.to_string(); // Perform string conversion outside critical section.
        let mut map = self.inner.write();
        if map.len() >= self.max_capacity {
            return InternResult::AtCapacity;
        }
        InternResult::Interned(
            map.entry(item)
                .or_insert_with(|| Box::leak(stringified_item.into_boxed_str())),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_string_interner_simple_case() {
        let interner: ToStringInterner<u64> = ToStringInterner::new(2);

        // Inserts a new value
        assert_eq!(interner.intern(1).as_static_str(), "1");
        assert_eq!(interner.inner.read().len(), 1);

        // Reads the previously-inserted value
        assert_eq!(interner.intern(1).as_static_str(), "1");
        assert_eq!(interner.inner.read().len(), 1);

        // Inserts a new value
        assert_eq!(interner.intern(2).as_static_str(), "2");
        assert_eq!(interner.inner.read().len(), 2);

        // The original value is still correct
        assert_eq!(interner.intern(1).as_static_str(), "1");
    }

    #[test]
    fn test_to_string_interner_over_capacity() {
        let interner: ToStringInterner<u64> = ToStringInterner::new(2);

        assert_eq!(interner.intern(1).as_static_str(), "1");
        assert_eq!(interner.intern(2).as_static_str(), "2");
        assert_eq!(
            interner.intern(3).as_static_str(),
            "string_interner_at_capacity"
        );
    }
}
