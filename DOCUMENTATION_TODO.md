# Aurora Documentation Improvement Plan

## ✅ Completed (Top APIs in db.rs):

1. **Aurora::open()** - Already had examples ✓
2. **Aurora::with_config()** - Added comprehensive config example ✓
3. **Aurora::new_collection()** - Added schema & indexing examples ✓
4. **Aurora::get()** - Added low-level KV docs with performance notes ✓
5. **Aurora::put()** - Added storage docs with TTL examples ✓
6. **Aurora::query()** - Added comprehensive query docs with early termination notes ✓
7. **Aurora::listen()** - Added PubSub/reactive examples ✓
8. **Aurora::begin_transaction()** - Added transaction example ✓

## 🔴 Critical - Still Need Examples:

### query.rs - FilterBuilder Methods (ALL missing docs!)
```rust
// Lines 731-756 - Zero documentation
pub fn eq(self, field: &str, value: Value) -> Self
pub fn gt(self, field: &str, value: Value) -> Self  
pub fn gte(self, field: &str, value: Value) -> Self
pub fn lt(self, field: &str, value: Value) -> Self
pub fn lte(self, field: &str, value: Value) -> Self
pub fn contains(self, field: &str, value: &str) -> Self
```

**Suggested docs:**
```rust
/// Filter for exact equality
///
/// Uses secondary index if the field is indexed (O(1) lookup).
/// Falls back to full scan if not indexed.
///
/// # Examples
/// ```
/// db.query("users")
///     .filter(|f| f.eq("status", Value::String("active".into())))
///     .collect().await?;
/// ```
pub fn eq(self, field: &str, value: Value) -> Self

/// Filter for greater than
///
/// Always requires scanning (no B-tree indices yet).
/// With LIMIT, uses early termination for speed.
///
/// # Examples
/// ```
/// db.query("products")
///     .filter(|f| f.gt("price", Value::Float(99.99)))
///     .limit(10)  // Early termination!
///     .collect().await?;
/// ```
pub fn gt(self, field: &str, value: Value) -> Self
```

### db.rs - Missing Examples:

**Transaction Methods:**
- `commit_transaction()` - needs example
- `rollback_transaction()` - needs example

**Index Methods:**
- `create_index()` - needs example showing when/why to index

**Collection Methods:**
- `insert_into()` - has docs but example could be better
- `get_document()` - needs example
- `delete()` - needs example

## 🟡 Medium Priority:

**Export/Import:**
- `export_as_json()` - needs example
- `export_as_csv()` - needs example  
- `import_from_json()` - needs example

**Cache Management:**
- `flush()` - needs example showing when to use
- `get_cache_stats()` - needs example
- `prewarm_cache()` - needs example

**Batch Operations:**
- `batch_insert()` - needs example
- `batch_write()` - needs example

## 📝 Pattern for Good Documentation:

```rust
/// [One-line description of what it does]
///
/// [1-2 sentences explaining when/why to use it]
///
/// # Performance (if relevant)
/// - Key performance characteristics
/// - Complexity: O(?)
/// - When it's fast vs slow
///
/// # Examples
///
/// ```
/// // Simple case
/// let result = db.method(...)?;
///
/// // Complex case showing real-world usage
/// let result = db.method(...)
///     .chain()
///     .with_options()
///     .execute()?;
///
/// // Edge case or gotcha
/// // Note: XYZ behaves differently when...
/// ```
///
/// # See Also (if relevant)
/// - Related methods
/// - Alternative approaches
```

## 🎯 Next Steps:

1. **Phase 1:** Document FilterBuilder methods (most used, zero docs!)
2. **Phase 2:** Add transaction examples (commit/rollback)
3. **Phase 3:** Add collection operation examples (insert/get/delete)
4. **Phase 4:** Add export/import examples
5. **Phase 5:** Add cache management examples

## 💡 Quick Wins:

Many methods already have doc comments but just need examples added.
Search for: `⚠️  Line XXX: method_name - Missing usage example`

The audit found 47 such methods that just need examples added to existing docs.

---

**Total Impact:**
- 31 APIs with NO docs → High priority
- 47 APIs with docs but no examples → Medium priority
- = 78 improvements needed for complete documentation

