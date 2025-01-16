use crate::types::Document;

// Keep only the basic types and traits needed for the new closure-based system
pub trait Queryable {
    fn matches(&self, doc: &Document) -> bool;
}

impl<F> Queryable for F
where
    F: Fn(&Document) -> bool
{
    fn matches(&self, doc: &Document) -> bool {
        self(doc)
    }
} 