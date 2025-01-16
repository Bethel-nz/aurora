use crate::error::Result;
use crate::types::{Document, Value};
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use unicode_segmentation::UnicodeSegmentation;

#[derive(Debug)]
pub struct FullTextIndex {
    field: String,
    index: Arc<DashMap<String, Vec<(String, f32)>>>, // term -> [(doc_id, score)]
}

impl FullTextIndex {
    pub fn new(_collection: &str, field: &str) -> Self {
        Self {
            field: field.to_string(),
            index: Arc::new(DashMap::new()),
        }
    }

    pub fn index_document(&self, doc: &Document) -> Result<()> {
        if let Some(Value::String(text)) = doc.data.get(&self.field) {
            let terms = self.tokenize(text);
            let term_freq = self.calculate_term_frequencies(&terms);
            
            for (term, freq) in term_freq {
                let score = freq / terms.len() as f32; // TF score
                self.index
                    .entry(term)
                    .or_default()
                    .push((doc.id.clone(), score));
            }
        }
        Ok(())
    }

    pub fn search(&self, query: &str) -> Vec<(String, f32)> {
        let query_terms = self.tokenize(query);
        let mut scores: HashMap<String, f32> = HashMap::new();
        let total_docs = self.get_total_docs() as f32;

        for term in query_terms {
            let Some(term_entry) = self.index.get(&term) else { continue };
            let idf = (total_docs / term_entry.len() as f32).ln();
            
            term_entry.iter().for_each(|(doc_id, tf)| {
                *scores.entry(doc_id.clone()).or_insert(0.0) += tf * idf;
            });
        }

        let mut results: Vec<_> = scores.into_iter().collect();
        results.sort_by(|(_, score1), (_, score2)| score2.partial_cmp(score1).unwrap());
        results
    }

    fn tokenize(&self, text: &str) -> Vec<String> {
        text.unicode_words()
            .map(|word| word.to_lowercase())
            .collect()
    }

    fn calculate_term_frequencies(&self, terms: &[String]) -> HashMap<String, f32> {
        let mut freq = HashMap::new();
        for term in terms {
            *freq.entry(term.clone()).or_insert(0.0) += 1.0;
        }
        freq
    }

    fn get_total_docs(&self) -> usize {
        self.index
            .iter()
            .flat_map(|entry| {
                // Clone the doc_ids to avoid borrowing issues
                entry.value()
                    .iter()
                    .map(|(doc_id, _)| doc_id.clone())
                    .collect::<Vec<_>>()
            })
            .collect::<std::collections::HashSet<_>>()
            .len()
            .max(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_full_text_index() -> Result<()> {
        let index = FullTextIndex::new("test_collection", "content");

        // Test document indexing
        let doc1 = Document {
            id: "1".to_string(),
            data: {
                let mut map = HashMap::new();
                map.insert("content".to_string(), Value::String("The quick brown fox".to_string()));
                map
            },
        };

        let doc2 = Document {
            id: "2".to_string(),
            data: {
                let mut map = HashMap::new();
                map.insert("content".to_string(), Value::String("The lazy brown dog".to_string()));
                map
            },
        };

        index.index_document(&doc1)?;
        index.index_document(&doc2)?;

        // Test search functionality
        let results = index.search("brown");
        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|(id, _)| id == "1"));
        assert!(results.iter().any(|(id, _)| id == "2"));

        let results = index.search("quick");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "1");

        Ok(())
    }

    #[test]
    fn test_tokenization() -> Result<()> {
        let index = FullTextIndex::new("test", "content");
        let tokens = index.tokenize("The Quick-Brown FOX!");
        
        assert_eq!(tokens, vec![
            "the".to_string(),
            "quick".to_string(),
            "brown".to_string(),
            "fox".to_string(),
        ]);

        Ok(())
    }
} 