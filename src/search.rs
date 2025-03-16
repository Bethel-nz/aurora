//! # Aurora Full-Text Search
//! 
//! This module provides full-text search capabilities for Aurora documents.
//! It implements an inverted index with BM25-inspired ranking and fuzzy matching support.
//! 
//! ## Features
//! 
//! * **BM25 Ranking**: Documents are scored by relevance using a BM25-inspired algorithm
//! * **Fuzzy Matching**: Find words with typos or spelling variations
//! * **Custom Stopwords**: Add your own stopwords for filtering if needed
//! * **Unicode Support**: Properly handles different languages and character sets
//! 
//! ## Example
//! 
//! ```rust
//! // Basic search
//! let results = db.search("products")
//!     .field("description")
//!     .matching("wireless headphones")
//!     .collect()
//!     .await?;
//!     
//! // With fuzzy matching for typo tolerance
//! let fuzzy_results = db.search("products")
//!     .field("description")
//!     .matching("wireles headphnes")
//!     .fuzzy(true)
//!     .collect()
//!     .await?;
//! ```

use crate::error::Result;
use crate::types::{Document, Value};
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use unicode_segmentation::UnicodeSegmentation;

/// Full-text search index for a collection and field
///
/// This struct maintains an inverted index mapping terms to the documents
/// that contain them, with term frequency information for relevance scoring.
#[allow(dead_code)]
pub struct FullTextIndex {
    /// Name of the collection being indexed
    field: String,
    index: Arc<DashMap<String, Vec<(String, f32)>>>, // term -> [(doc_id, score)]
    document_count: Arc<std::sync::atomic::AtomicUsize>,
    stop_words: HashSet<String>,
    enable_stop_words: bool,
}

impl FullTextIndex {
    /// Create a new full-text index for a collection field
    ///
    /// # Arguments
    /// * `collection` - Name of the collection to index
    /// * `field` - Name of the field within documents to index
    ///
    /// # Examples
    /// ```
    /// let description_index = FullTextIndex::new("products", "description");
    /// ```
    pub fn new(_collection: &str, field: &str) -> Self {
        Self {
            field: field.to_string(),
            index: Arc::new(DashMap::new()),
            document_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            stop_words: HashSet::new(),
            enable_stop_words: false,
        }
    }

    /// Create a search index with specific options
    ///
    /// # Arguments
    /// * `collection` - Name of the collection to index
    /// * `field` - Name of the field within documents to index
    /// * `enable_stop_words` - Whether to filter out common words (false = no filtering)
    ///
    /// # Examples
    /// ```
    /// // Create an index that doesn't filter out common words
    /// let index = FullTextIndex::with_options("products", "description", false);
    /// 
    /// // Create an index that does filter out common words
    /// let index = FullTextIndex::with_options("articles", "content", true);
    /// ```
    #[allow(dead_code)]
    pub fn with_options(collection: &str, field: &str, enable_stop_words: bool) -> Self {
        let mut index = Self::new(collection, field);
        index.enable_stop_words = enable_stop_words;
        index
    }

    /// Set whether to filter out common stop words
    ///
    /// # Arguments
    /// * `enable` - true to filter out common words, false to include all words
    ///
    /// # Examples
    /// ```
    /// // Enable filtering of common words like "the", "and", etc.
    /// index.set_stop_words_filter(true);
    /// 
    /// // Disable filtering to allow searching for all words
    /// index.set_stop_words_filter(false);
    /// ```
    #[allow(dead_code)]
    pub fn set_stop_words_filter(&mut self, enable: bool) {
        self.enable_stop_words = enable;
    }

    /// Add or update a document in the index
    ///
    /// # Arguments
    /// * `doc` - The document to index
    ///
    /// # Returns
    /// A Result indicating success or an error
    ///
    /// # Examples
    /// ```
    /// index.index_document(&document)?;
    /// ```
    pub fn index_document(&self, doc: &Document) -> Result<()> {
        if let Some(Value::String(text)) = doc.data.get(&self.field) {
            let terms = self.tokenize(text);
            if !terms.is_empty() {
                self.document_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            
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

    /// Search the index for documents matching a query
    ///
    /// # Arguments
    /// * `query` - The search query text
    ///
    /// # Returns
    /// A vector of document IDs and relevance scores
    ///
    /// # Examples
    /// ```
    /// let matches = index.search("wireless headphones");
    /// ```
    pub fn search(&self, query: &str) -> Vec<(String, f32)> {
        let query_terms = self.tokenize(query);
        if query_terms.is_empty() {
            return Vec::new();
        }
        
        let mut scores: HashMap<String, f32> = HashMap::new();
        let total_docs = self.document_count.load(std::sync::atomic::Ordering::Relaxed).max(1) as f32;

        for term in query_terms {
            let Some(term_entry) = self.index.get(&term) else { continue };
            
            // BM25-inspired scoring (simpler than full BM25)
            let doc_freq = term_entry.len() as f32;
            let idf = (1.0 + (total_docs - doc_freq + 0.5) / (doc_freq + 0.5)).ln();
            
            term_entry.iter().for_each(|(doc_id, tf)| {
                // Apply BM25-like k1 and b parameters (simplified)
                let k1 = 1.2;
                let tf_adjusted = ((k1 + 1.0) * tf) / (k1 + tf);
                
                *scores.entry(doc_id.clone()).or_insert(0.0) += tf_adjusted * idf;
            });
        }

        let mut results: Vec<_> = scores.into_iter().collect();
        results.sort_by(|(_, score1), (_, score2)| score2.partial_cmp(score1).unwrap());
        results
    }

    /// Search with fuzzy matching for typo tolerance
    ///
    /// # Arguments
    /// * `query` - The search query text
    /// * `max_distance` - Maximum edit distance for fuzzy matching
    ///
    /// # Returns
    /// A vector of document IDs and relevance scores
    ///
    /// # Examples
    /// ```
    /// // Allow up to 2 character differences
    /// let matches = index.fuzzy_search("wireles headpones", 2);
    /// ```
    #[allow(dead_code)]
    pub fn fuzzy_search(&self, query: &str, max_distance: usize) -> Vec<(String, f32)> {
        let query_terms = self.tokenize(query);
        if query_terms.is_empty() {
            return Vec::new();
        }
        
        let mut scores: HashMap<String, f32> = HashMap::new();
        
        // Get all indexed terms
        let all_terms: Vec<String> = self.index.iter().map(|e| e.key().clone()).collect();
        
        for query_term in query_terms {
            // Find fuzzy matches
            for indexed_term in &all_terms {
                if query_term.len() > 3 && self.levenshtein_distance(&query_term, indexed_term) <= max_distance {
                    if let Some(matches) = self.index.get(indexed_term) {
                        // Discount score based on distance
                        let distance = self.levenshtein_distance(&query_term, indexed_term) as f32;
                        let distance_factor = 1.0 / (1.0 + distance);
                        
                        for (doc_id, score) in matches.iter() {
                            *scores.entry(doc_id.clone()).or_insert(0.0) += score * distance_factor;
                        }
                    }
                }
            }
        }
        
        let mut results: Vec<_> = scores.into_iter().collect();
        results.sort_by(|(_, score1), (_, score2)| score2.partial_cmp(score1).unwrap());
        results
    }

    /// Break text into tokens (words) for indexing or searching
    ///
    /// # Arguments
    /// * `text` - The text to tokenize
    ///
    /// # Returns
    /// A vector of token strings
    fn tokenize(&self, text: &str) -> Vec<String> {
        let mut tokens: Vec<String> = text.unicode_words()
            .map(|word| word.to_lowercase())
            .collect();
            
        // Filter out stop words if enabled
        if self.enable_stop_words {
            tokens.retain(|token| !self.stop_words.contains(token));
        }
        
        tokens
    }

    /// Calculate term frequencies for a set of terms
    ///
    /// # Returns
    /// A map of term to frequency
    fn calculate_term_frequencies(&self, terms: &[String]) -> HashMap<String, f32> {
        let mut freq = HashMap::new();
        for term in terms {
            *freq.entry(term.clone()).or_insert(0.0) += 1.0;
        }
        freq
    }

    /// Get the total number of documents in the index
    ///
    /// # Returns
    /// The number of documents indexed
    #[allow(dead_code)]
    fn get_total_docs(&self) -> usize {
        self.document_count.load(std::sync::atomic::Ordering::Relaxed)
    }
    
    /// Calculate Levenshtein distance between two strings
    ///
    /// Used for fuzzy matching to handle typos and spelling variations
    ///
    /// # Arguments
    /// * `s1` - First string
    /// * `s2` - Second string
    ///
    /// # Returns
    /// The edit distance (number of changes needed to transform s1 into s2)
    fn levenshtein_distance(&self, s1: &str, s2: &str) -> usize {
        let s1_len = s1.len();
        let s2_len = s2.len();
        
        // Early return for edge cases
        if s1_len == 0 { return s2_len; }
        if s2_len == 0 { return s1_len; }
        
        let mut prev_row = (0..=s2_len).collect::<Vec<_>>();
        let mut curr_row = vec![0; s2_len + 1];
        
        for i in 1..=s1_len {
            curr_row[0] = i;
            
            for j in 1..=s2_len {
                let cost = if s1.chars().nth(i-1) == s2.chars().nth(j-1) { 0 } else { 1 };
                curr_row[j] = std::cmp::min(
                    curr_row[j-1] + 1,  // insertion
                    std::cmp::min(
                        prev_row[j] + 1,  // deletion
                        prev_row[j-1] + cost  // substitution
                    )
                );
            }
            
            // Swap rows
            std::mem::swap(&mut prev_row, &mut curr_row);
        }
        
        prev_row[s2_len]
    }
    
    /// Remove a document from the index
    ///
    /// # Arguments
    /// * `doc_id` - ID of the document to remove
    ///
    /// # Examples
    /// ```
    /// index.remove_document("doc-123");
    /// ```
    #[allow(dead_code)]
    pub fn remove_document(&self, doc_id: &str) {
        // Remove the document from all term entries
        for mut entry in self.index.iter_mut() {
            let doc_entries = entry.value_mut();
            let original_len = doc_entries.len();
            doc_entries.retain(|(id, _)| id != doc_id);
            
            // If we removed an entry, decrement the document count
            if doc_entries.len() < original_len {
                self.document_count.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                // Only count the document once
                break;
            }
        }
    }

    /// Add custom stopwords to the filter list
    ///
    /// # Arguments
    /// * `words` - List of words to add to the stopword list
    ///
    /// # Examples
    /// ```
    /// // Add domain-specific stopwords
    /// index.add_stopwords(&["widget", "product", "item"]);
    /// ```
    #[allow(dead_code)]
    pub fn add_stopwords(&mut self, words: &[&str]) {
        for word in words {
            self.stop_words.insert(word.to_string());
        }
    }

    /// Remove all stopwords from the filter
    ///
    /// # Examples
    /// ```
    /// // Allow searching for any word, including common ones
    /// index.clear_stopwords();
    /// ```
    #[allow(dead_code)]
    pub fn clear_stopwords(&mut self) {
        self.stop_words.clear();
    }

    /// Get the current list of stopwords
    ///
    /// # Returns
    /// A vector of stopwords currently in use
    ///
    /// # Examples
    /// ```
    /// let current_stopwords = index.get_stopwords();
    /// println!("Currently filtering: {:?}", current_stopwords);
    /// ```
    #[allow(dead_code)]
    pub fn get_stopwords(&self) -> Vec<String> {
        self.stop_words.iter().cloned().collect()
    }

    #[allow(dead_code)]
    #[allow(unused_variables)]
    pub fn get_fuzzy_matches(&self, query: &str, max_distance: u32) -> Vec<(String, f32)> {
        // ...
        Vec::new()
    }
    
    #[allow(dead_code)]
    #[allow(unused_variables)]
    pub fn highlight_matches(&self, text: &str, query: &str) -> String {
        // ...
        String::new()
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