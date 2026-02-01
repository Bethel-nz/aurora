use crate::error::{AqlError, ErrorCode, Result};
use crate::parser::ast;

/// Filter with compiled regexes for performance
#[derive(Debug, Clone)]
pub enum CompiledFilter {
    Eq(String, ast::Value),
    Ne(String, ast::Value),
    Gt(String, ast::Value),
    Gte(String, ast::Value),
    Lt(String, ast::Value),
    Lte(String, ast::Value),
    In(String, ast::Value),
    NotIn(String, ast::Value),
    Contains(String, ast::Value),
    StartsWith(String, ast::Value),
    EndsWith(String, ast::Value),
    Matches(String, regex::Regex),
    IsNull(String),
    IsNotNull(String),
    And(Vec<CompiledFilter>),
    Or(Vec<CompiledFilter>),
    Not(Box<CompiledFilter>),
}

/// Compile an AQL filter into a CompiledFilter
pub fn compile_filter(filter: &ast::Filter) -> Result<CompiledFilter> {
    match filter {
        ast::Filter::Eq(f, v) => Ok(CompiledFilter::Eq(f.clone(), v.clone())),
        ast::Filter::Ne(f, v) => Ok(CompiledFilter::Ne(f.clone(), v.clone())),
        ast::Filter::Gt(f, v) => Ok(CompiledFilter::Gt(f.clone(), v.clone())),
        ast::Filter::Gte(f, v) => Ok(CompiledFilter::Gte(f.clone(), v.clone())),
        ast::Filter::Lt(f, v) => Ok(CompiledFilter::Lt(f.clone(), v.clone())),
        ast::Filter::Lte(f, v) => Ok(CompiledFilter::Lte(f.clone(), v.clone())),
        ast::Filter::In(f, v) => Ok(CompiledFilter::In(f.clone(), v.clone())),
        ast::Filter::NotIn(f, v) => Ok(CompiledFilter::NotIn(f.clone(), v.clone())),
        ast::Filter::Contains(f, v) => Ok(CompiledFilter::Contains(f.clone(), v.clone())),
        ast::Filter::StartsWith(f, v) => Ok(CompiledFilter::StartsWith(f.clone(), v.clone())),
        ast::Filter::EndsWith(f, v) => Ok(CompiledFilter::EndsWith(f.clone(), v.clone())),
        ast::Filter::Matches(f, v) => {
            if let ast::Value::String(pattern) = v {
                // Use RegexBuilder with size limits to prevent ReDoS attacks
                let re = regex::RegexBuilder::new(pattern)
                    .size_limit(10_000_000) // 10MB compiled regex size limit
                    .dfa_size_limit(2_000_000) // 2MB DFA size limit
                    .build()
                    .map_err(|e| {
                        AqlError::new(
                            ErrorCode::SecurityError,
                            format!(
                                "Regex pattern '{}' is too complex or invalid: {}",
                                pattern, e
                            ),
                        )
                    })?;
                Ok(CompiledFilter::Matches(f.clone(), re))
            } else {
                Err(AqlError::new(
                    ErrorCode::InvalidInput,
                    "Matches filter requires a string pattern",
                ))
            }
        }
        ast::Filter::IsNull(f) => Ok(CompiledFilter::IsNull(f.clone())),
        ast::Filter::IsNotNull(f) => Ok(CompiledFilter::IsNotNull(f.clone())),
        ast::Filter::And(filters) => {
            let compiled = filters
                .iter()
                .map(compile_filter)
                .collect::<Result<Vec<_>>>()?;
            Ok(CompiledFilter::And(compiled))
        }
        ast::Filter::Or(filters) => {
            let compiled = filters
                .iter()
                .map(compile_filter)
                .collect::<Result<Vec<_>>>()?;
            Ok(CompiledFilter::Or(compiled))
        }
        ast::Filter::Not(filter) => {
            let compiled = compile_filter(filter)?;
            Ok(CompiledFilter::Not(Box::new(compiled)))
        }
    }
}
