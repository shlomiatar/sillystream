//! Filters decide on a record's header only (SPEC section 4).

use crate::{BlockAuthor, BlockSummary, Decision, FormatError, RecordHeader};

pub const MAX_CLAUSES: usize = 4;
/// Wire code for `eq` (D1).
pub const OP_EQ: u8 = 0;
/// Wire code for `any` (D1).
pub const OP_ANY: u8 = 1;

/// One options clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Clause {
    /// `(options & mask) == value`
    Eq { mask: u64, value: u64 },
    /// `(options & mask) != 0`
    Any { mask: u64 },
}

impl Clause {
    /// From the frame's `mask`, `value` and `op`. `any` has no use for `value`.
    pub fn from_wire(mask: u64, value: u64, op: u8) -> Result<Clause, FormatError> {
        match op {
            OP_EQ => Ok(Clause::Eq { mask, value }),
            OP_ANY => Ok(Clause::Any { mask }),
            _ => Err(FormatError::UnknownClauseOp),
        }
    }

    #[inline]
    pub fn matches(&self, options: u64) -> bool {
        match *self {
            Clause::Eq { mask, value } => options & mask == value,
            Clause::Any { mask } => options & mask != 0,
        }
    }

    /// The rules of section 4.1, over a block's `options_or` and `options_and`.
    ///
    /// An `eq` clause whose value has bits outside its mask can never match any
    /// record, which the section 4.1 formula alone would miss (D33).
    pub fn decide(&self, options_or: u64, options_and: u64) -> Decision {
        match *self {
            Clause::Eq { mask, value } => {
                if value & !mask != 0 {
                    return Decision::None;
                }
                let need1 = mask & value;
                let need0 = mask & !value;
                if need1 & !options_or != 0 || need0 & options_and != 0 {
                    Decision::None
                } else if need1 & !options_and == 0 && need0 & options_or == 0 {
                    Decision::All
                } else {
                    Decision::Maybe
                }
            }
            Clause::Any { mask } => {
                if options_or & mask == 0 {
                    Decision::None
                } else if options_and & mask != 0 {
                    Decision::All
                } else {
                    Decision::Maybe
                }
            }
        }
    }
}

/// A complete filter: up to 4 clauses OR-ed, an author, an id range, and whether
/// system records are wanted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Filter {
    clauses: [Clause; MAX_CLAUSES],
    clause_count: u8,
    /// Equality on `author`.
    pub author: Option<u64>,
    /// `INCLUDE_SYSTEM`.
    pub include_system: bool,
    /// Lowest id wanted, inclusive. A record overlapping the range matches whole
    /// (section 5.2).
    pub id_first: u64,
    /// Highest id wanted, inclusive.
    pub id_last: u64,
}

impl Filter {
    /// No clauses match every record (D2), and the id range starts out unbounded.
    pub fn new(clauses: &[Clause]) -> Result<Filter, FormatError> {
        if clauses.len() > MAX_CLAUSES {
            return Err(FormatError::TooManyClauses);
        }
        // Unused slots hold one fixed value so that equal filters compare and
        // hash equal, which the filter table relies on.
        let mut slots = [Clause::Any { mask: 0 }; MAX_CLAUSES];
        slots[..clauses.len()].copy_from_slice(clauses);
        Ok(Filter {
            clauses: slots,
            clause_count: clauses.len() as u8,
            author: None,
            include_system: false,
            id_first: 0,
            id_last: u64::MAX,
        })
    }

    pub fn clauses(&self) -> &[Clause] {
        &self.clauses[..self.clause_count as usize]
    }

    #[inline]
    fn options_match(&self, options: u64) -> bool {
        let clauses = self.clauses();
        clauses.is_empty() || clauses.iter().any(|clause| clause.matches(options))
    }

    /// Per-record evaluation. The id range applies to every record. Options and
    /// author don't apply to system records: `include_system` alone decides (D3).
    #[inline]
    pub fn matches(&self, header: &RecordHeader) -> bool {
        if header.id > self.id_last || header.last_id() < self.id_first {
            return false;
        }
        if header.is_system() {
            return self.include_system;
        }
        if let Some(author) = self.author
            && author != header.author
        {
            return false;
        }
        self.options_match(header.options)
    }

    /// Decides a whole block from its summary, without opening it.
    ///
    /// `Decision::None` means no record in the block matches, and `All` that
    /// every record does. `options_or` and `options_and` cover system records
    /// too, whose options are entry types, which is why a block that may hold
    /// system records can't be `None` for a system reader (D3) nor `All` for
    /// anyone else.
    pub fn decide_block(&self, summary: &BlockSummary) -> Decision {
        if summary.count == 0 || summary.id_first > self.id_last || summary.id_last < self.id_first
        {
            return Decision::None;
        }
        let in_range = summary.id_first >= self.id_first && summary.id_last <= self.id_last;
        let decision = self.decide_contents(summary);
        if decision == Decision::All && !in_range {
            return Decision::Maybe;
        }
        decision
    }

    fn decide_contents(&self, summary: &BlockSummary) -> Decision {
        let options = self.decide_options(summary.options_or, summary.options_and);
        match summary.author {
            BlockAuthor::Single(0) => {
                if self.include_system {
                    Decision::All
                } else {
                    Decision::None
                }
            }
            BlockAuthor::Single(author) => {
                if self.author.is_some_and(|wanted| wanted != author) {
                    Decision::None
                } else {
                    options
                }
            }
            BlockAuthor::Mixed => match options {
                Decision::None if !self.include_system => Decision::None,
                // Authors differ, or system records may be present and not wanted.
                Decision::All if self.include_system && self.author.is_none() => Decision::All,
                _ => Decision::Maybe,
            },
        }
    }

    /// Clauses are OR-ed: `All` if any clause is, `None` if every clause is.
    fn decide_options(&self, options_or: u64, options_and: u64) -> Decision {
        let clauses = self.clauses();
        if clauses.is_empty() {
            return Decision::All;
        }
        let mut result = Decision::None;
        for clause in clauses {
            match clause.decide(options_or, options_and) {
                Decision::All => return Decision::All,
                Decision::Maybe => result = Decision::Maybe,
                Decision::None => {}
            }
        }
        result
    }
}
