use std::collections::HashMap;

use gitstratum_core::Oid;

#[derive(Debug, Clone)]
pub struct BaseCandidate {
    pub oid: Oid,
    pub size: usize,
    pub similarity_score: f64,
}

pub struct BaseSelector {
    max_candidates: usize,
    min_similarity: f64,
    max_base_size_ratio: f64,
}

impl BaseSelector {
    pub fn new() -> Self {
        Self {
            max_candidates: 10,
            min_similarity: 0.5,
            max_base_size_ratio: 2.0,
        }
    }

    pub fn with_config(
        max_candidates: usize,
        min_similarity: f64,
        max_base_size_ratio: f64,
    ) -> Self {
        Self {
            max_candidates,
            min_similarity,
            max_base_size_ratio,
        }
    }

    pub fn find_best_base(
        &self,
        target: &[u8],
        candidates: &[(Oid, &[u8])],
    ) -> Option<BaseCandidate> {
        let candidates = self.rank_candidates(target, candidates);
        candidates.into_iter().next()
    }

    pub fn rank_candidates(
        &self,
        target: &[u8],
        candidates: &[(Oid, &[u8])],
    ) -> Vec<BaseCandidate> {
        let target_size = target.len();

        let mut scored: Vec<BaseCandidate> = candidates
            .iter()
            .filter(|(_, data)| {
                let ratio = data.len() as f64 / target_size as f64;
                ratio <= self.max_base_size_ratio && ratio >= 1.0 / self.max_base_size_ratio
            })
            .filter_map(|(oid, data)| {
                let similarity = self.compute_similarity(target, data);
                if similarity >= self.min_similarity {
                    Some(BaseCandidate {
                        oid: *oid,
                        size: data.len(),
                        similarity_score: similarity,
                    })
                } else {
                    None
                }
            })
            .collect();

        scored.sort_by(|a, b| {
            b.similarity_score
                .partial_cmp(&a.similarity_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        scored.truncate(self.max_candidates);
        scored
    }

    fn compute_similarity(&self, target: &[u8], candidate: &[u8]) -> f64 {
        let target_set: HashMap<u32, u32> = self.compute_ngrams(target, 4);
        let candidate_set: HashMap<u32, u32> = self.compute_ngrams(candidate, 4);

        let mut intersection = 0u64;
        let mut union = 0u64;

        for (ngram, &count) in &target_set {
            let candidate_count = candidate_set.get(ngram).copied().unwrap_or(0);
            intersection += std::cmp::min(count, candidate_count) as u64;
            union += std::cmp::max(count, candidate_count) as u64;
        }

        for (ngram, &count) in &candidate_set {
            if !target_set.contains_key(ngram) {
                union += count as u64;
            }
        }

        if union == 0 {
            0.0
        } else {
            intersection as f64 / union as f64
        }
    }

    fn compute_ngrams(&self, data: &[u8], n: usize) -> HashMap<u32, u32> {
        let mut ngrams = HashMap::new();
        if data.len() < n {
            return ngrams;
        }

        for window in data.windows(n) {
            let mut hash = 0u32;
            for (i, &byte) in window.iter().enumerate() {
                hash ^= (byte as u32) << ((i % 4) * 8);
            }
            *ngrams.entry(hash).or_insert(0) += 1;
        }

        ngrams
    }

    pub fn is_good_base(&self, target: &[u8], candidate: &[u8]) -> bool {
        let similarity = self.compute_similarity(target, candidate);
        similarity >= self.min_similarity
    }
}

impl Default for BaseSelector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base_selector_new() {
        let selector = BaseSelector::new();
        assert_eq!(selector.max_candidates, 10);
        assert_eq!(selector.min_similarity, 0.5);
    }

    #[test]
    fn test_base_selector_with_config() {
        let selector = BaseSelector::with_config(5, 0.7, 3.0);
        assert_eq!(selector.max_candidates, 5);
        assert_eq!(selector.min_similarity, 0.7);
        assert_eq!(selector.max_base_size_ratio, 3.0);
    }

    #[test]
    fn test_find_best_base_identical() {
        let selector = BaseSelector::new();
        let target = b"hello world hello world hello world";
        let oid = Oid::hash(target);

        let candidates = vec![(oid, target.as_slice())];
        let best = selector.find_best_base(target, &candidates);

        assert!(best.is_some());
        let best = best.unwrap();
        assert_eq!(best.oid, oid);
        assert!(best.similarity_score > 0.99);
    }

    #[test]
    fn test_find_best_base_similar() {
        let selector = BaseSelector::new();
        let target = b"the quick brown fox jumps over the lazy dog";
        let similar = b"the quick brown cat jumps over the lazy dog";
        let oid = Oid::hash(similar);

        let candidates = vec![(oid, similar.as_slice())];
        let best = selector.find_best_base(target, &candidates);

        assert!(best.is_some());
        let best = best.unwrap();
        assert!(best.similarity_score > 0.5);
    }

    #[test]
    fn test_find_best_base_no_candidates() {
        let selector = BaseSelector::new();
        let target = b"hello world";
        let candidates: Vec<(Oid, &[u8])> = vec![];

        let best = selector.find_best_base(target, &candidates);
        assert!(best.is_none());
    }

    #[test]
    fn test_rank_candidates() {
        let selector = BaseSelector::new();
        let target = b"hello world hello world hello world";

        let good_match = b"hello world hello world hello";
        let bad_match = b"completely different data here";

        let candidates = vec![
            (Oid::hash(bad_match), bad_match.as_slice()),
            (Oid::hash(good_match), good_match.as_slice()),
        ];

        let ranked = selector.rank_candidates(target, &candidates);
        if ranked.len() >= 2 {
            assert!(ranked[0].similarity_score >= ranked[1].similarity_score);
        }
    }

    #[test]
    fn test_is_good_base() {
        let selector = BaseSelector::new();
        let target = b"hello world hello world";
        let good = b"hello world hello";
        let bad = b"completely different";

        assert!(selector.is_good_base(target, good));
        assert!(!selector.is_good_base(target, bad));
    }

    #[test]
    fn test_base_selector_default() {
        let selector = BaseSelector::default();
        assert_eq!(selector.max_candidates, 10);
    }

    #[test]
    fn test_size_ratio_filter() {
        let selector = BaseSelector::with_config(10, 0.0, 1.5);
        let target = b"short";
        let too_large = b"this is a much much much longer string that exceeds the ratio";

        let candidates = vec![(Oid::hash(too_large), too_large.as_slice())];
        let ranked = selector.rank_candidates(target, &candidates);
        assert!(ranked.is_empty());
    }
}
