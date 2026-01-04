use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;
use gitstratum_core::{Commit, Oid, RepoId};

use crate::error::Result;
use crate::store::MetadataStore;

pub struct CommitWalker {
    store: Arc<MetadataStore>,
    repo_id: RepoId,
    queue: VecDeque<Oid>,
    visited: HashSet<Oid>,
    until: HashSet<Oid>,
    limit: Option<usize>,
    count: usize,
}

impl CommitWalker {
    pub fn new(
        store: Arc<MetadataStore>,
        repo_id: RepoId,
        from: Vec<Oid>,
        until: Vec<Oid>,
        limit: Option<usize>,
    ) -> Self {
        let mut queue = VecDeque::new();
        let mut visited = HashSet::new();
        let until_set: HashSet<Oid> = until.into_iter().collect();

        for oid in from {
            if !until_set.contains(&oid) && visited.insert(oid) {
                queue.push_back(oid);
            }
        }

        Self {
            store,
            repo_id,
            queue,
            visited,
            until: until_set,
            limit,
            count: 0,
        }
    }

    fn next_commit(&mut self) -> Result<Option<Commit>> {
        if let Some(limit) = self.limit {
            if self.count >= limit {
                return Ok(None);
            }
        }

        while let Some(oid) = self.queue.pop_front() {
            if let Some(commit) = self.store.get_commit(&self.repo_id, &oid)? {
                for parent in &commit.parents {
                    if !self.until.contains(parent) && self.visited.insert(*parent) {
                        self.queue.push_back(*parent);
                    }
                }
                self.count += 1;
                return Ok(Some(commit));
            }
        }

        Ok(None)
    }
}

impl Stream for CommitWalker {
    type Item = Result<Commit>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.next_commit() {
            Ok(Some(commit)) => Poll::Ready(Some(Ok(commit))),
            Ok(None) => Poll::Ready(None),
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }
}

pub fn walk_commits(
    store: Arc<MetadataStore>,
    repo_id: RepoId,
    from: Vec<Oid>,
    until: Vec<Oid>,
    limit: Option<usize>,
) -> CommitWalker {
    CommitWalker::new(store, repo_id, from, until, limit)
}

#[derive(Eq, PartialEq)]
struct TimestampedOid {
    timestamp: i64,
    oid: Oid,
}

impl Ord for TimestampedOid {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.timestamp.cmp(&self.timestamp)
    }
}

impl PartialOrd for TimestampedOid {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub fn find_merge_base(
    store: &MetadataStore,
    repo_id: &RepoId,
    commits: &[Oid],
) -> Result<Option<Oid>> {
    if commits.is_empty() {
        return Ok(None);
    }

    if commits.len() == 1 {
        return Ok(Some(commits[0]));
    }

    let mut ancestors: Vec<HashMap<Oid, usize>> = Vec::with_capacity(commits.len());
    let mut queues: Vec<BinaryHeap<TimestampedOid>> = Vec::with_capacity(commits.len());

    for &oid in commits {
        let mut ancestor_map = HashMap::new();
        let mut queue = BinaryHeap::new();

        if let Some(commit) = store.get_commit(repo_id, &oid)? {
            ancestor_map.insert(oid, 0);
            queue.push(TimestampedOid {
                timestamp: commit.committer.timestamp,
                oid,
            });
        }

        ancestors.push(ancestor_map);
        queues.push(queue);
    }

    let max_iterations = 100000;
    let mut iterations = 0;

    loop {
        if iterations >= max_iterations {
            break;
        }
        iterations += 1;

        let mut any_progress = false;

        for i in 0..commits.len() {
            if let Some(ts_oid) = queues[i].pop() {
                any_progress = true;
                let current_depth = *ancestors[i].get(&ts_oid.oid).unwrap_or(&0);

                let mut is_common = true;
                for j in 0..commits.len() {
                    if j != i && !ancestors[j].contains_key(&ts_oid.oid) {
                        is_common = false;
                        break;
                    }
                }

                if is_common {
                    return Ok(Some(ts_oid.oid));
                }

                if let Some(commit) = store.get_commit(repo_id, &ts_oid.oid)? {
                    for parent in &commit.parents {
                        if !ancestors[i].contains_key(parent) {
                            ancestors[i].insert(*parent, current_depth + 1);
                            if let Some(parent_commit) = store.get_commit(repo_id, parent)? {
                                queues[i].push(TimestampedOid {
                                    timestamp: parent_commit.committer.timestamp,
                                    oid: *parent,
                                });
                            }
                        }
                    }
                }
            }
        }

        if !any_progress {
            break;
        }
    }

    let mut common: Option<(Oid, usize)> = None;

    for oid in ancestors[0].keys() {
        let mut is_common = true;
        let mut total_depth = 0;

        for ancestor_map in &ancestors {
            if let Some(&depth) = ancestor_map.get(oid) {
                total_depth += depth;
            } else {
                is_common = false;
                break;
            }
        }

        if is_common {
            match common {
                None => common = Some((*oid, total_depth)),
                Some((_, existing_depth)) if total_depth < existing_depth => {
                    common = Some((*oid, total_depth))
                }
                _ => {}
            }
        }
    }

    Ok(common.map(|(oid, _)| oid))
}

pub async fn walk_commits_async(
    store: Arc<MetadataStore>,
    repo_id: RepoId,
    from: Vec<Oid>,
    until: Vec<Oid>,
    limit: Option<usize>,
) -> Result<Vec<Commit>> {
    let walker = CommitWalker::new(store, repo_id, from, until, limit);
    let mut commits = Vec::new();

    let mut walker = walker;
    while let Some(result) = { walker.next_commit() }? {
        commits.push(result);
    }

    Ok(commits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamped_oid_ordering() {
        let oid1 = Oid::hash(b"oid1");
        let oid2 = Oid::hash(b"oid2");

        let ts1 = TimestampedOid {
            timestamp: 1000,
            oid: oid1,
        };
        let ts2 = TimestampedOid {
            timestamp: 2000,
            oid: oid2,
        };

        assert!(ts1 > ts2);
        assert!(ts2 < ts1);

        let ts3 = TimestampedOid {
            timestamp: 1000,
            oid: oid2,
        };
        assert!(ts1.partial_cmp(&ts3).is_some());
    }

    #[test]
    fn test_timestamped_oid_equal_timestamps() {
        let oid1 = Oid::hash(b"oid1");
        let oid2 = Oid::hash(b"oid2");

        let ts1 = TimestampedOid {
            timestamp: 1000,
            oid: oid1,
        };
        let ts2 = TimestampedOid {
            timestamp: 1000,
            oid: oid2,
        };

        assert_eq!(ts1.cmp(&ts2), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_timestamped_oid_in_binary_heap() {
        use std::collections::BinaryHeap;

        let oid1 = Oid::hash(b"older");
        let oid2 = Oid::hash(b"newer");

        let mut heap = BinaryHeap::new();
        heap.push(TimestampedOid {
            timestamp: 1000,
            oid: oid1,
        });
        heap.push(TimestampedOid {
            timestamp: 2000,
            oid: oid2,
        });

        let first = heap.pop().unwrap();
        assert_eq!(first.timestamp, 1000);

        let second = heap.pop().unwrap();
        assert_eq!(second.timestamp, 2000);
    }
}
