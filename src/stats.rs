//! Contains items related to gathering statistics about found images.

use std::collections::HashMap;

use crate::convert::image::ImageFormat;
use crate::convert::search::ImageInfo;
use crate::stdout;

/// Aggregated statistics.
pub struct Stats {
    /// Maps an image format to the number of occurences
    pub inner: HashMap<ImageFormat, usize>,
}

impl Stats {
    /// Create a new, empty statistics object.
    pub fn new() -> Self {
        let inner = HashMap::new();
        Self { inner }
    }

    /// Count the occurences of each image type in the iterator.
    pub fn compute<'a>(images: impl Iterator<Item = &'a ImageInfo>) -> Self {
        let inner = images.fold(HashMap::new(), |mut counts, info| {
            counts
                .entry(info.format())
                .and_modify(|v| *v += 1)
                .or_insert(1);
            counts
        });
        Self { inner }
    }

    /// Combine two statistics into one
    pub fn combine(&mut self, other: &Self) {
        for (&format, &count) in &other.inner {
            self.inner
                .entry(format)
                .and_modify(|v| *v += count)
                .or_insert(count);
        }
    }

    /// Print out statistics per image format.
    pub fn print_per_format(&self) {
        let mut counts = Vec::from_iter(self.inner.clone());
        counts.sort_unstable_by_key(|(f, _)| f.ext());
        for (format, count) in &counts {
            let format = format.ext();
            stdout(format!("{format}: {count}"));
        }
    }

    /// Print out the total number of images found.
    pub fn print_total(&self) {
        let total: usize = self.inner.values().sum();
        stdout(format!("total: {total}"));
    }
}
