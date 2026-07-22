pub(in crate::protocol) struct FreeChannels {
    words: Box<[u64]>,
    summary: Box<[u64]>,
    root: u64,
}

impl FreeChannels {
    pub(in crate::protocol) fn new(capacity: usize) -> Self {
        let mut words = vec![u64::MAX; capacity.div_ceil(u64::BITS as usize)];
        if let Some(first) = words.first_mut() {
            *first &= !1;
        }
        if let Some(last) = words.last_mut() {
            let tail = capacity % u64::BITS as usize;
            if tail != 0 {
                *last &= (1 << tail) - 1;
            }
        }
        let mut summary = vec![0u64; words.len().div_ceil(u64::BITS as usize)];
        for (index, word) in words.iter().enumerate() {
            if *word != 0 {
                summary[index / u64::BITS as usize] |= 1 << (index % u64::BITS as usize);
            }
        }
        let root = summary.iter().enumerate().fold(0, |root, (index, word)| {
            root | (u64::from(*word != 0) << index)
        });
        Self {
            words: words.into_boxed_slice(),
            summary: summary.into_boxed_slice(),
            root,
        }
    }

    pub(in crate::protocol) fn take(&mut self, id: u16) -> bool {
        let index = id as usize;
        let word_index = index / u64::BITS as usize;
        let Some(word) = self.words.get(word_index) else {
            return false;
        };
        let bit = 1 << (index % u64::BITS as usize);
        if *word & bit == 0 {
            return false;
        }
        unsafe { self.take_unchecked(id) };
        true
    }

    pub(in crate::protocol) fn take_first(&mut self, upper: u16) -> Option<u16> {
        let summary_index = (self.root != 0).then(|| self.root.trailing_zeros() as usize)?;
        let summary = unsafe { *self.summary.get_unchecked(summary_index) };
        let word_index = summary_index * u64::BITS as usize + summary.trailing_zeros() as usize;
        let word = unsafe { *self.words.get_unchecked(word_index) };
        let id = word_index * u64::BITS as usize + word.trailing_zeros() as usize;
        let id = u16::try_from(id).ok()?;
        if id > upper {
            return None;
        }
        unsafe { self.take_unchecked(id) };
        Some(id)
    }

    pub(in crate::protocol) fn put(&mut self, id: u16) {
        let index = id as usize;
        let word_index = index / u64::BITS as usize;
        let Some(word) = self.words.get_mut(word_index) else {
            return;
        };
        let bit = 1 << (index % u64::BITS as usize);
        if *word & bit != 0 {
            return;
        }
        let was_empty = *word == 0;
        *word |= bit;
        if was_empty {
            let summary_index = word_index / u64::BITS as usize;
            let summary_bit = 1 << (word_index % u64::BITS as usize);
            unsafe { *self.summary.get_unchecked_mut(summary_index) |= summary_bit };
            self.root |= 1 << summary_index;
        }
    }

    unsafe fn take_unchecked(&mut self, id: u16) {
        let index = id as usize;
        let word_index = index / u64::BITS as usize;
        let word = unsafe { self.words.get_unchecked_mut(word_index) };
        *word &= !(1 << (index % u64::BITS as usize));
        if *word == 0 {
            let summary_index = word_index / u64::BITS as usize;
            let summary_bit = 1 << (word_index % u64::BITS as usize);
            let summary = unsafe { self.summary.get_unchecked_mut(summary_index) };
            *summary &= !summary_bit;
            if *summary == 0 {
                self.root &= !(1 << summary_index);
            }
        }
    }
}
