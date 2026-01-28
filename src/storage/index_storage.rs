use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexStorage {
    Single(u32),
    List(Vec<u32>),
    Bitmap(RoaringBitmap),
}

pub enum IndexStorageIter<'a> {
    Single(std::iter::Once<u32>),
    List(std::slice::Iter<'a, u32>),
    Bitmap(roaring::bitmap::Iter<'a>),
}

impl<'a> Iterator for IndexStorageIter<'a> {
    type Item = u32;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            IndexStorageIter::Single(i) => i.next(),
            IndexStorageIter::List(i) => i.next().copied(),
            IndexStorageIter::Bitmap(i) => i.next(),
        }
    }
}

impl IndexStorage {
    pub fn new() -> Self {
        IndexStorage::Bitmap(RoaringBitmap::new())
    }

    pub fn add(&mut self, id: u32) {
        match self {
            IndexStorage::Single(old_id) => {
                if *old_id != id {
                    *self = IndexStorage::List(vec![*old_id, id]);
                }
            }
            IndexStorage::List(list) => {
                if !list.contains(&id) {
                    list.push(id);
                    if list.len() > 16 {
                        let mut bitmap = RoaringBitmap::new();
                        for &i in list.iter() {
                            bitmap.insert(i);
                        }
                        bitmap.insert(id);
                        *self = IndexStorage::Bitmap(bitmap);
                    }
                }
            }
            IndexStorage::Bitmap(bitmap) => {
                bitmap.insert(id);
            }
        }
    }

    pub fn remove(&mut self, id: u32) {
        match self {
            IndexStorage::Single(old_id) => {
                if *old_id == id {
                    *self = IndexStorage::Bitmap(RoaringBitmap::new());
                }
            }
            IndexStorage::List(list) => {
                list.retain(|&i| i != id);
            }
            IndexStorage::Bitmap(bitmap) => {
                bitmap.remove(id);
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            IndexStorage::Single(_) => 1,
            IndexStorage::List(list) => list.len(),
            IndexStorage::Bitmap(bitmap) => bitmap.len() as usize,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn to_bitmap(&self) -> RoaringBitmap {
        match self {
            IndexStorage::Single(id) => {
                let mut b = RoaringBitmap::new();
                b.insert(*id);
                b
            }
            IndexStorage::List(list) => {
                let mut b = RoaringBitmap::new();
                for &i in list {
                    b.insert(i);
                }
                b
            }
            IndexStorage::Bitmap(bitmap) => bitmap.clone(),
        }
    }

    pub fn iter(&self) -> IndexStorageIter<'_> {
        match self {
            IndexStorage::Single(id) => IndexStorageIter::Single(std::iter::once(*id)),
            IndexStorage::List(list) => IndexStorageIter::List(list.iter()),
            IndexStorage::Bitmap(bitmap) => IndexStorageIter::Bitmap(bitmap.iter()),
        }
    }
}

impl Default for IndexStorage {
    fn default() -> Self {
        Self::new()
    }
}
