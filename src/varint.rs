use core::iter::{once, Chain, Once};
use core::slice::Iter;

use std::vec::IntoIter;

use crate::io;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Varint {
    pub a0: u8,
    pub tail: Vec<u8>,
}
impl<'a> IntoIterator for &'a Varint {
    type Item = &'a u8;
    type IntoIter = Chain<Once<&'a u8>, Iter<'a, u8>>;
    fn into_iter(self) -> Self::IntoIter {
        let Varint { a0, tail } = self;
        once(a0).chain(tail)
    }
}
impl IntoIterator for Varint {
    type Item = u8;
    type IntoIter = Chain<Once<u8>, IntoIter<u8>>;
    fn into_iter(self) -> Self::IntoIter {
        let Varint { a0, tail } = self;
        once(a0).chain(tail)
    }
}
pub fn len(Varint { tail, .. }: &Varint) -> usize {
    tail.len() + 1
}
fn high_bit_is_set(val: &u8) -> bool {
    val & 0b1000_0000 != 0
}
pub fn read<R: io::Read>(r: &mut R) -> io::Result<Varint> {
    let a0 = io::read_one(r)?;

    let mut prev = a0;

    let tail = core::iter::from_fn(|| {
        if !high_bit_is_set(&prev) {
            return None;
        };

        let next = io::read_one(r).ok()?;

        prev = next;

        Some(next)
    })
    .collect();

    Ok(Varint { a0, tail })
}
pub fn value_of(varint: &Varint) -> u64 {
    let count = len(varint);
    varint.into_iter().enumerate().fold(0, |acc, (idx, elt)| {
        let elt = if idx < count - 1 {
            *elt ^ 0b1000_0000
        } else {
            *elt
        } as u64;
        let elt = elt << (7 * (count - idx - 1));
        elt | acc
    })
}
impl From<Varint> for u64 {
    fn from(value: Varint) -> Self {
        value_of(&value)
    }
}
impl From<&Varint> for u64 {
    fn from(value: &Varint) -> Self {
        value_of(value)
    }
}
pub fn size_of(Varint { a0, tail }: &Varint) -> usize {
    core::mem::size_of_val(a0) + tail.len()
}
