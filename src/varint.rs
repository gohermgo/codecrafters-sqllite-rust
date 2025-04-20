use crate::io;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Varint {
    pub a0: u8,
    pub tail: Vec<u8>,
}
impl<'a> IntoIterator for &'a Varint {
    type Item = &'a u8;
    type IntoIter = Iter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        Iter(core::iter::once(&self.a0).chain(self.tail.iter()))
    }
}
pub struct Iter<'a>(core::iter::Chain<core::iter::Once<&'a u8>, std::slice::Iter<'a, u8>>);
impl<'a> Iterator for Iter<'a> {
    type Item = &'a u8;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}
impl IntoIterator for Varint {
    type Item = u8;
    type IntoIter = IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        IntoIter(core::iter::once(self.a0).chain(self.tail))
    }
}
pub fn iter(Varint { a0, tail }: &Varint) -> impl Iterator<Item = &u8> {
    core::iter::once(a0).chain(tail.iter())
}
pub struct IntoIter(core::iter::Chain<core::iter::Once<u8>, std::vec::IntoIter<u8>>);
impl Iterator for IntoIter {
    type Item = u8;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}
pub fn into_iter(Varint { a0, tail }: Varint) -> impl Iterator<Item = u8> {
    core::iter::once(a0).chain(tail)
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
pub fn size_of(Varint { a0, tail }: &Varint) -> usize {
    core::mem::size_of_val(a0) + tail.len()
}
