use crate::io;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Varint {
    pub a0: u8,
    pub tail: Vec<u8>,
}
pub fn iter(Varint { a0, tail }: &Varint) -> impl Iterator<Item = &u8> {
    core::iter::once(a0).chain(tail.iter())
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
pub fn value_of(varint @ Varint { a0, tail }: &Varint) -> u64 {
    eprintln!("FOLDING;A0={a0};TAIL={tail:X?}");
    let count = len(varint);
    iter(varint).enumerate().fold(0, |acc, (idx, elt)| {
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
