use crate::io;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Varint {
    pub a0: u8,
    pub tail: Vec<u8>,
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
pub fn value_of(Varint { a0, tail }: &Varint) -> u64 {
    let init = (*a0 & 0b0111_1111) as u64;
    tail.iter().fold(init, |acc, elt| {
        eprintln!("Starting with acc={acc}, elt={elt}");
        let shifted = acc << 8;
        let current = (*elt & 0b0111_1111) as u64;
        let res = shifted | current;
        eprintln!("SHIFTED={shifted}, CURRENT={current}, RES={res}");
        res
    })
}
pub fn size_of(Varint { a0, tail }: &Varint) -> usize {
    core::mem::size_of_val(a0) + tail.len()
}
