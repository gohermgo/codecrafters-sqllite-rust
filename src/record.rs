use crate::io;
use crate::{varint, Varint};
#[derive(Debug)]
pub struct RecordHeader {
    pub size: Varint,
    pub serial_types: Vec<Varint>,
}
pub fn read_header<R: io::Read>(r: &mut R) -> io::Result<RecordHeader> {
    let size = varint::read(r)?;
    let tail_size = varint::value_of(&size) as usize - varint::size_of(&size);
    eprintln!("RECORD HEADER: SIZE={:?}; TAIL_SIZE={tail_size}", size);
    let mut tail = vec![0; tail_size];
    io::Read::read_exact(r, &mut tail)?;
    let mut src = tail.as_slice();
    let serial_types = core::iter::from_fn(|| {
        let varint = varint::read(&mut src).ok();
        eprintln!("VARINT={varint:?};SRC={src:?}");
        varint
    })
    .collect();

    Ok(RecordHeader { size, serial_types })
}
#[derive(Debug)]
pub struct RecordElement(pub Vec<u8>);
fn is_string_serial_type(serial_type_value: u64) -> bool {
    let is_even = (serial_type_value % 2) == 0;
    (serial_type_value >= 13) && !is_even
}
fn string_serial_type_size(serial_type_value: u64) -> usize {
    (serial_type_value as usize - 13) / 2
}
fn serial_type_size(serial_type: &Varint) -> usize {
    const NULL_SERIAL_TYPE: u64 = 0;
    const EIGHT_BIT_SERIAL_TYPE: u64 = 1;
    let size = match varint::value_of(serial_type) {
        // Value is a null
        NULL_SERIAL_TYPE => 0,
        // Value is an 8-bit twos-complement integer
        EIGHT_BIT_SERIAL_TYPE => {
            eprintln!("Value is a 8-bit twos-complement integer");
            1
        }
        // Value is a string
        serial_type_value if is_string_serial_type(serial_type_value) => {
            let size = string_serial_type_size(serial_type_value);
            eprintln!("Values is a string with size {size}");
            size
        }
        _ => todo!(),
    };
    eprintln!("SIZE={size} FOR {serial_type:?}");
    size
}
pub fn read_element<R: io::Read>(r: &mut R, serial_type: &Varint) -> io::Result<RecordElement> {
    eprintln!("READ RECORD: {serial_type:?}");
    let size = serial_type_size(serial_type);
    if size > 0 {
        io::read_exact_vec(r, size)
    } else {
        Ok(vec![])
    }
    .map(RecordElement)
}
