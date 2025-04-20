use crate::io;
use crate::{varint, Varint};
#[derive(Debug)]
pub struct RecordHeader {
    pub size: Varint,
    pub serial_types: Vec<Varint>,
}
fn header_tail_size(size_varint: &Varint) -> usize {
    varint::value_of(size_varint) as usize - varint::size_of(size_varint)
}
pub fn read_header<R: io::Read>(r: &mut R) -> io::Result<RecordHeader> {
    let size = varint::read(r)?;

    let mut tail = vec![0; header_tail_size(&size)];
    io::Read::read_exact(r, &mut tail)?;

    let mut src = tail.as_slice();

    Ok(RecordHeader {
        size,
        serial_types: core::iter::from_fn(|| varint::read(&mut src).ok()).collect(),
    })
}
#[derive(Debug)]
pub struct RawRecord {
    pub header: RecordHeader,
    pub data: Vec<u8>,
}
fn read_raw<R: io::Read>(r: &mut R) -> io::Result<RawRecord> {
    let header = read_header(r)?;
    let mut data = vec![];
    io::Read::read_to_end(r, &mut data)?;
    eprintln!("RECORD_DATA={}", String::from_utf8_lossy(&data));
    Ok(RawRecord { header, data })
}
#[derive(Debug)]
pub struct RecordElement(pub Vec<u8>);
pub fn is_string_serial_type(serial_type_value: u64) -> bool {
    let is_even = (serial_type_value % 2) == 0;
    (serial_type_value >= 13) && !is_even
}
pub fn string_serial_type_size(serial_type_value: u64) -> usize {
    (serial_type_value as usize - 13) / 2
}
fn serial_type_size(serial_type: &Varint) -> usize {
    const NULL_SERIAL_TYPE: u64 = 0;
    const EIGHT_BIT_SERIAL_TYPE: u64 = 1;
    let size = match varint::value_of(serial_type) {
        // Value is a null
        NULL_SERIAL_TYPE => 0,
        // Value is an 8-bit twos-complement integer
        EIGHT_BIT_SERIAL_TYPE => 1,
        // Value is a string
        serial_type_value if is_string_serial_type(serial_type_value) => {
            string_serial_type_size(serial_type_value)
        }
        _ => todo!(),
    };
    eprintln!("SIZE={size} FOR {serial_type:?}");
    size
}
#[derive(Debug)]
pub enum RecordValue {
    Null,
    TwosComplement8(u8),
    EncodedString(Vec<u8>),
}
pub fn read_value<R: io::Read>(r: &mut R, serial_type: &Varint) -> io::Result<RecordValue> {
    const NULL_SERIAL_TYPE: u64 = 0;
    const EIGHT_BIT_SERIAL_TYPE: u64 = 1;
    match varint::value_of(serial_type) {
        NULL_SERIAL_TYPE => Ok(RecordValue::Null),
        EIGHT_BIT_SERIAL_TYPE => io::read_one(r).map(RecordValue::TwosComplement8),
        serial_type_value if is_string_serial_type(serial_type_value) => {
            io::read_exact_vec(r, string_serial_type_size(serial_type_value))
                .map(RecordValue::EncodedString)
        }
        _ => todo!(),
    }
}
#[derive(Debug)]
pub struct RecordRow {
    pub xs: Vec<RecordValue>,
}
pub fn read_element<R: io::Read>(r: &mut R, serial_type: &Varint) -> io::Result<RecordElement> {
    let size = serial_type_size(serial_type);
    if size > 0 {
        io::read_exact_vec(r, size)
    } else {
        Ok(vec![])
    }
    .map(RecordElement)
}
#[derive(Debug)]
pub struct Column {
    pub cells: Vec<RecordValue>,
}
fn read_column<'s, R: io::Read>(
    r: &mut R,
    serial_types: impl Iterator<Item = &'s Varint>,
) -> Column {
    Column {
        cells: serial_types
            .filter_map(|serial_type| read_value(r, serial_type).ok())
            .collect(),
    }
}
#[derive(Debug)]
pub struct Record {
    pub header: RecordHeader,
    pub columns: Vec<Column>,
}
pub fn read<R: io::Read>(r: &mut R) -> io::Result<Record> {
    let RawRecord { header, data } = read_raw(r)?;
    let mut src = data.as_slice();
    let columns = core::iter::from_fn(|| {
        let column = read_column(&mut src, header.serial_types.iter());
        if column.cells.is_empty() {
            None
        } else {
            Some(column)
        }
    })
    .collect();
    Ok(Record { header, columns })
}
#[derive(Debug)]
pub struct SchemaRow {
    pub r#type: Vec<u8>,
    pub name: Vec<u8>,
    pub table_name: Vec<u8>,
    pub rootpage: u8,
    pub sql: Vec<u8>,
}
fn read_encoded_string<R: io::Read>(r: &mut R, serial_type: &Varint) -> io::Result<Vec<u8>> {
    read_value(r, serial_type).and_then(|value| match value {
        RecordValue::EncodedString(s) => Ok(s),
        otherwise => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Received {otherwise:?} when expecting an encoded string"),
        )),
    })
}
