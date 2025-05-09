use crate::io;
use crate::{varint, Varint};

mod schema;
pub use schema::{SchemaColumn, SchemaRecord};

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
    Ok(RawRecord { header, data })
}
// #[derive(Debug)]
// pub struct RecordElement(pub Vec<u8>);
pub fn is_string_serial_type(serial_type_value: u64) -> bool {
    let is_even = (serial_type_value % 2) == 0;
    (serial_type_value >= 13) && !is_even
}
pub fn string_serial_type_size(serial_type_value: u64) -> usize {
    (serial_type_value as usize - 13) / 2
}
#[allow(dead_code)]
fn serial_type_size(serial_type: &Varint) -> usize {
    const NULL_SERIAL_TYPE: u64 = 0;
    const EIGHT_BIT_SERIAL_TYPE: u64 = 1;
    match varint::value_of(serial_type) {
        // Value is a null
        NULL_SERIAL_TYPE => 0,
        // Value is an 8-bit twos-complement integer
        EIGHT_BIT_SERIAL_TYPE => 1,
        // Value is a string
        serial_type_value if is_string_serial_type(serial_type_value) => {
            string_serial_type_size(serial_type_value)
        }
        _ => todo!(),
    }
}
#[derive(Clone, Debug)]
pub enum RecordValue {
    Null,
    TwosComplement8(u8),
    EncodedString(Vec<u8>),
}
fn lift_twos_complement_8(value: RecordValue) -> io::Result<u8> {
    match value {
        RecordValue::TwosComplement8(value) => Ok(value),
        otherwise => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Received {otherwise:?} when expecting an encoded string"),
        )),
    }
}
pub fn lift_encoded_string(value: RecordValue) -> io::Result<Vec<u8>> {
    match value {
        RecordValue::EncodedString(s) => Ok(s),
        otherwise => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Received {otherwise:?} when expecting an encoded string"),
        )),
    }
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
pub struct RawColumn {
    pub cells: Vec<RecordValue>,
}
pub fn read_raw_column<'s, R: io::Read>(
    r: &mut R,
    serial_types: impl IntoIterator<Item = &'s Varint>,
) -> RawColumn {
    RawColumn {
        cells: serial_types
            .into_iter()
            .map_while(|serial_type| read_value(r, serial_type).ok())
            .collect(),
    }
}
pub trait FromRawColumn {
    fn from_raw_column(column: RawColumn) -> io::Result<Self>
    where
        Self: Sized;
}
impl FromRawColumn for RawColumn {
    #[inline(always)]
    fn from_raw_column(column: RawColumn) -> io::Result<Self>
    where
        Self: Sized,
    {
        Ok(column)
    }
}
#[derive(Debug)]
pub struct Record<C> {
    pub header: RecordHeader,
    pub columns: Vec<C>,
}
pub fn read<R: io::Read, C: FromRawColumn>(r: &mut R) -> io::Result<Record<C>> {
    let RawRecord { header, data } = read_raw(r)?;
    let mut src = data.as_slice();
    let columns = core::iter::from_fn(|| {
        let column = read_raw_column(&mut src, header.serial_types.iter());
        if column.cells.is_empty() {
            eprintln!("Column cells empty");
            None
        } else {
            FromRawColumn::from_raw_column(column).ok()
        }
    })
    .collect();
    Ok(Record { header, columns })
}
#[derive(Debug)]
pub struct RecordBytes<'a> {
    pub header: RecordHeader,
    pub bytes: &'a [u8],
}
impl RecordBytes<'_> {
    pub fn from_bytes(mut bytes: &[u8]) -> Option<RecordBytes<'_>> {
        match read_header(&mut bytes).map(|header| RecordBytes { header, bytes }) {
            Ok(val) => Some(val),
            Err(e) => {
                eprintln!("FAILED TO READ HEADER FROM BYTES: {e}");
                None
            }
        }
    }
}
#[derive(Debug)]
pub struct SerializedRecord {
    pub header: RecordHeader,
    pub column: RawColumn,
}
impl SerializedRecord {
    pub fn from_bytes(RecordBytes { header, mut bytes }: RecordBytes) -> SerializedRecord {
        let column = read_raw_column(&mut bytes, &header.serial_types);
        SerializedRecord { header, column }
    }
}
// fn serialize_bytes(RecordBytes { header, mut bytes }: RecordBytes) -> SerializedRecord {
//     let serial_types = header.serial_types.iter();
//     let column = record::read_raw_column(&mut bytes, serial_types);
//     record::SerializedRecord { header, column }
// }
