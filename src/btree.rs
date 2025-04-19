use core::fmt;

use std::error::Error;
use std::io;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum BTreePageType {
    InteriorIndex = 0x02,
    InteriorTable = 0x05,
    LeafIndex = 0x0A,
    LeafTable = 0x0D,
}
impl TryFrom<u8> for BTreePageType {
    type Error = BTreePageTypeError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x02 => Ok(BTreePageType::InteriorIndex),
            0x05 => Ok(BTreePageType::InteriorTable),
            0x0A => Ok(BTreePageType::LeafIndex),
            0x0D => Ok(BTreePageType::LeafTable),
            other => Err(BTreePageTypeError(other)),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct BTreePageTypeError(pub u8);
impl fmt::Display for BTreePageTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args! {"Error parsing BTreePageType from 0x{:01X}", self.0})
    }
}
impl Error for BTreePageTypeError {}
impl From<BTreePageTypeError> for io::Error {
    fn from(value: BTreePageTypeError) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, value)
    }
}
#[derive(Debug)]
#[repr(C, packed(1))]
pub struct BTreePageHeaderInner<Ty> {
    /// Indicates the b-tree page type.
    pub r#type: Ty,
    /// Gives the start of the first freeblock on the page,
    /// or zero if there are no freeblocks
    pub first_freeblock_start: u16,
    /// Gives the number of cells on the page
    pub cell_count: u16,
    /// Gives the start of the cell content area.
    ///
    /// A zero value for this integer is interpreted as [`u16::MAX`] + 1 (or 65536)
    pub content_area_start: u16,
    /// Gives the number of fragmented free bytes
    /// within the cell content area
    pub free_bytes_in_content_area: u8,
}
impl<Ty> BTreePageHeaderInner<Ty> {
    pub fn to_be(self) -> BTreePageHeaderInner<Ty> {
        let BTreePageHeaderInner {
            first_freeblock_start,
            cell_count,
            content_area_start,
            ..
        } = self;
        BTreePageHeaderInner {
            first_freeblock_start: first_freeblock_start.to_be(),
            cell_count: cell_count.to_be(),
            content_area_start: content_area_start.to_be(),
            ..self
        }
    }
}
fn read_page_header_inner<R: io::Read>(
    r: &mut R,
) -> io::Result<BTreePageHeaderInner<BTreePageType>> {
    let mut buf = [0; core::mem::size_of::<BTreePageHeaderInner<u8>>()];
    io::Read::read_exact(r, &mut buf)?;
    let header: BTreePageHeaderInner<u8> = unsafe { core::mem::transmute(buf) };
    let BTreePageHeaderInner {
        r#type,
        first_freeblock_start,
        cell_count,
        content_area_start,
        free_bytes_in_content_area,
    } = header.to_be();
    let r#type = BTreePageType::try_from(r#type)?;
    Ok(BTreePageHeaderInner {
        r#type,
        first_freeblock_start,
        cell_count,
        content_area_start,
        free_bytes_in_content_area,
    })
}
#[derive(Debug)]
pub struct BTreePageHeader {
    pub inner: BTreePageHeaderInner<BTreePageType>,
    pub right_most_pointer: Option<u32>,
}
fn read_page_header<R: io::Read>(r: &mut R) -> io::Result<BTreePageHeader> {
    let inner = read_page_header_inner(r)?;
    let mut right_most_pointer = None;
    if matches!(
        inner.r#type,
        BTreePageType::InteriorIndex | BTreePageType::InteriorTable
    ) {
        let mut buf = [0; core::mem::size_of::<u32>()];
        io::Read::read_exact(r, &mut buf)?;
        right_most_pointer = Some(u32::from_be_bytes(buf));
    };
    Ok(BTreePageHeader {
        inner,
        right_most_pointer,
    })
}
fn size_of_page_header(
    BTreePageHeader {
        inner,
        right_most_pointer,
    }: &BTreePageHeader,
) -> usize {
    core::mem::size_of_val(inner)
        + right_most_pointer
            .is_some()
            .then_some(core::mem::size_of::<u32>())
            .unwrap_or_default()
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct BTreeCellPointer(pub u16);
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct BTreeCellPointerArray(Vec<BTreeCellPointer>);
fn read_cell_pointer_array<R: io::Read>(
    r: &mut R,
    cell_count: usize,
) -> io::Result<BTreeCellPointerArray> {
    let mut cell_pointers = vec![0; cell_count * core::mem::size_of::<BTreeCellPointer>()];
    io::Read::read_exact(r, &mut cell_pointers)?;
    let cell_pointers = cell_pointers
        .chunks_exact(2)
        .filter_map(|b| {
            let &[hi, lo] = b else {
                eprintln!("Unexpected input slice {b:?}");
                return None;
            };
            Some(u16::from_be_bytes([hi, lo]))
        })
        .map(BTreeCellPointer)
        .collect();
    Ok(BTreeCellPointerArray(cell_pointers))
}
fn size_of_cell_pointer_array(BTreeCellPointerArray(xs): &BTreeCellPointerArray) -> usize {
    xs.len() * core::mem::size_of::<BTreeCellPointer>()
}
#[derive(Debug)]
pub struct BTreePage {
    pub header: BTreePageHeader,
    pub cell_pointers: BTreeCellPointerArray,
    pub reserved_area: Vec<u8>,
    pub content: Vec<u8>,
}
pub fn read_page<R: io::Read>(r: &mut R) -> io::Result<BTreePage> {
    let header = read_page_header(r)?;
    eprintln!("Read page with header {header:?}");
    eprintln!("Header size is {}", size_of_page_header(&header));
    let cell_pointers = read_cell_pointer_array(r, header.inner.cell_count as usize)?;
    eprintln!(
        "Size of cell-pointer array is {}",
        size_of_cell_pointer_array(&cell_pointers)
    );
    let currently_read = size_of_page_header(&header) + size_of_cell_pointer_array(&cell_pointers);
    let adjusted_offset = header.inner.content_area_start as usize - currently_read;
    eprintln!("Adjusted offset is {adjusted_offset}");
    // TODO: Make this actually use the DB-header to calculate and read etc etc
    let reserved_area = vec![];

    let mut content = vec![];
    io::Read::read_to_end(r, &mut content)?;

    Ok(BTreePage {
        header,
        cell_pointers,
        reserved_area,
        content,
    })
}
pub fn read_cells<'p>(
    BTreePage {
        cell_pointers: BTreeCellPointerArray(cells),
        content,
        header:
            BTreePageHeader {
                inner: BTreePageHeaderInner { r#type, .. },
                ..
            },
        ..
    }: &'p BTreePage,
    initial_offset: usize,
    page_size: usize,
) -> impl Iterator<Item = BTreeCell> + 'p {
    let header_size = page_size - content.len();
    // eprintln!("Calculated header size as {header_size}");
    let content_offset = header_size + initial_offset;
    // eprintln!("Calculated offset as {content_offset}");
    cells.iter().filter_map(move |BTreeCellPointer(offset)| {
        // eprintln!("Reading cell from content with length {}", content.len());
        let adjusted_offset = *offset as usize - content_offset;
        // eprintln!("Adjusted offset from {offset} to {adjusted_offset}");
        let mut src = &content[adjusted_offset..];
        read_cell(&mut src, *r#type).ok()
    })
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Varint {
    pub a0: u8,
    pub tail: Vec<u8>,
}
fn read_exact<R: io::Read, const N: usize>(r: &mut R) -> io::Result<[u8; N]> {
    let mut buf = [0; N];
    io::Read::read_exact(r, &mut buf)?;
    Ok(buf)
}
fn read_one<R: io::Read>(r: &mut R) -> io::Result<u8> {
    read_exact(r).map(|[elt]: [u8; 1]| elt)
}
fn high_bit_is_set(val: &u8) -> bool {
    val & 0b1000_0000 != 0
}
fn read_varint<R: io::Read>(r: &mut R) -> io::Result<Varint> {
    let a0 = read_one(r)?;

    let mut prev = a0;

    let tail = core::iter::from_fn(|| {
        if !high_bit_is_set(&prev) {
            return None;
        };

        let next = read_one(r).ok()?;

        prev = next;

        Some(next)
    })
    .collect();

    Ok(Varint { a0, tail })
}
fn calculate_varint(Varint { a0, tail }: &Varint) -> u64 {
    if tail.is_empty() {
        (*a0 & 0b0111_1111) as u64
    } else {
        todo!()
    }
}
fn size_of_varint(Varint { a0, tail }: &Varint) -> usize {
    core::mem::size_of_val(a0) + tail.len()
}
#[derive(Debug)]
pub struct BTreeLeafTableCell {
    /// A [`Varint`] which is the total number
    /// of bytes of payload, including overflow
    pub total_payload_bytes: Varint,
    /// A [`Varint`] which is the integer key, a.k.a. rowid
    pub rowid: Varint,
    /// The initial portion of the payload
    /// that does not spill to overflow pages
    pub initial_payload: Vec<u8>,
    /// Integer page number for the first page
    /// of the overflow page list - omitted if
    /// all payload fits on the b-tree page
    pub first_overflow_page_number: Option<u32>,
}
fn read_leaf_table_cell<R: io::Read>(r: &mut R) -> io::Result<BTreeLeafTableCell> {
    let total_payload_bytes = read_varint(r)?;
    let calculated_total_payload_bytes = calculate_varint(&total_payload_bytes);
    // eprintln!("total payload bytes calculated as {calculated_total_payload_bytes}");
    let rowid = read_varint(r)?;

    let mut initial_payload = vec![0; calculated_total_payload_bytes as usize];
    io::Read::read_exact(r, &mut initial_payload)?;

    Ok(BTreeLeafTableCell {
        total_payload_bytes,
        rowid,
        initial_payload,
        first_overflow_page_number: None,
    })
}
#[derive(Debug)]
pub enum BTreeCell {
    LeafTable(BTreeLeafTableCell),
}
fn read_cell<R: io::Read>(r: &mut R, r#type: BTreePageType) -> io::Result<BTreeCell> {
    match r#type {
        BTreePageType::LeafTable => read_leaf_table_cell(r).map(BTreeCell::LeafTable),
        _ => todo!(),
    }
}
#[repr(transparent)]
pub struct TableBTreeInteriorCell {
    /// A big-endian number which is the left child pointer
    pub page_number: u32,
}

#[derive(Debug)]
pub struct RecordHeader {
    pub size: Varint,
    pub serial_types: Vec<Varint>,
    // pub serial_type: Varint,
    // pub tail: Vec<u8>,
}
fn read_record_header<R: io::Read>(r: &mut R) -> io::Result<RecordHeader> {
    let size = read_varint(r)?;
    let tail_size = calculate_varint(&size) as usize - size_of_varint(&size);
    let mut tail = vec![0; tail_size];
    io::Read::read_exact(r, &mut tail)?;
    let mut src = tail.as_slice();
    let serial_types = core::iter::from_fn(|| read_varint(&mut src).ok()).collect();
    // let serial_type = read_varint(r)?;

    // let tail_size =
    //     calculate_varint(&size) as usize - (size_of_varint(&size) + size_of_varint(&serial_type));

    // let mut tail = vec![0; tail_size];
    // io::Read::read_exact(r, &mut tail)?;

    Ok(RecordHeader { size, serial_types })
}
#[derive(Debug)]
pub struct RecordElement(pub Vec<u8>);
fn read_record_element<R: io::Read>(r: &mut R, serial_type: &Varint) -> io::Result<RecordElement> {
    let body = match calculate_varint(serial_type) {
        // Value is a null
        0 => vec![],
        // Value is an 8-bit twos-complement integer
        1 => {
            eprintln!("Value is a 8-bit twos-complement integer");
            read_exact::<R, 1>(r).map(|arr| arr.to_vec())?
        }
        // Value is a string
        val if val >= 13 && val % 2 != 0 => {
            let size = (val as usize - 13) / 2;
            eprintln!("Value is a string with size {size}");
            let mut buf = vec![0; size];
            io::Read::read_exact(r, &mut buf)?;
            buf
        }
        _ => todo!(),
    };
    Ok(RecordElement(body))
}
#[derive(Debug)]
pub struct Record {
    pub header: RecordHeader,
    // pub elt: RecordElement,
    pub tail: Vec<u8>,
}
pub fn read_record<R: io::Read>(r: &mut R) -> io::Result<Record> {
    let header = read_record_header(r)?;
    eprintln!("RECORD SIZE: {:?}", calculate_varint(&header.size));
    for (idx, t) in header.serial_types.iter().enumerate() {
        eprintln!("RECORD TYPE: {idx} -> {}", calculate_varint(t));
        let elt = read_record_element(r, t)?;
        eprintln!("TABLE NAME: {}", String::from_utf8_lossy(&elt.0));
    }
    // eprintln!("RECORD TYPE: {:?}", calculate_varint(&header.serial_type));

    let mut tail = vec![];
    io::Read::read_to_end(r, &mut tail)?;

    eprintln!("TABLE DATA: {}", String::from_utf8_lossy(&tail));

    Ok(Record { header, tail })
}
