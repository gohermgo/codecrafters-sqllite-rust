use core::fmt;

use std::error::Error;

use crate::database::record::{self, FromRawColumn, Record};

use crate::io;
use crate::{varint, Varint};

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
#[derive(Clone, Copy, Debug)]
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
    #[must_use]
    pub fn to_be(self) -> BTreePageHeaderInner<Ty>
    where
        Ty: Copy,
    {
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
pub struct BTreePageInner {
    pub header: BTreePageHeader,
    pub cell_pointers: BTreeCellPointerArray,
    #[allow(dead_code)]
    pub reserved_area: Vec<u8>,
}
fn read_page_inner<R: io::Read>(r: &mut R) -> io::Result<BTreePageInner> {
    eprintln!("READING BTREE PAGE INNER");
    let header = read_page_header(r)?;
    eprintln!(
        "BTREE PAGE HEADER={header:X?};HEADER SIZE={}",
        size_of_page_header(&header)
    );
    let cell_pointers = read_cell_pointer_array(r, header.inner.cell_count as usize)?;
    eprintln!(
        "CELL POINTERS={cell_pointers:?};CELL POINTERS SIZE={}",
        size_of_cell_pointer_array(&cell_pointers)
    );
    // TODO: Make this actually use the DB-header to calculate and read etc etc
    let reserved_area = vec![];
    Ok(BTreePageInner {
        header,
        cell_pointers,
        reserved_area,
    })
}
#[derive(Debug)]
pub struct BTreePage {
    #[allow(dead_code)]
    pub inner: BTreePageInner,
    pub content: Vec<BTreeCell>,
}

pub fn read_page<R: io::Read>(r: &mut R, initial_offset: usize) -> io::Result<BTreePage> {
    #[derive(Debug)]
    pub struct BTreePageBytes {
        pub inner: BTreePageInner,
        pub content: Vec<u8>,
    }
    fn parse_page_bytes(
        BTreePageBytes { inner, content }: BTreePageBytes,
        initial_offset: usize,
    ) -> BTreePage {
        let BTreePageInner {
            header,
            cell_pointers,
            ..
        } = &inner;

        let BTreePageHeader {
            inner: BTreePageHeaderInner { r#type, .. },
            ..
        } = header;
        let content_offset = size_of_page_header(header)
            + size_of_cell_pointer_array(cell_pointers)
            + initial_offset;
        let BTreeCellPointerArray(cell_pointers) = cell_pointers;
        let content = cell_pointers
            .iter()
            .filter_map(move |BTreeCellPointer(offset)| {
                let adjusted_offset = *offset as usize - content_offset;
                let mut src = &content[adjusted_offset..];
                read_cell(&mut src, *r#type).ok()
            })
            .collect();
        BTreePage { inner, content }
    }

    read_page_inner(r)
        .and_then(|inner| {
            let mut content = vec![];
            io::Read::read_to_end(r, &mut content)?;
            Ok(BTreePageBytes { inner, content })
        })
        .map(|btree_page_bytes| parse_page_bytes(btree_page_bytes, initial_offset))
}
pub fn parse_cell<C: FromRawColumn>(cell: BTreeCell) -> io::Result<RecordCell<C>> {
    match cell {
        BTreeCell::LeafTable(BTreeLeafTableCell {
            rowid,
            initial_payload,
            ..
        }) => {
            record::read(&mut initial_payload.as_slice()).map(|record| RecordCell { rowid, record })
        }
    }
}

#[derive(Debug)]
pub struct BTreeLeafTableCell {
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    pub first_overflow_page_number: Option<u32>,
}
fn read_leaf_table_cell<R: io::Read>(r: &mut R) -> io::Result<BTreeLeafTableCell> {
    let total_payload_bytes = varint::read(r)?;
    let calculated_total_payload_bytes = varint::value_of(&total_payload_bytes);
    let rowid = varint::read(r)?;

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
pub fn get_cell_content(cell: &BTreeCell) -> Option<&[u8]> {
    match cell {
        BTreeCell::LeafTable(BTreeLeafTableCell {
            initial_payload, ..
        }) => Some(initial_payload.as_slice()),
        // _ => None,
    }
}
#[allow(dead_code)]
pub fn print_cell_rowid(cell: &BTreeCell) {
    match cell {
        BTreeCell::LeafTable(BTreeLeafTableCell { rowid, .. }) => {
            eprintln!("CELL_ROWID={}", varint::value_of(rowid))
        }
    }
}
fn read_cell<R: io::Read>(r: &mut R, r#type: BTreePageType) -> io::Result<BTreeCell> {
    match r#type {
        BTreePageType::LeafTable => read_leaf_table_cell(r).map(BTreeCell::LeafTable),
        _ => todo!(),
    }
}
#[derive(Debug)]
pub struct RecordCell<C> {
    #[allow(dead_code)]
    pub rowid: Varint,
    pub record: Record<C>,
}
#[allow(dead_code)]
#[repr(transparent)]
pub struct TableBTreeInteriorCell {
    /// A big-endian number which is the left child pointer
    pub page_number: u32,
}
