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
#[repr(C)]
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct BTreeCellPointer(pub u16);
#[derive(Debug)]
pub struct BTreePage {
    pub header: BTreePageHeader,
    pub cell_pointers: Vec<BTreeCellPointer>,
}
pub fn read_page<R: io::Read>(r: &mut R) -> io::Result<BTreePage> {
    let header = read_page_header(r)?;
    eprintln!("Read page with header {header:?}");
    let mut cell_pointers =
        vec![0; header.inner.cell_count as usize * core::mem::size_of::<BTreeCellPointer>()];
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
    Ok(BTreePage {
        header,
        cell_pointers,
    })
}

#[repr(transparent)]
pub struct TableBTreeInteriorCell {
    /// A big-endian number which is the left child pointer
    pub page_number: u32,
}
