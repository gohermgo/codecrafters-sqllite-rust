pub use std::io::*;
pub fn read_exact_array<R: Read, const N: usize>(r: &mut R) -> Result<[u8; N]> {
    let mut buf = [0; N];
    Read::read_exact(r, &mut buf)?;
    Ok(buf)
}
pub fn read_exact_vec<R: Read>(r: &mut R, count: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0; count];
    Read::read_exact(r, &mut buf)?;
    Ok(buf)
}
pub fn read_one<R: Read>(r: &mut R) -> Result<u8> {
    read_exact_array(r).map(|[elt]: [u8; 1]| elt)
}
