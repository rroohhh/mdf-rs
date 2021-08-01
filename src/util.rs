/*
pub(crate) fn parse_utf16_string(data: &[u8]) -> String {
    String::from_utf16(
        &data
            .chunks_exact(2)
            .into_iter()
            .map(|a| u16::from_le_bytes([a[0], a[1]]))
            .collect::<Vec<_>>(),
    ).unwrap()
}
 */

pub(crate) fn parse_utf16_string(data: &[u8]) -> String {
    let (s, _, _) = encoding_rs::UTF_16LE.decode(data);
    s.into_owned()
}
