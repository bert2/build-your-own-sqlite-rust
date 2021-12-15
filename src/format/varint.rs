use crate::util::TakeWhileInclExt;

/// Parses SQLite's "varint" (short for variable-length integer) as mentioned here:
/// [varint](https://www.sqlite.org/fileformat2.html#varint)
///
/// Returns (varint, bytes_read)
pub fn parse(stream: &[u8]) -> (i64, usize) {
    stream
        .iter()
        .take(9)
        .take_while_incl(|&b| (b & 0b10000000) != 0)
        .enumerate()
        .map(|(i, &b)| if i == 8 { (8, b) } else { (7, b & 0b01111111) })
        .fold((0, 0), |(varint, bytes_read), (used_bits, byte)| {
            let varint = (varint << used_bits) | i64::from(byte);
            (varint, bytes_read + 1)
        })
}

#[cfg(test)]
mod test {
    use super::parse;

    #[test]
    fn conversion() {
        assert_eq!(parse(&[0b00000000]), (0, 1));
        assert_eq!(parse(&[0b00000001]), (1, 1));
        assert_eq!(parse(&[0b01111111]), (127, 1));
        assert_eq!(parse(&[0b10000001, 0b00000000]), (128, 2));
        assert_eq!(parse(&[0b10000010, 0b00101100]), (300, 2));
    }

    #[test]
    // the last byte 0b10101010 does not belong to the varint
    fn checks_continuation_bit() {
        assert_eq!(parse(&[0b00000000, 0b10101010]), (0, 1));
        assert_eq!(parse(&[0b00000001, 0b10101010]), (1, 1));
        assert_eq!(parse(&[0b01111111, 0b10101010]), (127, 1));
        assert_eq!(parse(&[0b10000001, 0b00000000, 0b10101010]), (128, 2));
        assert_eq!(parse(&[0b10000010, 0b00101100, 0b10101010]), (300, 2));
    }

    #[test]
    // the last byte 0b10101010 does not belong to the varint
    fn reads_no_more_than_9_bytes() {
        assert_eq!(
            parse(&[
                0b10111111, 0b11111111, 0b11111111, 0b11111111, 0b11111111, 0b11111111, 0b11111111,
                0b11111111, 0b11111111, 0b10101010
            ]),
            (i64::MAX, 9)
        );
    }
}
