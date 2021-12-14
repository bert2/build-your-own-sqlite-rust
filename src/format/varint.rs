use std::iter;

const IS_FIRST_BIT_ZERO_MASK: u8 = 0b10000000;
const LAST_SEVEN_BITS_MASK: u8 = 0b01111111;

/// Parses SQLite's "varint" (short for variable-length integer) as mentioned here:
/// [varint](https://www.sqlite.org/fileformat2.html#varint)
///
/// Returns (varint, bytes_read)
pub fn parse_varint(stream: &[u8]) -> (i64, usize) {
    read_usable_bytes(stream)
        .enumerate()
        .map(|(i, byte)| {
            if i == 8 {
                (8, byte as i64)
            } else {
                (7, (byte & LAST_SEVEN_BITS_MASK) as i64)
            }
        })
        .fold((0, 0), |(varint, bytes_read), (used_bits, byte)| {
            ((varint << used_bits) | byte, bytes_read + 1)
        })
}

fn read_usable_bytes(stream: &[u8]) -> impl Iterator<Item = u8> + '_ {
    fn has_no_cont_bit(byte: u8) -> bool {
        (byte & IS_FIRST_BIT_ZERO_MASK) == 0
    }

    let mut i = 0;
    let mut last_byte_found = false;
    iter::from_fn(move || {
        if last_byte_found {
            return None;
        };

        let byte = stream[i];
        if has_no_cont_bit(byte) {
            last_byte_found = true;
        }

        i = i + 1;
        Some(byte)
    })
    .take(9)
}

#[cfg(test)]
mod test {
    use super::parse_varint;

    #[test]
    fn test() {
        assert_eq!(parse_varint(&[0b00000000]), (0, 1));
        assert_eq!(parse_varint(&[0b00000001]), (1, 1));
        assert_eq!(parse_varint(&[0b01111111]), (127, 1));
        assert_eq!(parse_varint(&[0b10000001, 0b00000000]), (128, 2));
        assert_eq!(parse_varint(&[0b10000010, 0b00101100]), (300, 2));
        assert_eq!(
            parse_varint(&[
                0b10111111, 0b11111111, 0b11111111, 0b11111111, 0b11111111, 0b11111111, 0b11111111,
                0b11111111, 0b11111111
            ]),
            (9223372036854775807, 9)
        );
    }
}
