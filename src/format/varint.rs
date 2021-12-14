use std::iter;

/// Parses SQLite's "varint" (short for variable-length integer) as mentioned here:
/// [varint](https://www.sqlite.org/fileformat2.html#varint)
///
/// Returns (varint, bytes_read)
pub fn parse_varint(stream: &[u8]) -> (i64, usize) {
    read_varint_bytes(stream)
        .enumerate()
        .map(|(i, byte)| {
            if i == 8 {
                (8, byte)
            } else {
                (7, byte & 0b01111111)
            }
        })
        .fold((0, 0), |(varint, bytes_read), (used_bits, byte)| {
            ((varint << used_bits) | i64::from(byte), bytes_read + 1)
        })
}

fn read_varint_bytes(stream: &[u8]) -> impl Iterator<Item = u8> + '_ {
    fn is_last_byte(byte: u8) -> bool {
        (byte & 0b10000000) == 0
    }

    let mut i = 0;
    let mut done = false;
    iter::from_fn(move || {
        if done {
            return None;
        };

        let byte = stream[i];
        done = is_last_byte(byte);
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
