// Copyright 2016 Claus Matzinger
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

extern crate ring;
extern crate hex;

use self::ring::digest;
use std::io::{Read, Seek, SeekFrom, Error};
use self::hex::ToHex;

pub fn sha1_digest<B: Read + Seek>(input: &mut B) -> Result<Vec<u8>, Error> {
    let mut buffer = [0; 1024 * 100]; // 100 kb buffer size
    let mut ctx = digest::Context::new(&digest::SHA1);
    loop {
        match input.read(&mut buffer[..]) {
            Ok(n) if n > 0 => ctx.update(&buffer[0..n]),
            Err(e) => return Err(e),
            _ => break,
        };
    }
    let _ = input.seek(SeekFrom::Start(0)); //hacky?
    let sha1 = ctx.finish();
    Ok((*sha1.as_ref()).to_vec())
}

///
/// Creates a string object from an array of bytes
///
/// # Example
///
/// ```rust,ignore
/// assert_eq!(to_hex_string(b"11111"), "3131313131");
/// assert_eq!(to_hex_string(b"aaad2"), "6161616432");
/// assert_eq!(to_hex_string(b"zzzzz"), "7A7A7A7A7A");
/// ```
pub fn to_hex_string(sha1: &[u8]) -> String {
    sha1.to_hex()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn digest_string() {
        let one234 = [113, 16, 237, 164, 208, 158, 6, 42, 165, 228, 163, 144, 176, 165, 114, 172,
                      13, 44, 2, 32];
        let contents = [74, 117, 108, 160, 126, 148, 135, 244, 130, 70, 90, 153, 232, 40, 106,
                        188, 134, 186, 77, 199];
        assert_eq!(sha1_digest(&mut Cursor::new(b"1234")).unwrap(), one234);
        assert_eq!(sha1_digest(&mut Cursor::new(b"contents")).unwrap(),
                   contents);

    }

    #[test]
    fn string_from_hex() {
        let sha_1234 = [113, 16, 237, 164, 208, 158, 6, 42, 165, 228, 163, 144, 176, 165, 114,
                        172, 13, 44, 2, 32];
        let sha_contents = [203, 2, 235, 164, 178, 218, 138, 242, 242, 203, 29, 167, 94, 67, 205,
                            143, 13, 123, 69, 69];
        assert_eq!(to_hex_string(&[0xF]), "0f");
        assert_eq!(to_hex_string(&[11]), "0b");
        assert_eq!(to_hex_string(&[255]), "ff");
        assert_eq!(to_hex_string(b"11111"), "3131313131");
        assert_eq!(to_hex_string(b"aaad2"), "6161616432");
        assert_eq!(to_hex_string(b"zzzzz"), "7a7a7a7a7a");
        assert_eq!(to_hex_string(b""), "");
        assert_eq!(to_hex_string(b""), "");
        assert_eq!(to_hex_string(&sha_1234),
                   "7110eda4d09e062aa5e4a390b0a572ac0d2c0220");
    }
}