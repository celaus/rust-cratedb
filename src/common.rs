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
use self::ring::digest;
use std::io::{Read, Seek, SeekFrom, Error};


pub fn sha1_digest<B: Read + Seek>(input: &mut B) -> Result<Vec<u8>, Error> {
    let mut buffer = [0; 1024 * 100]; // 100 kb buffer size
    let mut ctx = digest::Context::new(&digest::SHA1);
    loop {
        match input.read(&mut buffer[..]) {
            Ok(n) if n > 0 => ctx.update(&buffer),
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
    let mut sha_string = String::with_capacity(sha1.len());
    for b in sha1 {
        let s = format!("{:x}", b);
        sha_string.push_str(&s);
    }
    sha_string
}

#[cfg(test)]
mod tests {
    use common::to_hex_string;
    #[test]
    fn string_from_hex() {
        assert_eq!(to_hex_string(&[0xF]), "f");
        assert_eq!(to_hex_string(&[11]), "b");
        assert_eq!(to_hex_string(&[255]), "ff");
        assert_eq!(to_hex_string(b"11111"), "3131313131");
        assert_eq!(to_hex_string(b"aaad2"), "6161616432");
        assert_eq!(to_hex_string(b"zzzzz"), "7a7a7a7a7a");
        assert_eq!(to_hex_string(b""), "");
    }
}