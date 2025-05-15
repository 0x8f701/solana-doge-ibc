/*
Copyright (C) 2025 Zero Knowledge Labs Limited, Psy Protocol

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

Additional terms under GNU AGPL version 3 section 7:

As permitted by section 7(b) of the GNU Affero General Public License, 
you must retain the following attribution notice in all copies or 
substantial portions of the software:

"This software was created by Psy (https://Psy.xyz)
with contributions from Carter Feldman (https://x.com/cmpeq)."
*/

use hex::FromHexError;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::fmt::Display;

#[serde_as]
#[derive(Serialize, Deserialize, PartialEq, Clone, Copy, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct U8BytesFixed<const N: usize>(#[serde_as(as = "serde_with::hex::Hex")] pub [u8; N]);

impl<const N: usize> Display for U8BytesFixed<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(&self.0))
    }
}
#[serde_as]
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct U8Bytes(#[serde_as(as = "serde_with::hex::Hex")] pub Vec<u8>);

impl U8Bytes {
    pub fn from_hex_string(s: &str) -> Result<Self, FromHexError> {
        let bytes = hex::decode(s)?;
        Ok(Self(bytes))
    }
    pub fn to_str(&self) -> String {
        hex::encode(&self.0)
    }
    pub fn rand(len: usize) -> Self {
        let mut bytes = vec![0u8; len];
        rand::thread_rng().fill_bytes(&mut bytes);
        U8Bytes(bytes)
    }
}

impl Display for U8Bytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(&self.0))
    }
}
impl From<Vec<u8>> for U8Bytes {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}
impl From<&[u8]> for U8Bytes {
    fn from(value: &[u8]) -> Self {
        Self(value.to_vec())
    }
}
impl<const S: usize> From<[u8; S]> for U8Bytes {
    fn from(value: [u8; S]) -> Self {
        Self(value.to_vec())
    }
}
