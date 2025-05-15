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

use crate::impl_kvq_serialize;

use super::traits::KVQSerializable;

impl_kvq_serialize!(u8, u32, u64, u128);


#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DBRow {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Copy, Clone, Debug)]
pub enum DBFlush {
    Disable,
    Enable,
}
impl KVQSerializable for DBRow {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let key_len = self.key.len() as u32;
        let mut result = Vec::with_capacity(self.key.len()+self.value.len()+4);
        result.extend_from_slice(&key_len.to_be_bytes());
        result.extend_from_slice(&self.key);
        result.extend_from_slice(&self.value);
        
        Ok(result)
    }
    fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        if bytes.len() < 4 {
            return Err(anyhow::anyhow!("Invalid DBRow length"));
        }
        let size = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        if bytes.len() < size + 4 {
            return Err(anyhow::anyhow!("Invalid DBRow length"));
        }
        let key = bytes[4..size + 4].to_vec();
        let value = bytes[size + 4..].to_vec();
        Ok(DBRow {
            key,
            value,
        })
    }
}

impl<const SIZE: usize> KVQSerializable for [u8; SIZE] {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        Ok(self.to_vec())
    }
    fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        let mut result = [0u8; SIZE];
        result.copy_from_slice(bytes);
        Ok(result)
    }
}

impl KVQSerializable for Vec<u8> {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        Ok(self.clone())
    }
    fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(bytes.to_vec())
    }
}
impl<const SIZE: usize> KVQSerializable for [u64; SIZE] {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let mut result = Vec::with_capacity(SIZE * 8);
        for i in 0..SIZE {
            result.extend_from_slice(&self[i].to_be_bytes());
        }
        Ok(result)
    }
    fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        let mut result = [0u64; SIZE];
        for i in 0..SIZE {
            let mut bytes_u64 = [0u8; 8];
            bytes_u64.copy_from_slice(&bytes[i * 8..(i + 1) * 8]);
            result[i] = u64::from_be_bytes(bytes_u64);
        }
        Ok(result)
    }
}

impl<const SIZE: usize> KVQSerializable for [u32; SIZE] {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let mut result = Vec::with_capacity(SIZE * 4);
        for i in 0..SIZE {
            result.extend_from_slice(&self[i].to_be_bytes());
        }
        Ok(result)
    }
    fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        let mut result = [0u32; SIZE];
        for i in 0..SIZE {
            let mut bytes_u32 = [0u8; 4];
            bytes_u32.copy_from_slice(&bytes[i * 4..(i + 1) * 4]);
            result[i] = u32::from_be_bytes(bytes_u32);
        }
        Ok(result)
    }
}


