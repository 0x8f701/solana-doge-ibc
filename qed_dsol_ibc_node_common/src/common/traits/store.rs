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

use kvq::traits::KVQSerializable;

pub trait QEDBlobStoreReader {
    fn get_bin(&self, key: Vec<u8>) -> anyhow::Result<Vec<u8>>;
    fn exists_bin(&self, key: Vec<u8>) -> anyhow::Result<bool>;
    fn get<K: KVQSerializable, V: KVQSerializable>(&self, key: &K) -> anyhow::Result<V> {
        V::from_bytes(&self.get_bin(key.to_bytes()?)?)
    }
    fn exists<K: KVQSerializable>(&self, key: &K) -> anyhow::Result<bool> {
        self.exists_bin(key.to_bytes()?)
    }
}
pub trait QEDBlobStoreMut: QEDBlobStoreReader {
    fn set_bin_mut(&mut self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<()>;
    fn remove_bin_mut(&mut self, key: Vec<u8>) -> anyhow::Result<bool>;
    fn pop_bin_mut(&mut self, key: Vec<u8>) -> anyhow::Result<Option<Vec<u8>>>;

    fn set_mut<K: KVQSerializable, V: KVQSerializable>(&mut self, key: &K, value: &V) -> anyhow::Result<()> {
        self.set_bin_mut(key.to_bytes()?, value.to_bytes()?)
    }
    fn remove_mut<K: KVQSerializable>(&mut self, key: &K) -> anyhow::Result<bool> {
        self.remove_bin_mut(key.to_bytes()?)
    }
    fn pop_mut<K: KVQSerializable, V: KVQSerializable>(&mut self, key: &K) -> anyhow::Result<Option<V>> {
        Ok(
            match self.pop_bin_mut(key.to_bytes()?)? {
                Some(data) => Some(V::from_bytes(&data)?),
                None => None,
            }
        )
    }
}



pub trait QEDBlobStoreImm: QEDBlobStoreReader {
    fn set_bin_imm(&self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<()>;
    fn get_bin_imm(&self, key: Vec<u8>) -> anyhow::Result<Vec<u8>>;
    fn remove_bin_imm(&self, key: Vec<u8>) -> anyhow::Result<bool>;
    fn pop_bin_imm(&self, key: Vec<u8>) -> anyhow::Result<Option<Vec<u8>>>;

    fn set_imm<K: KVQSerializable, V: KVQSerializable>(&self, key: &K, value: &V) -> anyhow::Result<()> {
        self.set_bin_imm(key.to_bytes()?, value.to_bytes()?)
    }
    fn remove_imm<K: KVQSerializable>(&self, key: &K) -> anyhow::Result<bool> {
        self.remove_bin_imm(key.to_bytes()?)
    }
    fn pop_imm<K: KVQSerializable, V: KVQSerializable>(&self, key: &K) -> anyhow::Result<Option<V>> {
        Ok(
            match self.pop_bin_imm(key.to_bytes()?)? {
                Some(data) => Some(V::from_bytes(&data)?),
                None => None,
            }
        )
    }
}


