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

use serde_repr::{Deserialize_repr, Serialize_repr};


#[derive(
    Serialize_repr,
    Deserialize_repr,
    PartialEq,
    Debug,
    Clone,
    Copy,
    Eq,
    Hash,
    PartialOrd,
    Ord,
)]
#[repr(u32)]
pub enum QWorkerMode {
    All = 0,
    NoGroth16 = 1,
    OnlyGroth16 = 2,
}
impl QWorkerMode {
    pub fn to_u32(&self) -> u32 {
        *self as u32
    }
    pub fn is_groth16_enabled(&self) -> bool {
        match self {
            QWorkerMode::All => true,
            QWorkerMode::NoGroth16 => false,
            QWorkerMode::OnlyGroth16 => true,
        }
    }
}
impl From<QWorkerMode> for u32 {
    fn from(value: QWorkerMode) -> u32 {
        value as u32
    }
}
impl TryFrom<u32> for QWorkerMode {
    type Error = anyhow::Error;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(QWorkerMode::All),
            1 => Ok(QWorkerMode::NoGroth16),
            2 => Ok(QWorkerMode::OnlyGroth16),
            _ => Err(anyhow::format_err!("Invalid QWorkerMode value: {}", value)),
        }
    }
}

impl ToString for QWorkerMode {
    fn to_string(&self) -> String {
        match *self {
            QWorkerMode::All => "all".to_string(),
            QWorkerMode::NoGroth16 => "no-groth16".to_string(),
            QWorkerMode::OnlyGroth16 => "only-groth16".to_string(),
        }
    }
}