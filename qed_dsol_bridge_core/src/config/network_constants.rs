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

use doge_light_client::{block_data_tracker::BlockDataTracker, chain_state::QEDDogeChainStateCore, common_types::QHash256, hash::merkle::fixed_append_tree::FixedMerkleAppendTree};



// start constant channels
pub const BLOCK_API_REGISTER_USER_CHANNEL_ID: u64 = 0xCC524547555352;
pub const COORD_API_DEPLOY_CONTRACT_CHANNEL_ID: u64 = 0xCC444550434F4E;
pub const COORD_API_GUTA_FROM_REALMS_CHANNEL_ID: u64 = 0xCC475554414652;


pub const REALM_API_GUTA_FROM_USER_CHANNEL_ID: u64 = 0x22475554414652;
pub const REALM_API_UPDATE_CONTRACT_STATE_TREE_CHANNEL_ID: u64 = 0x22435354555044;

pub const QED_CHECKPOINT_SYNC_INFO_COMPACT_DRAIN_QUEUE_CHANNEL: u64 = 0x901337123;
pub const CST_USER_UPDATE_CHANNEL_ID: u64 = 0x101337;



// new stuff

pub const QDOGE_BRIDGE_REQUIRED_CONFIRMATIONS: usize = 1;
pub const QDOGE_BRIDGE_BLOCK_HASH_CACHE_SIZE: usize = 32;
pub const QDOGE_BRIDGE_BLOCK_TREE_HEIGHT: usize = 32;
// end parameters

pub type QEDDogeChainState = QEDDogeChainStateCore<
    QDOGE_BRIDGE_BLOCK_HASH_CACHE_SIZE,
    QDOGE_BRIDGE_BLOCK_TREE_HEIGHT,
>;

pub type QBlockDataTracker = BlockDataTracker<QDOGE_BRIDGE_BLOCK_HASH_CACHE_SIZE>;
pub type QBlockTreeTracker = FixedMerkleAppendTree<QHash256, QDOGE_BRIDGE_BLOCK_TREE_HEIGHT>;

