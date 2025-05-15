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

use std::{sync::{Arc, RwLock}, time::Instant};
const TIME_LONG: &str = "\x1b[48;5;124m";
const TIME_MEDIUM: &str = "\x1b[48;5;24m";
const TIME_FAST: &str = "\x1B[38;5;230m\x1b[48;5;34m";
fn get_time_color(elapsed_ms: u64) -> &'static str {
    if elapsed_ms > 2000 {
        TIME_LONG
    } else if elapsed_ms > 500 {
        TIME_MEDIUM
    } else {
        TIME_FAST
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DebugTimer {
    pub start_time: Instant,
    pub name: String,
}
impl DebugTimer {
    pub fn new(name: &str) -> Self {
        let n = name.to_string();
        Self {
            start_time: Instant::now(),
            name: n,
        }
    }
    pub fn lap(&mut self, event_name: &str) -> u64 {
        let elapsed = self.start_time.elapsed();
        let elapsed_ms = elapsed.as_millis() as u64;
        println!(
            "\x1b[96m{}\x1b[0m - \x1b[94m{}\x1b[0m: {} {}ms \x1b[0m",
            self.name,
            event_name,
            get_time_color(elapsed_ms),
            elapsed_ms
        );
        self.start_time = Instant::now();
        elapsed_ms
    }
    pub fn event(&mut self, event_name: String) -> u64 {
        let elapsed = self.start_time.elapsed();
        let elapsed_ms = elapsed.as_millis() as u64;
        println!(
            "\x1b[96m{}\x1b[0m - \x1b[94m{}\x1b[0m: {} {}ms \x1b[0m",
            self.name,
            event_name,
            get_time_color(elapsed_ms),
            elapsed_ms
        );
        self.start_time = Instant::now();
        elapsed_ms
    }
    pub fn batch_average(&mut self, event_name: &str, batch_item_type: &str, batch_size: usize) -> (u64, u64) {
        let elapsed = self.start_time.elapsed();
        let elapsed_ms = elapsed.as_millis() as u64;
        println!(
            "\x1b[96m{}\x1b[0m - \x1b[94m{} ({}x - {})\x1b[0m: {} {}ms \x1b[0m",
            self.name,
            event_name,
            batch_size,
            batch_item_type,
            get_time_color(elapsed_ms),
            elapsed_ms
        );
        let per_time = ((elapsed_ms as f64) / (batch_size as f64)).floor() as u64;
        println!(
            "\x1b[96m{}\x1b[0m - \x1b[94m({}) Per {}\x1b[0m: {} {}ms \x1b[0m",
            self.name,
            event_name,
            batch_item_type,
            get_time_color(per_time),
            per_time
        );
        self.start_time = Instant::now();
        (elapsed_ms, per_time)
    }
}



#[derive(Debug, Clone)]
pub struct ImmDebugTimer {
    pub inner: Arc<RwLock<DebugTimer>>,
}
impl ImmDebugTimer {
    pub fn new(name: &str) -> Self {
        Self {
            inner: Arc::new(RwLock::new(DebugTimer::new(name))),
        }
    }
    pub fn lap(&self, event_name: &str) -> u64 {
        self.inner.write().unwrap().lap(event_name)
    }
    pub fn event(&self, event_name: String) -> u64 {
        self.inner.write().unwrap().event(event_name)
    }
    pub fn batch_average(&self, event_name: &str, batch_item_type: &str, batch_size: usize) -> (u64, u64) {
        self.inner.write().unwrap().batch_average(event_name, batch_item_type, batch_size)
    }
}