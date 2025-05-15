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


/*
NO_FORMAT="\x1b[0m"
F_BOLD="\x1b[1m"
C_LIGHTGOLDENROD1="\x1b[38;5;227m"
C_PURPLE="\x1b[48;5;93m"
echo -e "${F_BOLD}${C_LIGHTGOLDENROD1}${C_PURPLE}Color me, surprised${NO_FORMAT}"

NO_FORMAT="\x1b[0m"
C_GREENYELLOW="\x1b[48;5;154m"
echo -e "${C_GREENYELLOW}Violets are blue${NO_FORMAT}"

C_MEDIUMTURQUOISE="\x1b[38;5;80m"

C_WHEAT1="\x1b[38;5;229m"
C_DEEPSKYBLUE4="\x1b[48;5;23m"
*/
use std::time::Instant;
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

#[derive(Clone)]
pub struct TraceTimer {
    pub start_time: Instant,
    pub name: String,
}
impl TraceTimer {
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
            "\x1b[1m\x1b[38;5;227m\x1b[48;5;93m{}\x1b[0m - \x1b[94m{}\x1b[0m: {} {}ms \x1b[0m",
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
            "\x1b[1m\x1b[38;5;227m\x1b[48;5;93m{}\x1b[0m - \x1b[94m{}\x1b[0m: {} {}ms \x1b[0m",
            self.name,
            event_name,
            get_time_color(elapsed_ms),
            elapsed_ms
        );
        self.start_time = Instant::now();
        elapsed_ms
    }
}
