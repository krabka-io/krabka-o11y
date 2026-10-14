// Derived from golang.org/x/text v0.40.0 cases.Title(AmericanEnglish, NoLower).
// Source cases/map.go SHA256 2e7f128e208198348b041d4e32f5bad8884ea5246a0d24fdcb8c02f06391cbe6
// Source cases/tables17.0.0.go SHA256 1298c8c19141f894f8b6ab79c23b7e31df554bf14492d1dae3d279f6d4a37419
// Copyright 2009 The Go Authors.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//    * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//    * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//    * Neither the name of Google LLC nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#[derive(Clone, Copy)]
pub(super) struct RangeFlags {
    #[allow(dead_code)] // Retained from the generated Unicode range table.
    pub start: u32,
    pub end: u32,
    pub flags: u8,
}

pub(super) const RANGES: &[RangeFlags] = &[
    RangeFlags {
        start: 0x0,
        end: 0x26,
        flags: 2,
    },
    RangeFlags {
        start: 0x27,
        end: 0x27,
        flags: 4,
    },
    RangeFlags {
        start: 0x28,
        end: 0x2d,
        flags: 2,
    },
    RangeFlags {
        start: 0x2e,
        end: 0x2e,
        flags: 4,
    },
    RangeFlags {
        start: 0x2f,
        end: 0x2f,
        flags: 2,
    },
    RangeFlags {
        start: 0x30,
        end: 0x39,
        flags: 0,
    },
    RangeFlags {
        start: 0x3a,
        end: 0x3a,
        flags: 4,
    },
    RangeFlags {
        start: 0x3b,
        end: 0x40,
        flags: 2,
    },
    RangeFlags {
        start: 0x41,
        end: 0x5a,
        flags: 1,
    },
    RangeFlags {
        start: 0x5b,
        end: 0x5e,
        flags: 2,
    },
    RangeFlags {
        start: 0x5f,
        end: 0x5f,
        flags: 0,
    },
    RangeFlags {
        start: 0x60,
        end: 0x60,
        flags: 2,
    },
    RangeFlags {
        start: 0x61,
        end: 0x7a,
        flags: 1,
    },
    RangeFlags {
        start: 0x7b,
        end: 0xa9,
        flags: 2,
    },
    RangeFlags {
        start: 0xaa,
        end: 0xaa,
        flags: 1,
    },
    RangeFlags {
        start: 0xab,
        end: 0xac,
        flags: 2,
    },
    RangeFlags {
        start: 0xad,
        end: 0xad,
        flags: 0,
    },
    RangeFlags {
        start: 0xae,
        end: 0xb4,
        flags: 2,
    },
    RangeFlags {
        start: 0xb5,
        end: 0xb5,
        flags: 1,
    },
    RangeFlags {
        start: 0xb6,
        end: 0xb6,
        flags: 2,
    },
    RangeFlags {
        start: 0xb7,
        end: 0xb7,
        flags: 4,
    },
    RangeFlags {
        start: 0xb8,
        end: 0xb9,
        flags: 2,
    },
    RangeFlags {
        start: 0xba,
        end: 0xba,
        flags: 1,
    },
    RangeFlags {
        start: 0xbb,
        end: 0xbf,
        flags: 2,
    },
    RangeFlags {
        start: 0xc0,
        end: 0xd6,
        flags: 1,
    },
    RangeFlags {
        start: 0xd7,
        end: 0xd7,
        flags: 2,
    },
    RangeFlags {
        start: 0xd8,
        end: 0xf6,
        flags: 1,
    },
    RangeFlags {
        start: 0xf7,
        end: 0xf7,
        flags: 2,
    },
    RangeFlags {
        start: 0xf8,
        end: 0x1ba,
        flags: 1,
    },
    RangeFlags {
        start: 0x1bb,
        end: 0x1bb,
        flags: 0,
    },
    RangeFlags {
        start: 0x1bc,
        end: 0x1bf,
        flags: 1,
    },
    RangeFlags {
        start: 0x1c0,
        end: 0x1c3,
        flags: 0,
    },
    RangeFlags {
        start: 0x1c4,
        end: 0x293,
        flags: 1,
    },
    RangeFlags {
        start: 0x294,
        end: 0x294,
        flags: 0,
    },
    RangeFlags {
        start: 0x295,
        end: 0x2b8,
        flags: 1,
    },
    RangeFlags {
        start: 0x2b9,
        end: 0x2bf,
        flags: 0,
    },
    RangeFlags {
        start: 0x2c0,
        end: 0x2c1,
        flags: 1,
    },
    RangeFlags {
        start: 0x2c2,
        end: 0x2d7,
        flags: 0,
    },
    RangeFlags {
        start: 0x2d8,
        end: 0x2dd,
        flags: 2,
    },
    RangeFlags {
        start: 0x2de,
        end: 0x2df,
        flags: 0,
    },
    RangeFlags {
        start: 0x2e0,
        end: 0x2e4,
        flags: 1,
    },
    RangeFlags {
        start: 0x2e5,
        end: 0x344,
        flags: 0,
    },
    RangeFlags {
        start: 0x345,
        end: 0x345,
        flags: 1,
    },
    RangeFlags {
        start: 0x346,
        end: 0x36f,
        flags: 0,
    },
    RangeFlags {
        start: 0x370,
        end: 0x373,
        flags: 1,
    },
    RangeFlags {
        start: 0x374,
        end: 0x374,
        flags: 0,
    },
    RangeFlags {
        start: 0x375,
        end: 0x375,
        flags: 2,
    },
    RangeFlags {
        start: 0x376,
        end: 0x377,
        flags: 1,
    },
    RangeFlags {
        start: 0x378,
        end: 0x379,
        flags: 2,
    },
    RangeFlags {
        start: 0x37a,
        end: 0x37d,
        flags: 1,
    },
    RangeFlags {
        start: 0x37e,
        end: 0x37e,
        flags: 2,
    },
    RangeFlags {
        start: 0x37f,
        end: 0x37f,
        flags: 1,
    },
    RangeFlags {
        start: 0x380,
        end: 0x385,
        flags: 2,
    },
    RangeFlags {
        start: 0x386,
        end: 0x386,
        flags: 1,
    },
    RangeFlags {
        start: 0x387,
        end: 0x387,
        flags: 4,
    },
    RangeFlags {
        start: 0x388,
        end: 0x38a,
        flags: 1,
    },
    RangeFlags {
        start: 0x38b,
        end: 0x38b,
        flags: 2,
    },
    RangeFlags {
        start: 0x38c,
        end: 0x38c,
        flags: 1,
    },
    RangeFlags {
        start: 0x38d,
        end: 0x38d,
        flags: 2,
    },
    RangeFlags {
        start: 0x38e,
        end: 0x3a1,
        flags: 1,
    },
    RangeFlags {
        start: 0x3a2,
        end: 0x3a2,
        flags: 2,
    },
    RangeFlags {
        start: 0x3a3,
        end: 0x3f5,
        flags: 1,
    },
    RangeFlags {
        start: 0x3f6,
        end: 0x3f6,
        flags: 2,
    },
    RangeFlags {
        start: 0x3f7,
        end: 0x481,
        flags: 1,
    },
    RangeFlags {
        start: 0x482,
        end: 0x482,
        flags: 2,
    },
    RangeFlags {
        start: 0x483,
        end: 0x489,
        flags: 0,
    },
    RangeFlags {
        start: 0x48a,
        end: 0x52f,
        flags: 1,
    },
    RangeFlags {
        start: 0x530,
        end: 0x530,
        flags: 2,
    },
    RangeFlags {
        start: 0x531,
        end: 0x556,
        flags: 1,
    },
    RangeFlags {
        start: 0x557,
        end: 0x558,
        flags: 2,
    },
    RangeFlags {
        start: 0x559,
        end: 0x55c,
        flags: 0,
    },
    RangeFlags {
        start: 0x55d,
        end: 0x55d,
        flags: 2,
    },
    RangeFlags {
        start: 0x55e,
        end: 0x55e,
        flags: 0,
    },
    RangeFlags {
        start: 0x55f,
        end: 0x55f,
        flags: 4,
    },
    RangeFlags {
        start: 0x560,
        end: 0x588,
        flags: 1,
    },
    RangeFlags {
        start: 0x589,
        end: 0x589,
        flags: 2,
    },
    RangeFlags {
        start: 0x58a,
        end: 0x58a,
        flags: 0,
    },
    RangeFlags {
        start: 0x58b,
        end: 0x590,
        flags: 2,
    },
    RangeFlags {
        start: 0x591,
        end: 0x5bd,
        flags: 0,
    },
    RangeFlags {
        start: 0x5be,
        end: 0x5be,
        flags: 2,
    },
    RangeFlags {
        start: 0x5bf,
        end: 0x5bf,
        flags: 0,
    },
    RangeFlags {
        start: 0x5c0,
        end: 0x5c0,
        flags: 2,
    },
    RangeFlags {
        start: 0x5c1,
        end: 0x5c2,
        flags: 0,
    },
    RangeFlags {
        start: 0x5c3,
        end: 0x5c3,
        flags: 2,
    },
    RangeFlags {
        start: 0x5c4,
        end: 0x5c5,
        flags: 0,
    },
    RangeFlags {
        start: 0x5c6,
        end: 0x5c6,
        flags: 2,
    },
    RangeFlags {
        start: 0x5c7,
        end: 0x5c7,
        flags: 0,
    },
    RangeFlags {
        start: 0x5c8,
        end: 0x5cf,
        flags: 2,
    },
    RangeFlags {
        start: 0x5d0,
        end: 0x5ea,
        flags: 0,
    },
    RangeFlags {
        start: 0x5eb,
        end: 0x5ee,
        flags: 2,
    },
    RangeFlags {
        start: 0x5ef,
        end: 0x5f3,
        flags: 0,
    },
    RangeFlags {
        start: 0x5f4,
        end: 0x5f4,
        flags: 4,
    },
    RangeFlags {
        start: 0x5f5,
        end: 0x5ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x600,
        end: 0x605,
        flags: 0,
    },
    RangeFlags {
        start: 0x606,
        end: 0x60f,
        flags: 2,
    },
    RangeFlags {
        start: 0x610,
        end: 0x61a,
        flags: 0,
    },
    RangeFlags {
        start: 0x61b,
        end: 0x61b,
        flags: 2,
    },
    RangeFlags {
        start: 0x61c,
        end: 0x61c,
        flags: 0,
    },
    RangeFlags {
        start: 0x61d,
        end: 0x61f,
        flags: 2,
    },
    RangeFlags {
        start: 0x620,
        end: 0x669,
        flags: 0,
    },
    RangeFlags {
        start: 0x66a,
        end: 0x66a,
        flags: 2,
    },
    RangeFlags {
        start: 0x66b,
        end: 0x66b,
        flags: 0,
    },
    RangeFlags {
        start: 0x66c,
        end: 0x66d,
        flags: 2,
    },
    RangeFlags {
        start: 0x66e,
        end: 0x6d3,
        flags: 0,
    },
    RangeFlags {
        start: 0x6d4,
        end: 0x6d4,
        flags: 2,
    },
    RangeFlags {
        start: 0x6d5,
        end: 0x6dd,
        flags: 0,
    },
    RangeFlags {
        start: 0x6de,
        end: 0x6de,
        flags: 2,
    },
    RangeFlags {
        start: 0x6df,
        end: 0x6e8,
        flags: 0,
    },
    RangeFlags {
        start: 0x6e9,
        end: 0x6e9,
        flags: 2,
    },
    RangeFlags {
        start: 0x6ea,
        end: 0x6fc,
        flags: 0,
    },
    RangeFlags {
        start: 0x6fd,
        end: 0x6fe,
        flags: 2,
    },
    RangeFlags {
        start: 0x6ff,
        end: 0x6ff,
        flags: 0,
    },
    RangeFlags {
        start: 0x700,
        end: 0x70e,
        flags: 2,
    },
    RangeFlags {
        start: 0x70f,
        end: 0x74a,
        flags: 0,
    },
    RangeFlags {
        start: 0x74b,
        end: 0x74c,
        flags: 2,
    },
    RangeFlags {
        start: 0x74d,
        end: 0x7b1,
        flags: 0,
    },
    RangeFlags {
        start: 0x7b2,
        end: 0x7bf,
        flags: 2,
    },
    RangeFlags {
        start: 0x7c0,
        end: 0x7f5,
        flags: 0,
    },
    RangeFlags {
        start: 0x7f6,
        end: 0x7f9,
        flags: 2,
    },
    RangeFlags {
        start: 0x7fa,
        end: 0x7fa,
        flags: 0,
    },
    RangeFlags {
        start: 0x7fb,
        end: 0x7fc,
        flags: 2,
    },
    RangeFlags {
        start: 0x7fd,
        end: 0x7fd,
        flags: 0,
    },
    RangeFlags {
        start: 0x7fe,
        end: 0x7ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x800,
        end: 0x82d,
        flags: 0,
    },
    RangeFlags {
        start: 0x82e,
        end: 0x83f,
        flags: 2,
    },
    RangeFlags {
        start: 0x840,
        end: 0x85b,
        flags: 0,
    },
    RangeFlags {
        start: 0x85c,
        end: 0x85f,
        flags: 2,
    },
    RangeFlags {
        start: 0x860,
        end: 0x86a,
        flags: 0,
    },
    RangeFlags {
        start: 0x86b,
        end: 0x86f,
        flags: 2,
    },
    RangeFlags {
        start: 0x870,
        end: 0x887,
        flags: 0,
    },
    RangeFlags {
        start: 0x888,
        end: 0x888,
        flags: 2,
    },
    RangeFlags {
        start: 0x889,
        end: 0x88e,
        flags: 0,
    },
    RangeFlags {
        start: 0x88f,
        end: 0x88f,
        flags: 2,
    },
    RangeFlags {
        start: 0x890,
        end: 0x891,
        flags: 0,
    },
    RangeFlags {
        start: 0x892,
        end: 0x897,
        flags: 2,
    },
    RangeFlags {
        start: 0x898,
        end: 0x963,
        flags: 0,
    },
    RangeFlags {
        start: 0x964,
        end: 0x965,
        flags: 2,
    },
    RangeFlags {
        start: 0x966,
        end: 0x96f,
        flags: 0,
    },
    RangeFlags {
        start: 0x970,
        end: 0x970,
        flags: 2,
    },
    RangeFlags {
        start: 0x971,
        end: 0x983,
        flags: 0,
    },
    RangeFlags {
        start: 0x984,
        end: 0x984,
        flags: 2,
    },
    RangeFlags {
        start: 0x985,
        end: 0x98c,
        flags: 0,
    },
    RangeFlags {
        start: 0x98d,
        end: 0x98e,
        flags: 2,
    },
    RangeFlags {
        start: 0x98f,
        end: 0x990,
        flags: 0,
    },
    RangeFlags {
        start: 0x991,
        end: 0x992,
        flags: 2,
    },
    RangeFlags {
        start: 0x993,
        end: 0x9a8,
        flags: 0,
    },
    RangeFlags {
        start: 0x9a9,
        end: 0x9a9,
        flags: 2,
    },
    RangeFlags {
        start: 0x9aa,
        end: 0x9b0,
        flags: 0,
    },
    RangeFlags {
        start: 0x9b1,
        end: 0x9b1,
        flags: 2,
    },
    RangeFlags {
        start: 0x9b2,
        end: 0x9b2,
        flags: 0,
    },
    RangeFlags {
        start: 0x9b3,
        end: 0x9b5,
        flags: 2,
    },
    RangeFlags {
        start: 0x9b6,
        end: 0x9b9,
        flags: 0,
    },
    RangeFlags {
        start: 0x9ba,
        end: 0x9bb,
        flags: 2,
    },
    RangeFlags {
        start: 0x9bc,
        end: 0x9c4,
        flags: 0,
    },
    RangeFlags {
        start: 0x9c5,
        end: 0x9c6,
        flags: 2,
    },
    RangeFlags {
        start: 0x9c7,
        end: 0x9c8,
        flags: 0,
    },
    RangeFlags {
        start: 0x9c9,
        end: 0x9ca,
        flags: 2,
    },
    RangeFlags {
        start: 0x9cb,
        end: 0x9ce,
        flags: 0,
    },
    RangeFlags {
        start: 0x9cf,
        end: 0x9d6,
        flags: 2,
    },
    RangeFlags {
        start: 0x9d7,
        end: 0x9d7,
        flags: 0,
    },
    RangeFlags {
        start: 0x9d8,
        end: 0x9db,
        flags: 2,
    },
    RangeFlags {
        start: 0x9dc,
        end: 0x9dd,
        flags: 0,
    },
    RangeFlags {
        start: 0x9de,
        end: 0x9de,
        flags: 2,
    },
    RangeFlags {
        start: 0x9df,
        end: 0x9e3,
        flags: 0,
    },
    RangeFlags {
        start: 0x9e4,
        end: 0x9e5,
        flags: 2,
    },
    RangeFlags {
        start: 0x9e6,
        end: 0x9f1,
        flags: 0,
    },
    RangeFlags {
        start: 0x9f2,
        end: 0x9fb,
        flags: 2,
    },
    RangeFlags {
        start: 0x9fc,
        end: 0x9fc,
        flags: 0,
    },
    RangeFlags {
        start: 0x9fd,
        end: 0x9fd,
        flags: 2,
    },
    RangeFlags {
        start: 0x9fe,
        end: 0x9fe,
        flags: 0,
    },
    RangeFlags {
        start: 0x9ff,
        end: 0xa00,
        flags: 2,
    },
    RangeFlags {
        start: 0xa01,
        end: 0xa03,
        flags: 0,
    },
    RangeFlags {
        start: 0xa04,
        end: 0xa04,
        flags: 2,
    },
    RangeFlags {
        start: 0xa05,
        end: 0xa0a,
        flags: 0,
    },
    RangeFlags {
        start: 0xa0b,
        end: 0xa0e,
        flags: 2,
    },
    RangeFlags {
        start: 0xa0f,
        end: 0xa10,
        flags: 0,
    },
    RangeFlags {
        start: 0xa11,
        end: 0xa12,
        flags: 2,
    },
    RangeFlags {
        start: 0xa13,
        end: 0xa28,
        flags: 0,
    },
    RangeFlags {
        start: 0xa29,
        end: 0xa29,
        flags: 2,
    },
    RangeFlags {
        start: 0xa2a,
        end: 0xa30,
        flags: 0,
    },
    RangeFlags {
        start: 0xa31,
        end: 0xa31,
        flags: 2,
    },
    RangeFlags {
        start: 0xa32,
        end: 0xa33,
        flags: 0,
    },
    RangeFlags {
        start: 0xa34,
        end: 0xa34,
        flags: 2,
    },
    RangeFlags {
        start: 0xa35,
        end: 0xa36,
        flags: 0,
    },
    RangeFlags {
        start: 0xa37,
        end: 0xa37,
        flags: 2,
    },
    RangeFlags {
        start: 0xa38,
        end: 0xa39,
        flags: 0,
    },
    RangeFlags {
        start: 0xa3a,
        end: 0xa3b,
        flags: 2,
    },
    RangeFlags {
        start: 0xa3c,
        end: 0xa3c,
        flags: 0,
    },
    RangeFlags {
        start: 0xa3d,
        end: 0xa3d,
        flags: 2,
    },
    RangeFlags {
        start: 0xa3e,
        end: 0xa42,
        flags: 0,
    },
    RangeFlags {
        start: 0xa43,
        end: 0xa46,
        flags: 2,
    },
    RangeFlags {
        start: 0xa47,
        end: 0xa48,
        flags: 0,
    },
    RangeFlags {
        start: 0xa49,
        end: 0xa4a,
        flags: 2,
    },
    RangeFlags {
        start: 0xa4b,
        end: 0xa4d,
        flags: 0,
    },
    RangeFlags {
        start: 0xa4e,
        end: 0xa50,
        flags: 2,
    },
    RangeFlags {
        start: 0xa51,
        end: 0xa51,
        flags: 0,
    },
    RangeFlags {
        start: 0xa52,
        end: 0xa58,
        flags: 2,
    },
    RangeFlags {
        start: 0xa59,
        end: 0xa5c,
        flags: 0,
    },
    RangeFlags {
        start: 0xa5d,
        end: 0xa5d,
        flags: 2,
    },
    RangeFlags {
        start: 0xa5e,
        end: 0xa5e,
        flags: 0,
    },
    RangeFlags {
        start: 0xa5f,
        end: 0xa65,
        flags: 2,
    },
    RangeFlags {
        start: 0xa66,
        end: 0xa75,
        flags: 0,
    },
    RangeFlags {
        start: 0xa76,
        end: 0xa80,
        flags: 2,
    },
    RangeFlags {
        start: 0xa81,
        end: 0xa83,
        flags: 0,
    },
    RangeFlags {
        start: 0xa84,
        end: 0xa84,
        flags: 2,
    },
    RangeFlags {
        start: 0xa85,
        end: 0xa8d,
        flags: 0,
    },
    RangeFlags {
        start: 0xa8e,
        end: 0xa8e,
        flags: 2,
    },
    RangeFlags {
        start: 0xa8f,
        end: 0xa91,
        flags: 0,
    },
    RangeFlags {
        start: 0xa92,
        end: 0xa92,
        flags: 2,
    },
    RangeFlags {
        start: 0xa93,
        end: 0xaa8,
        flags: 0,
    },
    RangeFlags {
        start: 0xaa9,
        end: 0xaa9,
        flags: 2,
    },
    RangeFlags {
        start: 0xaaa,
        end: 0xab0,
        flags: 0,
    },
    RangeFlags {
        start: 0xab1,
        end: 0xab1,
        flags: 2,
    },
    RangeFlags {
        start: 0xab2,
        end: 0xab3,
        flags: 0,
    },
    RangeFlags {
        start: 0xab4,
        end: 0xab4,
        flags: 2,
    },
    RangeFlags {
        start: 0xab5,
        end: 0xab9,
        flags: 0,
    },
    RangeFlags {
        start: 0xaba,
        end: 0xabb,
        flags: 2,
    },
    RangeFlags {
        start: 0xabc,
        end: 0xac5,
        flags: 0,
    },
    RangeFlags {
        start: 0xac6,
        end: 0xac6,
        flags: 2,
    },
    RangeFlags {
        start: 0xac7,
        end: 0xac9,
        flags: 0,
    },
    RangeFlags {
        start: 0xaca,
        end: 0xaca,
        flags: 2,
    },
    RangeFlags {
        start: 0xacb,
        end: 0xacd,
        flags: 0,
    },
    RangeFlags {
        start: 0xace,
        end: 0xacf,
        flags: 2,
    },
    RangeFlags {
        start: 0xad0,
        end: 0xad0,
        flags: 0,
    },
    RangeFlags {
        start: 0xad1,
        end: 0xadf,
        flags: 2,
    },
    RangeFlags {
        start: 0xae0,
        end: 0xae3,
        flags: 0,
    },
    RangeFlags {
        start: 0xae4,
        end: 0xae5,
        flags: 2,
    },
    RangeFlags {
        start: 0xae6,
        end: 0xaef,
        flags: 0,
    },
    RangeFlags {
        start: 0xaf0,
        end: 0xaf8,
        flags: 2,
    },
    RangeFlags {
        start: 0xaf9,
        end: 0xaff,
        flags: 0,
    },
    RangeFlags {
        start: 0xb00,
        end: 0xb00,
        flags: 2,
    },
    RangeFlags {
        start: 0xb01,
        end: 0xb03,
        flags: 0,
    },
    RangeFlags {
        start: 0xb04,
        end: 0xb04,
        flags: 2,
    },
    RangeFlags {
        start: 0xb05,
        end: 0xb0c,
        flags: 0,
    },
    RangeFlags {
        start: 0xb0d,
        end: 0xb0e,
        flags: 2,
    },
    RangeFlags {
        start: 0xb0f,
        end: 0xb10,
        flags: 0,
    },
    RangeFlags {
        start: 0xb11,
        end: 0xb12,
        flags: 2,
    },
    RangeFlags {
        start: 0xb13,
        end: 0xb28,
        flags: 0,
    },
    RangeFlags {
        start: 0xb29,
        end: 0xb29,
        flags: 2,
    },
    RangeFlags {
        start: 0xb2a,
        end: 0xb30,
        flags: 0,
    },
    RangeFlags {
        start: 0xb31,
        end: 0xb31,
        flags: 2,
    },
    RangeFlags {
        start: 0xb32,
        end: 0xb33,
        flags: 0,
    },
    RangeFlags {
        start: 0xb34,
        end: 0xb34,
        flags: 2,
    },
    RangeFlags {
        start: 0xb35,
        end: 0xb39,
        flags: 0,
    },
    RangeFlags {
        start: 0xb3a,
        end: 0xb3b,
        flags: 2,
    },
    RangeFlags {
        start: 0xb3c,
        end: 0xb44,
        flags: 0,
    },
    RangeFlags {
        start: 0xb45,
        end: 0xb46,
        flags: 2,
    },
    RangeFlags {
        start: 0xb47,
        end: 0xb48,
        flags: 0,
    },
    RangeFlags {
        start: 0xb49,
        end: 0xb4a,
        flags: 2,
    },
    RangeFlags {
        start: 0xb4b,
        end: 0xb4d,
        flags: 0,
    },
    RangeFlags {
        start: 0xb4e,
        end: 0xb54,
        flags: 2,
    },
    RangeFlags {
        start: 0xb55,
        end: 0xb57,
        flags: 0,
    },
    RangeFlags {
        start: 0xb58,
        end: 0xb5b,
        flags: 2,
    },
    RangeFlags {
        start: 0xb5c,
        end: 0xb5d,
        flags: 0,
    },
    RangeFlags {
        start: 0xb5e,
        end: 0xb5e,
        flags: 2,
    },
    RangeFlags {
        start: 0xb5f,
        end: 0xb63,
        flags: 0,
    },
    RangeFlags {
        start: 0xb64,
        end: 0xb65,
        flags: 2,
    },
    RangeFlags {
        start: 0xb66,
        end: 0xb6f,
        flags: 0,
    },
    RangeFlags {
        start: 0xb70,
        end: 0xb70,
        flags: 2,
    },
    RangeFlags {
        start: 0xb71,
        end: 0xb71,
        flags: 0,
    },
    RangeFlags {
        start: 0xb72,
        end: 0xb81,
        flags: 2,
    },
    RangeFlags {
        start: 0xb82,
        end: 0xb83,
        flags: 0,
    },
    RangeFlags {
        start: 0xb84,
        end: 0xb84,
        flags: 2,
    },
    RangeFlags {
        start: 0xb85,
        end: 0xb8a,
        flags: 0,
    },
    RangeFlags {
        start: 0xb8b,
        end: 0xb8d,
        flags: 2,
    },
    RangeFlags {
        start: 0xb8e,
        end: 0xb90,
        flags: 0,
    },
    RangeFlags {
        start: 0xb91,
        end: 0xb91,
        flags: 2,
    },
    RangeFlags {
        start: 0xb92,
        end: 0xb95,
        flags: 0,
    },
    RangeFlags {
        start: 0xb96,
        end: 0xb98,
        flags: 2,
    },
    RangeFlags {
        start: 0xb99,
        end: 0xb9a,
        flags: 0,
    },
    RangeFlags {
        start: 0xb9b,
        end: 0xb9b,
        flags: 2,
    },
    RangeFlags {
        start: 0xb9c,
        end: 0xb9c,
        flags: 0,
    },
    RangeFlags {
        start: 0xb9d,
        end: 0xb9d,
        flags: 2,
    },
    RangeFlags {
        start: 0xb9e,
        end: 0xb9f,
        flags: 0,
    },
    RangeFlags {
        start: 0xba0,
        end: 0xba2,
        flags: 2,
    },
    RangeFlags {
        start: 0xba3,
        end: 0xba4,
        flags: 0,
    },
    RangeFlags {
        start: 0xba5,
        end: 0xba7,
        flags: 2,
    },
    RangeFlags {
        start: 0xba8,
        end: 0xbaa,
        flags: 0,
    },
    RangeFlags {
        start: 0xbab,
        end: 0xbad,
        flags: 2,
    },
    RangeFlags {
        start: 0xbae,
        end: 0xbb9,
        flags: 0,
    },
    RangeFlags {
        start: 0xbba,
        end: 0xbbd,
        flags: 2,
    },
    RangeFlags {
        start: 0xbbe,
        end: 0xbc2,
        flags: 0,
    },
    RangeFlags {
        start: 0xbc3,
        end: 0xbc5,
        flags: 2,
    },
    RangeFlags {
        start: 0xbc6,
        end: 0xbc8,
        flags: 0,
    },
    RangeFlags {
        start: 0xbc9,
        end: 0xbc9,
        flags: 2,
    },
    RangeFlags {
        start: 0xbca,
        end: 0xbcd,
        flags: 0,
    },
    RangeFlags {
        start: 0xbce,
        end: 0xbcf,
        flags: 2,
    },
    RangeFlags {
        start: 0xbd0,
        end: 0xbd0,
        flags: 0,
    },
    RangeFlags {
        start: 0xbd1,
        end: 0xbd6,
        flags: 2,
    },
    RangeFlags {
        start: 0xbd7,
        end: 0xbd7,
        flags: 0,
    },
    RangeFlags {
        start: 0xbd8,
        end: 0xbe5,
        flags: 2,
    },
    RangeFlags {
        start: 0xbe6,
        end: 0xbef,
        flags: 0,
    },
    RangeFlags {
        start: 0xbf0,
        end: 0xbff,
        flags: 2,
    },
    RangeFlags {
        start: 0xc00,
        end: 0xc0c,
        flags: 0,
    },
    RangeFlags {
        start: 0xc0d,
        end: 0xc0d,
        flags: 2,
    },
    RangeFlags {
        start: 0xc0e,
        end: 0xc10,
        flags: 0,
    },
    RangeFlags {
        start: 0xc11,
        end: 0xc11,
        flags: 2,
    },
    RangeFlags {
        start: 0xc12,
        end: 0xc28,
        flags: 0,
    },
    RangeFlags {
        start: 0xc29,
        end: 0xc29,
        flags: 2,
    },
    RangeFlags {
        start: 0xc2a,
        end: 0xc39,
        flags: 0,
    },
    RangeFlags {
        start: 0xc3a,
        end: 0xc3b,
        flags: 2,
    },
    RangeFlags {
        start: 0xc3c,
        end: 0xc44,
        flags: 0,
    },
    RangeFlags {
        start: 0xc45,
        end: 0xc45,
        flags: 2,
    },
    RangeFlags {
        start: 0xc46,
        end: 0xc48,
        flags: 0,
    },
    RangeFlags {
        start: 0xc49,
        end: 0xc49,
        flags: 2,
    },
    RangeFlags {
        start: 0xc4a,
        end: 0xc4d,
        flags: 0,
    },
    RangeFlags {
        start: 0xc4e,
        end: 0xc54,
        flags: 2,
    },
    RangeFlags {
        start: 0xc55,
        end: 0xc56,
        flags: 0,
    },
    RangeFlags {
        start: 0xc57,
        end: 0xc57,
        flags: 2,
    },
    RangeFlags {
        start: 0xc58,
        end: 0xc5a,
        flags: 0,
    },
    RangeFlags {
        start: 0xc5b,
        end: 0xc5c,
        flags: 2,
    },
    RangeFlags {
        start: 0xc5d,
        end: 0xc5d,
        flags: 0,
    },
    RangeFlags {
        start: 0xc5e,
        end: 0xc5f,
        flags: 2,
    },
    RangeFlags {
        start: 0xc60,
        end: 0xc63,
        flags: 0,
    },
    RangeFlags {
        start: 0xc64,
        end: 0xc65,
        flags: 2,
    },
    RangeFlags {
        start: 0xc66,
        end: 0xc6f,
        flags: 0,
    },
    RangeFlags {
        start: 0xc70,
        end: 0xc7f,
        flags: 2,
    },
    RangeFlags {
        start: 0xc80,
        end: 0xc83,
        flags: 0,
    },
    RangeFlags {
        start: 0xc84,
        end: 0xc84,
        flags: 2,
    },
    RangeFlags {
        start: 0xc85,
        end: 0xc8c,
        flags: 0,
    },
    RangeFlags {
        start: 0xc8d,
        end: 0xc8d,
        flags: 2,
    },
    RangeFlags {
        start: 0xc8e,
        end: 0xc90,
        flags: 0,
    },
    RangeFlags {
        start: 0xc91,
        end: 0xc91,
        flags: 2,
    },
    RangeFlags {
        start: 0xc92,
        end: 0xca8,
        flags: 0,
    },
    RangeFlags {
        start: 0xca9,
        end: 0xca9,
        flags: 2,
    },
    RangeFlags {
        start: 0xcaa,
        end: 0xcb3,
        flags: 0,
    },
    RangeFlags {
        start: 0xcb4,
        end: 0xcb4,
        flags: 2,
    },
    RangeFlags {
        start: 0xcb5,
        end: 0xcb9,
        flags: 0,
    },
    RangeFlags {
        start: 0xcba,
        end: 0xcbb,
        flags: 2,
    },
    RangeFlags {
        start: 0xcbc,
        end: 0xcc4,
        flags: 0,
    },
    RangeFlags {
        start: 0xcc5,
        end: 0xcc5,
        flags: 2,
    },
    RangeFlags {
        start: 0xcc6,
        end: 0xcc8,
        flags: 0,
    },
    RangeFlags {
        start: 0xcc9,
        end: 0xcc9,
        flags: 2,
    },
    RangeFlags {
        start: 0xcca,
        end: 0xccd,
        flags: 0,
    },
    RangeFlags {
        start: 0xcce,
        end: 0xcd4,
        flags: 2,
    },
    RangeFlags {
        start: 0xcd5,
        end: 0xcd6,
        flags: 0,
    },
    RangeFlags {
        start: 0xcd7,
        end: 0xcdc,
        flags: 2,
    },
    RangeFlags {
        start: 0xcdd,
        end: 0xcde,
        flags: 0,
    },
    RangeFlags {
        start: 0xcdf,
        end: 0xcdf,
        flags: 2,
    },
    RangeFlags {
        start: 0xce0,
        end: 0xce3,
        flags: 0,
    },
    RangeFlags {
        start: 0xce4,
        end: 0xce5,
        flags: 2,
    },
    RangeFlags {
        start: 0xce6,
        end: 0xcef,
        flags: 0,
    },
    RangeFlags {
        start: 0xcf0,
        end: 0xcf0,
        flags: 2,
    },
    RangeFlags {
        start: 0xcf1,
        end: 0xcf3,
        flags: 0,
    },
    RangeFlags {
        start: 0xcf4,
        end: 0xcff,
        flags: 2,
    },
    RangeFlags {
        start: 0xd00,
        end: 0xd0c,
        flags: 0,
    },
    RangeFlags {
        start: 0xd0d,
        end: 0xd0d,
        flags: 2,
    },
    RangeFlags {
        start: 0xd0e,
        end: 0xd10,
        flags: 0,
    },
    RangeFlags {
        start: 0xd11,
        end: 0xd11,
        flags: 2,
    },
    RangeFlags {
        start: 0xd12,
        end: 0xd44,
        flags: 0,
    },
    RangeFlags {
        start: 0xd45,
        end: 0xd45,
        flags: 2,
    },
    RangeFlags {
        start: 0xd46,
        end: 0xd48,
        flags: 0,
    },
    RangeFlags {
        start: 0xd49,
        end: 0xd49,
        flags: 2,
    },
    RangeFlags {
        start: 0xd4a,
        end: 0xd4e,
        flags: 0,
    },
    RangeFlags {
        start: 0xd4f,
        end: 0xd53,
        flags: 2,
    },
    RangeFlags {
        start: 0xd54,
        end: 0xd57,
        flags: 0,
    },
    RangeFlags {
        start: 0xd58,
        end: 0xd5e,
        flags: 2,
    },
    RangeFlags {
        start: 0xd5f,
        end: 0xd63,
        flags: 0,
    },
    RangeFlags {
        start: 0xd64,
        end: 0xd65,
        flags: 2,
    },
    RangeFlags {
        start: 0xd66,
        end: 0xd6f,
        flags: 0,
    },
    RangeFlags {
        start: 0xd70,
        end: 0xd79,
        flags: 2,
    },
    RangeFlags {
        start: 0xd7a,
        end: 0xd7f,
        flags: 0,
    },
    RangeFlags {
        start: 0xd80,
        end: 0xd80,
        flags: 2,
    },
    RangeFlags {
        start: 0xd81,
        end: 0xd83,
        flags: 0,
    },
    RangeFlags {
        start: 0xd84,
        end: 0xd84,
        flags: 2,
    },
    RangeFlags {
        start: 0xd85,
        end: 0xd96,
        flags: 0,
    },
    RangeFlags {
        start: 0xd97,
        end: 0xd99,
        flags: 2,
    },
    RangeFlags {
        start: 0xd9a,
        end: 0xdb1,
        flags: 0,
    },
    RangeFlags {
        start: 0xdb2,
        end: 0xdb2,
        flags: 2,
    },
    RangeFlags {
        start: 0xdb3,
        end: 0xdbb,
        flags: 0,
    },
    RangeFlags {
        start: 0xdbc,
        end: 0xdbc,
        flags: 2,
    },
    RangeFlags {
        start: 0xdbd,
        end: 0xdbd,
        flags: 0,
    },
    RangeFlags {
        start: 0xdbe,
        end: 0xdbf,
        flags: 2,
    },
    RangeFlags {
        start: 0xdc0,
        end: 0xdc6,
        flags: 0,
    },
    RangeFlags {
        start: 0xdc7,
        end: 0xdc9,
        flags: 2,
    },
    RangeFlags {
        start: 0xdca,
        end: 0xdca,
        flags: 0,
    },
    RangeFlags {
        start: 0xdcb,
        end: 0xdce,
        flags: 2,
    },
    RangeFlags {
        start: 0xdcf,
        end: 0xdd4,
        flags: 0,
    },
    RangeFlags {
        start: 0xdd5,
        end: 0xdd5,
        flags: 2,
    },
    RangeFlags {
        start: 0xdd6,
        end: 0xdd6,
        flags: 0,
    },
    RangeFlags {
        start: 0xdd7,
        end: 0xdd7,
        flags: 2,
    },
    RangeFlags {
        start: 0xdd8,
        end: 0xddf,
        flags: 0,
    },
    RangeFlags {
        start: 0xde0,
        end: 0xde5,
        flags: 2,
    },
    RangeFlags {
        start: 0xde6,
        end: 0xdef,
        flags: 0,
    },
    RangeFlags {
        start: 0xdf0,
        end: 0xdf1,
        flags: 2,
    },
    RangeFlags {
        start: 0xdf2,
        end: 0xdf3,
        flags: 0,
    },
    RangeFlags {
        start: 0xdf4,
        end: 0xe30,
        flags: 2,
    },
    RangeFlags {
        start: 0xe31,
        end: 0xe31,
        flags: 0,
    },
    RangeFlags {
        start: 0xe32,
        end: 0xe33,
        flags: 2,
    },
    RangeFlags {
        start: 0xe34,
        end: 0xe3a,
        flags: 0,
    },
    RangeFlags {
        start: 0xe3b,
        end: 0xe46,
        flags: 2,
    },
    RangeFlags {
        start: 0xe47,
        end: 0xe4e,
        flags: 0,
    },
    RangeFlags {
        start: 0xe4f,
        end: 0xe4f,
        flags: 2,
    },
    RangeFlags {
        start: 0xe50,
        end: 0xe59,
        flags: 0,
    },
    RangeFlags {
        start: 0xe5a,
        end: 0xeb0,
        flags: 2,
    },
    RangeFlags {
        start: 0xeb1,
        end: 0xeb1,
        flags: 0,
    },
    RangeFlags {
        start: 0xeb2,
        end: 0xeb3,
        flags: 2,
    },
    RangeFlags {
        start: 0xeb4,
        end: 0xebc,
        flags: 0,
    },
    RangeFlags {
        start: 0xebd,
        end: 0xec7,
        flags: 2,
    },
    RangeFlags {
        start: 0xec8,
        end: 0xece,
        flags: 0,
    },
    RangeFlags {
        start: 0xecf,
        end: 0xecf,
        flags: 2,
    },
    RangeFlags {
        start: 0xed0,
        end: 0xed9,
        flags: 0,
    },
    RangeFlags {
        start: 0xeda,
        end: 0xeff,
        flags: 2,
    },
    RangeFlags {
        start: 0xf00,
        end: 0xf00,
        flags: 0,
    },
    RangeFlags {
        start: 0xf01,
        end: 0xf17,
        flags: 2,
    },
    RangeFlags {
        start: 0xf18,
        end: 0xf19,
        flags: 0,
    },
    RangeFlags {
        start: 0xf1a,
        end: 0xf1f,
        flags: 2,
    },
    RangeFlags {
        start: 0xf20,
        end: 0xf29,
        flags: 0,
    },
    RangeFlags {
        start: 0xf2a,
        end: 0xf34,
        flags: 2,
    },
    RangeFlags {
        start: 0xf35,
        end: 0xf35,
        flags: 0,
    },
    RangeFlags {
        start: 0xf36,
        end: 0xf36,
        flags: 2,
    },
    RangeFlags {
        start: 0xf37,
        end: 0xf37,
        flags: 0,
    },
    RangeFlags {
        start: 0xf38,
        end: 0xf38,
        flags: 2,
    },
    RangeFlags {
        start: 0xf39,
        end: 0xf39,
        flags: 0,
    },
    RangeFlags {
        start: 0xf3a,
        end: 0xf3d,
        flags: 2,
    },
    RangeFlags {
        start: 0xf3e,
        end: 0xf47,
        flags: 0,
    },
    RangeFlags {
        start: 0xf48,
        end: 0xf48,
        flags: 2,
    },
    RangeFlags {
        start: 0xf49,
        end: 0xf6c,
        flags: 0,
    },
    RangeFlags {
        start: 0xf6d,
        end: 0xf70,
        flags: 2,
    },
    RangeFlags {
        start: 0xf71,
        end: 0xf84,
        flags: 0,
    },
    RangeFlags {
        start: 0xf85,
        end: 0xf85,
        flags: 2,
    },
    RangeFlags {
        start: 0xf86,
        end: 0xf97,
        flags: 0,
    },
    RangeFlags {
        start: 0xf98,
        end: 0xf98,
        flags: 2,
    },
    RangeFlags {
        start: 0xf99,
        end: 0xfbc,
        flags: 0,
    },
    RangeFlags {
        start: 0xfbd,
        end: 0xfc5,
        flags: 2,
    },
    RangeFlags {
        start: 0xfc6,
        end: 0xfc6,
        flags: 0,
    },
    RangeFlags {
        start: 0xfc7,
        end: 0x102a,
        flags: 2,
    },
    RangeFlags {
        start: 0x102b,
        end: 0x103e,
        flags: 0,
    },
    RangeFlags {
        start: 0x103f,
        end: 0x103f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1040,
        end: 0x1049,
        flags: 0,
    },
    RangeFlags {
        start: 0x104a,
        end: 0x1055,
        flags: 2,
    },
    RangeFlags {
        start: 0x1056,
        end: 0x1059,
        flags: 0,
    },
    RangeFlags {
        start: 0x105a,
        end: 0x105d,
        flags: 2,
    },
    RangeFlags {
        start: 0x105e,
        end: 0x1060,
        flags: 0,
    },
    RangeFlags {
        start: 0x1061,
        end: 0x1061,
        flags: 2,
    },
    RangeFlags {
        start: 0x1062,
        end: 0x1064,
        flags: 0,
    },
    RangeFlags {
        start: 0x1065,
        end: 0x1066,
        flags: 2,
    },
    RangeFlags {
        start: 0x1067,
        end: 0x106d,
        flags: 0,
    },
    RangeFlags {
        start: 0x106e,
        end: 0x1070,
        flags: 2,
    },
    RangeFlags {
        start: 0x1071,
        end: 0x1074,
        flags: 0,
    },
    RangeFlags {
        start: 0x1075,
        end: 0x1081,
        flags: 2,
    },
    RangeFlags {
        start: 0x1082,
        end: 0x108d,
        flags: 0,
    },
    RangeFlags {
        start: 0x108e,
        end: 0x108e,
        flags: 2,
    },
    RangeFlags {
        start: 0x108f,
        end: 0x109d,
        flags: 0,
    },
    RangeFlags {
        start: 0x109e,
        end: 0x109f,
        flags: 2,
    },
    RangeFlags {
        start: 0x10a0,
        end: 0x10c5,
        flags: 1,
    },
    RangeFlags {
        start: 0x10c6,
        end: 0x10c6,
        flags: 2,
    },
    RangeFlags {
        start: 0x10c7,
        end: 0x10c7,
        flags: 1,
    },
    RangeFlags {
        start: 0x10c8,
        end: 0x10cc,
        flags: 2,
    },
    RangeFlags {
        start: 0x10cd,
        end: 0x10cd,
        flags: 1,
    },
    RangeFlags {
        start: 0x10ce,
        end: 0x10cf,
        flags: 2,
    },
    RangeFlags {
        start: 0x10d0,
        end: 0x10fa,
        flags: 1,
    },
    RangeFlags {
        start: 0x10fb,
        end: 0x10fb,
        flags: 2,
    },
    RangeFlags {
        start: 0x10fc,
        end: 0x10ff,
        flags: 1,
    },
    RangeFlags {
        start: 0x1100,
        end: 0x1248,
        flags: 0,
    },
    RangeFlags {
        start: 0x1249,
        end: 0x1249,
        flags: 2,
    },
    RangeFlags {
        start: 0x124a,
        end: 0x124d,
        flags: 0,
    },
    RangeFlags {
        start: 0x124e,
        end: 0x124f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1250,
        end: 0x1256,
        flags: 0,
    },
    RangeFlags {
        start: 0x1257,
        end: 0x1257,
        flags: 2,
    },
    RangeFlags {
        start: 0x1258,
        end: 0x1258,
        flags: 0,
    },
    RangeFlags {
        start: 0x1259,
        end: 0x1259,
        flags: 2,
    },
    RangeFlags {
        start: 0x125a,
        end: 0x125d,
        flags: 0,
    },
    RangeFlags {
        start: 0x125e,
        end: 0x125f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1260,
        end: 0x1288,
        flags: 0,
    },
    RangeFlags {
        start: 0x1289,
        end: 0x1289,
        flags: 2,
    },
    RangeFlags {
        start: 0x128a,
        end: 0x128d,
        flags: 0,
    },
    RangeFlags {
        start: 0x128e,
        end: 0x128f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1290,
        end: 0x12b0,
        flags: 0,
    },
    RangeFlags {
        start: 0x12b1,
        end: 0x12b1,
        flags: 2,
    },
    RangeFlags {
        start: 0x12b2,
        end: 0x12b5,
        flags: 0,
    },
    RangeFlags {
        start: 0x12b6,
        end: 0x12b7,
        flags: 2,
    },
    RangeFlags {
        start: 0x12b8,
        end: 0x12be,
        flags: 0,
    },
    RangeFlags {
        start: 0x12bf,
        end: 0x12bf,
        flags: 2,
    },
    RangeFlags {
        start: 0x12c0,
        end: 0x12c0,
        flags: 0,
    },
    RangeFlags {
        start: 0x12c1,
        end: 0x12c1,
        flags: 2,
    },
    RangeFlags {
        start: 0x12c2,
        end: 0x12c5,
        flags: 0,
    },
    RangeFlags {
        start: 0x12c6,
        end: 0x12c7,
        flags: 2,
    },
    RangeFlags {
        start: 0x12c8,
        end: 0x12d6,
        flags: 0,
    },
    RangeFlags {
        start: 0x12d7,
        end: 0x12d7,
        flags: 2,
    },
    RangeFlags {
        start: 0x12d8,
        end: 0x1310,
        flags: 0,
    },
    RangeFlags {
        start: 0x1311,
        end: 0x1311,
        flags: 2,
    },
    RangeFlags {
        start: 0x1312,
        end: 0x1315,
        flags: 0,
    },
    RangeFlags {
        start: 0x1316,
        end: 0x1317,
        flags: 2,
    },
    RangeFlags {
        start: 0x1318,
        end: 0x135a,
        flags: 0,
    },
    RangeFlags {
        start: 0x135b,
        end: 0x135c,
        flags: 2,
    },
    RangeFlags {
        start: 0x135d,
        end: 0x135f,
        flags: 0,
    },
    RangeFlags {
        start: 0x1360,
        end: 0x137f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1380,
        end: 0x138f,
        flags: 0,
    },
    RangeFlags {
        start: 0x1390,
        end: 0x139f,
        flags: 2,
    },
    RangeFlags {
        start: 0x13a0,
        end: 0x13f5,
        flags: 1,
    },
    RangeFlags {
        start: 0x13f6,
        end: 0x13f7,
        flags: 2,
    },
    RangeFlags {
        start: 0x13f8,
        end: 0x13fd,
        flags: 1,
    },
    RangeFlags {
        start: 0x13fe,
        end: 0x1400,
        flags: 2,
    },
    RangeFlags {
        start: 0x1401,
        end: 0x166c,
        flags: 0,
    },
    RangeFlags {
        start: 0x166d,
        end: 0x166e,
        flags: 2,
    },
    RangeFlags {
        start: 0x166f,
        end: 0x167f,
        flags: 0,
    },
    RangeFlags {
        start: 0x1680,
        end: 0x1680,
        flags: 2,
    },
    RangeFlags {
        start: 0x1681,
        end: 0x169a,
        flags: 0,
    },
    RangeFlags {
        start: 0x169b,
        end: 0x169f,
        flags: 2,
    },
    RangeFlags {
        start: 0x16a0,
        end: 0x16ea,
        flags: 0,
    },
    RangeFlags {
        start: 0x16eb,
        end: 0x16ed,
        flags: 2,
    },
    RangeFlags {
        start: 0x16ee,
        end: 0x16f8,
        flags: 0,
    },
    RangeFlags {
        start: 0x16f9,
        end: 0x16ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x1700,
        end: 0x1715,
        flags: 0,
    },
    RangeFlags {
        start: 0x1716,
        end: 0x171e,
        flags: 2,
    },
    RangeFlags {
        start: 0x171f,
        end: 0x1734,
        flags: 0,
    },
    RangeFlags {
        start: 0x1735,
        end: 0x173f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1740,
        end: 0x1753,
        flags: 0,
    },
    RangeFlags {
        start: 0x1754,
        end: 0x175f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1760,
        end: 0x176c,
        flags: 0,
    },
    RangeFlags {
        start: 0x176d,
        end: 0x176d,
        flags: 2,
    },
    RangeFlags {
        start: 0x176e,
        end: 0x1770,
        flags: 0,
    },
    RangeFlags {
        start: 0x1771,
        end: 0x1771,
        flags: 2,
    },
    RangeFlags {
        start: 0x1772,
        end: 0x1773,
        flags: 0,
    },
    RangeFlags {
        start: 0x1774,
        end: 0x17b3,
        flags: 2,
    },
    RangeFlags {
        start: 0x17b4,
        end: 0x17d3,
        flags: 0,
    },
    RangeFlags {
        start: 0x17d4,
        end: 0x17dc,
        flags: 2,
    },
    RangeFlags {
        start: 0x17dd,
        end: 0x17dd,
        flags: 0,
    },
    RangeFlags {
        start: 0x17de,
        end: 0x17df,
        flags: 2,
    },
    RangeFlags {
        start: 0x17e0,
        end: 0x17e9,
        flags: 0,
    },
    RangeFlags {
        start: 0x17ea,
        end: 0x180a,
        flags: 2,
    },
    RangeFlags {
        start: 0x180b,
        end: 0x1819,
        flags: 0,
    },
    RangeFlags {
        start: 0x181a,
        end: 0x181f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1820,
        end: 0x1878,
        flags: 0,
    },
    RangeFlags {
        start: 0x1879,
        end: 0x187f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1880,
        end: 0x18aa,
        flags: 0,
    },
    RangeFlags {
        start: 0x18ab,
        end: 0x18af,
        flags: 2,
    },
    RangeFlags {
        start: 0x18b0,
        end: 0x18f5,
        flags: 0,
    },
    RangeFlags {
        start: 0x18f6,
        end: 0x18ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x1900,
        end: 0x191e,
        flags: 0,
    },
    RangeFlags {
        start: 0x191f,
        end: 0x191f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1920,
        end: 0x192b,
        flags: 0,
    },
    RangeFlags {
        start: 0x192c,
        end: 0x192f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1930,
        end: 0x193b,
        flags: 0,
    },
    RangeFlags {
        start: 0x193c,
        end: 0x1945,
        flags: 2,
    },
    RangeFlags {
        start: 0x1946,
        end: 0x194f,
        flags: 0,
    },
    RangeFlags {
        start: 0x1950,
        end: 0x19cf,
        flags: 2,
    },
    RangeFlags {
        start: 0x19d0,
        end: 0x19d9,
        flags: 0,
    },
    RangeFlags {
        start: 0x19da,
        end: 0x19ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x1a00,
        end: 0x1a1b,
        flags: 0,
    },
    RangeFlags {
        start: 0x1a1c,
        end: 0x1a54,
        flags: 2,
    },
    RangeFlags {
        start: 0x1a55,
        end: 0x1a5e,
        flags: 0,
    },
    RangeFlags {
        start: 0x1a5f,
        end: 0x1a5f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1a60,
        end: 0x1a7c,
        flags: 0,
    },
    RangeFlags {
        start: 0x1a7d,
        end: 0x1a7e,
        flags: 2,
    },
    RangeFlags {
        start: 0x1a7f,
        end: 0x1a89,
        flags: 0,
    },
    RangeFlags {
        start: 0x1a8a,
        end: 0x1a8f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1a90,
        end: 0x1a99,
        flags: 0,
    },
    RangeFlags {
        start: 0x1a9a,
        end: 0x1aaf,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ab0,
        end: 0x1ace,
        flags: 0,
    },
    RangeFlags {
        start: 0x1acf,
        end: 0x1aff,
        flags: 2,
    },
    RangeFlags {
        start: 0x1b00,
        end: 0x1b4c,
        flags: 0,
    },
    RangeFlags {
        start: 0x1b4d,
        end: 0x1b4f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1b50,
        end: 0x1b59,
        flags: 0,
    },
    RangeFlags {
        start: 0x1b5a,
        end: 0x1b6a,
        flags: 2,
    },
    RangeFlags {
        start: 0x1b6b,
        end: 0x1b73,
        flags: 0,
    },
    RangeFlags {
        start: 0x1b74,
        end: 0x1b7f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1b80,
        end: 0x1bf3,
        flags: 0,
    },
    RangeFlags {
        start: 0x1bf4,
        end: 0x1bff,
        flags: 2,
    },
    RangeFlags {
        start: 0x1c00,
        end: 0x1c37,
        flags: 0,
    },
    RangeFlags {
        start: 0x1c38,
        end: 0x1c3f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1c40,
        end: 0x1c49,
        flags: 0,
    },
    RangeFlags {
        start: 0x1c4a,
        end: 0x1c4c,
        flags: 2,
    },
    RangeFlags {
        start: 0x1c4d,
        end: 0x1c7d,
        flags: 0,
    },
    RangeFlags {
        start: 0x1c7e,
        end: 0x1c7f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1c80,
        end: 0x1c88,
        flags: 1,
    },
    RangeFlags {
        start: 0x1c89,
        end: 0x1c8f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1c90,
        end: 0x1cba,
        flags: 1,
    },
    RangeFlags {
        start: 0x1cbb,
        end: 0x1cbc,
        flags: 2,
    },
    RangeFlags {
        start: 0x1cbd,
        end: 0x1cbf,
        flags: 1,
    },
    RangeFlags {
        start: 0x1cc0,
        end: 0x1ccf,
        flags: 2,
    },
    RangeFlags {
        start: 0x1cd0,
        end: 0x1cd2,
        flags: 0,
    },
    RangeFlags {
        start: 0x1cd3,
        end: 0x1cd3,
        flags: 2,
    },
    RangeFlags {
        start: 0x1cd4,
        end: 0x1cfa,
        flags: 0,
    },
    RangeFlags {
        start: 0x1cfb,
        end: 0x1cff,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d00,
        end: 0x1dbf,
        flags: 1,
    },
    RangeFlags {
        start: 0x1dc0,
        end: 0x1dff,
        flags: 0,
    },
    RangeFlags {
        start: 0x1e00,
        end: 0x1f15,
        flags: 1,
    },
    RangeFlags {
        start: 0x1f16,
        end: 0x1f17,
        flags: 2,
    },
    RangeFlags {
        start: 0x1f18,
        end: 0x1f1d,
        flags: 1,
    },
    RangeFlags {
        start: 0x1f1e,
        end: 0x1f1f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1f20,
        end: 0x1f45,
        flags: 1,
    },
    RangeFlags {
        start: 0x1f46,
        end: 0x1f47,
        flags: 2,
    },
    RangeFlags {
        start: 0x1f48,
        end: 0x1f4d,
        flags: 1,
    },
    RangeFlags {
        start: 0x1f4e,
        end: 0x1f4f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1f50,
        end: 0x1f57,
        flags: 1,
    },
    RangeFlags {
        start: 0x1f58,
        end: 0x1f58,
        flags: 2,
    },
    RangeFlags {
        start: 0x1f59,
        end: 0x1f59,
        flags: 1,
    },
    RangeFlags {
        start: 0x1f5a,
        end: 0x1f5a,
        flags: 2,
    },
    RangeFlags {
        start: 0x1f5b,
        end: 0x1f5b,
        flags: 1,
    },
    RangeFlags {
        start: 0x1f5c,
        end: 0x1f5c,
        flags: 2,
    },
    RangeFlags {
        start: 0x1f5d,
        end: 0x1f5d,
        flags: 1,
    },
    RangeFlags {
        start: 0x1f5e,
        end: 0x1f5e,
        flags: 2,
    },
    RangeFlags {
        start: 0x1f5f,
        end: 0x1f7d,
        flags: 1,
    },
    RangeFlags {
        start: 0x1f7e,
        end: 0x1f7f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1f80,
        end: 0x1fb4,
        flags: 1,
    },
    RangeFlags {
        start: 0x1fb5,
        end: 0x1fb5,
        flags: 2,
    },
    RangeFlags {
        start: 0x1fb6,
        end: 0x1fbc,
        flags: 1,
    },
    RangeFlags {
        start: 0x1fbd,
        end: 0x1fbd,
        flags: 2,
    },
    RangeFlags {
        start: 0x1fbe,
        end: 0x1fbe,
        flags: 1,
    },
    RangeFlags {
        start: 0x1fbf,
        end: 0x1fc1,
        flags: 2,
    },
    RangeFlags {
        start: 0x1fc2,
        end: 0x1fc4,
        flags: 1,
    },
    RangeFlags {
        start: 0x1fc5,
        end: 0x1fc5,
        flags: 2,
    },
    RangeFlags {
        start: 0x1fc6,
        end: 0x1fcc,
        flags: 1,
    },
    RangeFlags {
        start: 0x1fcd,
        end: 0x1fcf,
        flags: 2,
    },
    RangeFlags {
        start: 0x1fd0,
        end: 0x1fd3,
        flags: 1,
    },
    RangeFlags {
        start: 0x1fd4,
        end: 0x1fd5,
        flags: 2,
    },
    RangeFlags {
        start: 0x1fd6,
        end: 0x1fdb,
        flags: 1,
    },
    RangeFlags {
        start: 0x1fdc,
        end: 0x1fdf,
        flags: 2,
    },
    RangeFlags {
        start: 0x1fe0,
        end: 0x1fec,
        flags: 1,
    },
    RangeFlags {
        start: 0x1fed,
        end: 0x1ff1,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ff2,
        end: 0x1ff4,
        flags: 1,
    },
    RangeFlags {
        start: 0x1ff5,
        end: 0x1ff5,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ff6,
        end: 0x1ffc,
        flags: 1,
    },
    RangeFlags {
        start: 0x1ffd,
        end: 0x200b,
        flags: 2,
    },
    RangeFlags {
        start: 0x200c,
        end: 0x200f,
        flags: 0,
    },
    RangeFlags {
        start: 0x2010,
        end: 0x2017,
        flags: 2,
    },
    RangeFlags {
        start: 0x2018,
        end: 0x2019,
        flags: 4,
    },
    RangeFlags {
        start: 0x201a,
        end: 0x2023,
        flags: 2,
    },
    RangeFlags {
        start: 0x2024,
        end: 0x2024,
        flags: 4,
    },
    RangeFlags {
        start: 0x2025,
        end: 0x2026,
        flags: 2,
    },
    RangeFlags {
        start: 0x2027,
        end: 0x2027,
        flags: 4,
    },
    RangeFlags {
        start: 0x2028,
        end: 0x2029,
        flags: 2,
    },
    RangeFlags {
        start: 0x202a,
        end: 0x202f,
        flags: 0,
    },
    RangeFlags {
        start: 0x2030,
        end: 0x203e,
        flags: 2,
    },
    RangeFlags {
        start: 0x203f,
        end: 0x2040,
        flags: 0,
    },
    RangeFlags {
        start: 0x2041,
        end: 0x2053,
        flags: 2,
    },
    RangeFlags {
        start: 0x2054,
        end: 0x2054,
        flags: 0,
    },
    RangeFlags {
        start: 0x2055,
        end: 0x205f,
        flags: 2,
    },
    RangeFlags {
        start: 0x2060,
        end: 0x2064,
        flags: 0,
    },
    RangeFlags {
        start: 0x2065,
        end: 0x2065,
        flags: 2,
    },
    RangeFlags {
        start: 0x2066,
        end: 0x206f,
        flags: 0,
    },
    RangeFlags {
        start: 0x2070,
        end: 0x2070,
        flags: 2,
    },
    RangeFlags {
        start: 0x2071,
        end: 0x2071,
        flags: 1,
    },
    RangeFlags {
        start: 0x2072,
        end: 0x207e,
        flags: 2,
    },
    RangeFlags {
        start: 0x207f,
        end: 0x207f,
        flags: 1,
    },
    RangeFlags {
        start: 0x2080,
        end: 0x208f,
        flags: 2,
    },
    RangeFlags {
        start: 0x2090,
        end: 0x209c,
        flags: 1,
    },
    RangeFlags {
        start: 0x209d,
        end: 0x20cf,
        flags: 2,
    },
    RangeFlags {
        start: 0x20d0,
        end: 0x20f0,
        flags: 0,
    },
    RangeFlags {
        start: 0x20f1,
        end: 0x2101,
        flags: 2,
    },
    RangeFlags {
        start: 0x2102,
        end: 0x2102,
        flags: 1,
    },
    RangeFlags {
        start: 0x2103,
        end: 0x2106,
        flags: 2,
    },
    RangeFlags {
        start: 0x2107,
        end: 0x2107,
        flags: 1,
    },
    RangeFlags {
        start: 0x2108,
        end: 0x2109,
        flags: 2,
    },
    RangeFlags {
        start: 0x210a,
        end: 0x2113,
        flags: 1,
    },
    RangeFlags {
        start: 0x2114,
        end: 0x2114,
        flags: 2,
    },
    RangeFlags {
        start: 0x2115,
        end: 0x2115,
        flags: 1,
    },
    RangeFlags {
        start: 0x2116,
        end: 0x2118,
        flags: 2,
    },
    RangeFlags {
        start: 0x2119,
        end: 0x211d,
        flags: 1,
    },
    RangeFlags {
        start: 0x211e,
        end: 0x2123,
        flags: 2,
    },
    RangeFlags {
        start: 0x2124,
        end: 0x2124,
        flags: 1,
    },
    RangeFlags {
        start: 0x2125,
        end: 0x2125,
        flags: 2,
    },
    RangeFlags {
        start: 0x2126,
        end: 0x2126,
        flags: 1,
    },
    RangeFlags {
        start: 0x2127,
        end: 0x2127,
        flags: 2,
    },
    RangeFlags {
        start: 0x2128,
        end: 0x2128,
        flags: 1,
    },
    RangeFlags {
        start: 0x2129,
        end: 0x2129,
        flags: 2,
    },
    RangeFlags {
        start: 0x212a,
        end: 0x212d,
        flags: 1,
    },
    RangeFlags {
        start: 0x212e,
        end: 0x212e,
        flags: 2,
    },
    RangeFlags {
        start: 0x212f,
        end: 0x2134,
        flags: 1,
    },
    RangeFlags {
        start: 0x2135,
        end: 0x2138,
        flags: 0,
    },
    RangeFlags {
        start: 0x2139,
        end: 0x2139,
        flags: 1,
    },
    RangeFlags {
        start: 0x213a,
        end: 0x213b,
        flags: 2,
    },
    RangeFlags {
        start: 0x213c,
        end: 0x213f,
        flags: 1,
    },
    RangeFlags {
        start: 0x2140,
        end: 0x2144,
        flags: 2,
    },
    RangeFlags {
        start: 0x2145,
        end: 0x2149,
        flags: 1,
    },
    RangeFlags {
        start: 0x214a,
        end: 0x214d,
        flags: 2,
    },
    RangeFlags {
        start: 0x214e,
        end: 0x214e,
        flags: 1,
    },
    RangeFlags {
        start: 0x214f,
        end: 0x215f,
        flags: 2,
    },
    RangeFlags {
        start: 0x2160,
        end: 0x217f,
        flags: 1,
    },
    RangeFlags {
        start: 0x2180,
        end: 0x2182,
        flags: 0,
    },
    RangeFlags {
        start: 0x2183,
        end: 0x2184,
        flags: 1,
    },
    RangeFlags {
        start: 0x2185,
        end: 0x2188,
        flags: 0,
    },
    RangeFlags {
        start: 0x2189,
        end: 0x24b5,
        flags: 2,
    },
    RangeFlags {
        start: 0x24b6,
        end: 0x24e9,
        flags: 1,
    },
    RangeFlags {
        start: 0x24ea,
        end: 0x2bff,
        flags: 2,
    },
    RangeFlags {
        start: 0x2c00,
        end: 0x2ce4,
        flags: 1,
    },
    RangeFlags {
        start: 0x2ce5,
        end: 0x2cea,
        flags: 2,
    },
    RangeFlags {
        start: 0x2ceb,
        end: 0x2cee,
        flags: 1,
    },
    RangeFlags {
        start: 0x2cef,
        end: 0x2cf1,
        flags: 0,
    },
    RangeFlags {
        start: 0x2cf2,
        end: 0x2cf3,
        flags: 1,
    },
    RangeFlags {
        start: 0x2cf4,
        end: 0x2cff,
        flags: 2,
    },
    RangeFlags {
        start: 0x2d00,
        end: 0x2d25,
        flags: 1,
    },
    RangeFlags {
        start: 0x2d26,
        end: 0x2d26,
        flags: 2,
    },
    RangeFlags {
        start: 0x2d27,
        end: 0x2d27,
        flags: 1,
    },
    RangeFlags {
        start: 0x2d28,
        end: 0x2d2c,
        flags: 2,
    },
    RangeFlags {
        start: 0x2d2d,
        end: 0x2d2d,
        flags: 1,
    },
    RangeFlags {
        start: 0x2d2e,
        end: 0x2d2f,
        flags: 2,
    },
    RangeFlags {
        start: 0x2d30,
        end: 0x2d67,
        flags: 0,
    },
    RangeFlags {
        start: 0x2d68,
        end: 0x2d6e,
        flags: 2,
    },
    RangeFlags {
        start: 0x2d6f,
        end: 0x2d6f,
        flags: 0,
    },
    RangeFlags {
        start: 0x2d70,
        end: 0x2d7e,
        flags: 2,
    },
    RangeFlags {
        start: 0x2d7f,
        end: 0x2d96,
        flags: 0,
    },
    RangeFlags {
        start: 0x2d97,
        end: 0x2d9f,
        flags: 2,
    },
    RangeFlags {
        start: 0x2da0,
        end: 0x2da6,
        flags: 0,
    },
    RangeFlags {
        start: 0x2da7,
        end: 0x2da7,
        flags: 2,
    },
    RangeFlags {
        start: 0x2da8,
        end: 0x2dae,
        flags: 0,
    },
    RangeFlags {
        start: 0x2daf,
        end: 0x2daf,
        flags: 2,
    },
    RangeFlags {
        start: 0x2db0,
        end: 0x2db6,
        flags: 0,
    },
    RangeFlags {
        start: 0x2db7,
        end: 0x2db7,
        flags: 2,
    },
    RangeFlags {
        start: 0x2db8,
        end: 0x2dbe,
        flags: 0,
    },
    RangeFlags {
        start: 0x2dbf,
        end: 0x2dbf,
        flags: 2,
    },
    RangeFlags {
        start: 0x2dc0,
        end: 0x2dc6,
        flags: 0,
    },
    RangeFlags {
        start: 0x2dc7,
        end: 0x2dc7,
        flags: 2,
    },
    RangeFlags {
        start: 0x2dc8,
        end: 0x2dce,
        flags: 0,
    },
    RangeFlags {
        start: 0x2dcf,
        end: 0x2dcf,
        flags: 2,
    },
    RangeFlags {
        start: 0x2dd0,
        end: 0x2dd6,
        flags: 0,
    },
    RangeFlags {
        start: 0x2dd7,
        end: 0x2dd7,
        flags: 2,
    },
    RangeFlags {
        start: 0x2dd8,
        end: 0x2dde,
        flags: 0,
    },
    RangeFlags {
        start: 0x2ddf,
        end: 0x2ddf,
        flags: 2,
    },
    RangeFlags {
        start: 0x2de0,
        end: 0x2dff,
        flags: 0,
    },
    RangeFlags {
        start: 0x2e00,
        end: 0x2e2e,
        flags: 2,
    },
    RangeFlags {
        start: 0x2e2f,
        end: 0x2e2f,
        flags: 0,
    },
    RangeFlags {
        start: 0x2e30,
        end: 0x3004,
        flags: 2,
    },
    RangeFlags {
        start: 0x3005,
        end: 0x3005,
        flags: 0,
    },
    RangeFlags {
        start: 0x3006,
        end: 0x3029,
        flags: 2,
    },
    RangeFlags {
        start: 0x302a,
        end: 0x302f,
        flags: 0,
    },
    RangeFlags {
        start: 0x3030,
        end: 0x303a,
        flags: 2,
    },
    RangeFlags {
        start: 0x303b,
        end: 0x303c,
        flags: 0,
    },
    RangeFlags {
        start: 0x303d,
        end: 0x3098,
        flags: 2,
    },
    RangeFlags {
        start: 0x3099,
        end: 0x309a,
        flags: 0,
    },
    RangeFlags {
        start: 0x309b,
        end: 0x3104,
        flags: 2,
    },
    RangeFlags {
        start: 0x3105,
        end: 0x312f,
        flags: 0,
    },
    RangeFlags {
        start: 0x3130,
        end: 0x3130,
        flags: 2,
    },
    RangeFlags {
        start: 0x3131,
        end: 0x318e,
        flags: 0,
    },
    RangeFlags {
        start: 0x318f,
        end: 0x319f,
        flags: 2,
    },
    RangeFlags {
        start: 0x31a0,
        end: 0x31bf,
        flags: 0,
    },
    RangeFlags {
        start: 0x31c0,
        end: 0x9fff,
        flags: 2,
    },
    RangeFlags {
        start: 0xa000,
        end: 0xa48c,
        flags: 0,
    },
    RangeFlags {
        start: 0xa48d,
        end: 0xa4cf,
        flags: 2,
    },
    RangeFlags {
        start: 0xa4d0,
        end: 0xa4fd,
        flags: 0,
    },
    RangeFlags {
        start: 0xa4fe,
        end: 0xa4ff,
        flags: 2,
    },
    RangeFlags {
        start: 0xa500,
        end: 0xa60c,
        flags: 0,
    },
    RangeFlags {
        start: 0xa60d,
        end: 0xa60f,
        flags: 2,
    },
    RangeFlags {
        start: 0xa610,
        end: 0xa62b,
        flags: 0,
    },
    RangeFlags {
        start: 0xa62c,
        end: 0xa63f,
        flags: 2,
    },
    RangeFlags {
        start: 0xa640,
        end: 0xa66d,
        flags: 1,
    },
    RangeFlags {
        start: 0xa66e,
        end: 0xa672,
        flags: 0,
    },
    RangeFlags {
        start: 0xa673,
        end: 0xa673,
        flags: 2,
    },
    RangeFlags {
        start: 0xa674,
        end: 0xa67d,
        flags: 0,
    },
    RangeFlags {
        start: 0xa67e,
        end: 0xa67e,
        flags: 2,
    },
    RangeFlags {
        start: 0xa67f,
        end: 0xa67f,
        flags: 0,
    },
    RangeFlags {
        start: 0xa680,
        end: 0xa69d,
        flags: 1,
    },
    RangeFlags {
        start: 0xa69e,
        end: 0xa6f1,
        flags: 0,
    },
    RangeFlags {
        start: 0xa6f2,
        end: 0xa707,
        flags: 2,
    },
    RangeFlags {
        start: 0xa708,
        end: 0xa721,
        flags: 0,
    },
    RangeFlags {
        start: 0xa722,
        end: 0xa787,
        flags: 1,
    },
    RangeFlags {
        start: 0xa788,
        end: 0xa78a,
        flags: 0,
    },
    RangeFlags {
        start: 0xa78b,
        end: 0xa78e,
        flags: 1,
    },
    RangeFlags {
        start: 0xa78f,
        end: 0xa78f,
        flags: 0,
    },
    RangeFlags {
        start: 0xa790,
        end: 0xa7ca,
        flags: 1,
    },
    RangeFlags {
        start: 0xa7cb,
        end: 0xa7cf,
        flags: 2,
    },
    RangeFlags {
        start: 0xa7d0,
        end: 0xa7d1,
        flags: 1,
    },
    RangeFlags {
        start: 0xa7d2,
        end: 0xa7d2,
        flags: 2,
    },
    RangeFlags {
        start: 0xa7d3,
        end: 0xa7d3,
        flags: 1,
    },
    RangeFlags {
        start: 0xa7d4,
        end: 0xa7d4,
        flags: 2,
    },
    RangeFlags {
        start: 0xa7d5,
        end: 0xa7d9,
        flags: 1,
    },
    RangeFlags {
        start: 0xa7da,
        end: 0xa7f1,
        flags: 2,
    },
    RangeFlags {
        start: 0xa7f2,
        end: 0xa7f6,
        flags: 1,
    },
    RangeFlags {
        start: 0xa7f7,
        end: 0xa7f7,
        flags: 0,
    },
    RangeFlags {
        start: 0xa7f8,
        end: 0xa7fa,
        flags: 1,
    },
    RangeFlags {
        start: 0xa7fb,
        end: 0xa827,
        flags: 0,
    },
    RangeFlags {
        start: 0xa828,
        end: 0xa82b,
        flags: 2,
    },
    RangeFlags {
        start: 0xa82c,
        end: 0xa82c,
        flags: 0,
    },
    RangeFlags {
        start: 0xa82d,
        end: 0xa83f,
        flags: 2,
    },
    RangeFlags {
        start: 0xa840,
        end: 0xa873,
        flags: 0,
    },
    RangeFlags {
        start: 0xa874,
        end: 0xa87f,
        flags: 2,
    },
    RangeFlags {
        start: 0xa880,
        end: 0xa8c5,
        flags: 0,
    },
    RangeFlags {
        start: 0xa8c6,
        end: 0xa8cf,
        flags: 2,
    },
    RangeFlags {
        start: 0xa8d0,
        end: 0xa8d9,
        flags: 0,
    },
    RangeFlags {
        start: 0xa8da,
        end: 0xa8df,
        flags: 2,
    },
    RangeFlags {
        start: 0xa8e0,
        end: 0xa8f7,
        flags: 0,
    },
    RangeFlags {
        start: 0xa8f8,
        end: 0xa8fa,
        flags: 2,
    },
    RangeFlags {
        start: 0xa8fb,
        end: 0xa8fb,
        flags: 0,
    },
    RangeFlags {
        start: 0xa8fc,
        end: 0xa8fc,
        flags: 2,
    },
    RangeFlags {
        start: 0xa8fd,
        end: 0xa92d,
        flags: 0,
    },
    RangeFlags {
        start: 0xa92e,
        end: 0xa92f,
        flags: 2,
    },
    RangeFlags {
        start: 0xa930,
        end: 0xa953,
        flags: 0,
    },
    RangeFlags {
        start: 0xa954,
        end: 0xa95f,
        flags: 2,
    },
    RangeFlags {
        start: 0xa960,
        end: 0xa97c,
        flags: 0,
    },
    RangeFlags {
        start: 0xa97d,
        end: 0xa97f,
        flags: 2,
    },
    RangeFlags {
        start: 0xa980,
        end: 0xa9c0,
        flags: 0,
    },
    RangeFlags {
        start: 0xa9c1,
        end: 0xa9ce,
        flags: 2,
    },
    RangeFlags {
        start: 0xa9cf,
        end: 0xa9d9,
        flags: 0,
    },
    RangeFlags {
        start: 0xa9da,
        end: 0xa9e4,
        flags: 2,
    },
    RangeFlags {
        start: 0xa9e5,
        end: 0xa9e5,
        flags: 0,
    },
    RangeFlags {
        start: 0xa9e6,
        end: 0xa9ef,
        flags: 2,
    },
    RangeFlags {
        start: 0xa9f0,
        end: 0xa9f9,
        flags: 0,
    },
    RangeFlags {
        start: 0xa9fa,
        end: 0xa9ff,
        flags: 2,
    },
    RangeFlags {
        start: 0xaa00,
        end: 0xaa36,
        flags: 0,
    },
    RangeFlags {
        start: 0xaa37,
        end: 0xaa3f,
        flags: 2,
    },
    RangeFlags {
        start: 0xaa40,
        end: 0xaa4d,
        flags: 0,
    },
    RangeFlags {
        start: 0xaa4e,
        end: 0xaa4f,
        flags: 2,
    },
    RangeFlags {
        start: 0xaa50,
        end: 0xaa59,
        flags: 0,
    },
    RangeFlags {
        start: 0xaa5a,
        end: 0xaa7a,
        flags: 2,
    },
    RangeFlags {
        start: 0xaa7b,
        end: 0xaa7d,
        flags: 0,
    },
    RangeFlags {
        start: 0xaa7e,
        end: 0xaaaf,
        flags: 2,
    },
    RangeFlags {
        start: 0xaab0,
        end: 0xaab0,
        flags: 0,
    },
    RangeFlags {
        start: 0xaab1,
        end: 0xaab1,
        flags: 2,
    },
    RangeFlags {
        start: 0xaab2,
        end: 0xaab4,
        flags: 0,
    },
    RangeFlags {
        start: 0xaab5,
        end: 0xaab6,
        flags: 2,
    },
    RangeFlags {
        start: 0xaab7,
        end: 0xaab8,
        flags: 0,
    },
    RangeFlags {
        start: 0xaab9,
        end: 0xaabd,
        flags: 2,
    },
    RangeFlags {
        start: 0xaabe,
        end: 0xaabf,
        flags: 0,
    },
    RangeFlags {
        start: 0xaac0,
        end: 0xaac0,
        flags: 2,
    },
    RangeFlags {
        start: 0xaac1,
        end: 0xaac1,
        flags: 0,
    },
    RangeFlags {
        start: 0xaac2,
        end: 0xaadf,
        flags: 2,
    },
    RangeFlags {
        start: 0xaae0,
        end: 0xaaef,
        flags: 0,
    },
    RangeFlags {
        start: 0xaaf0,
        end: 0xaaf1,
        flags: 2,
    },
    RangeFlags {
        start: 0xaaf2,
        end: 0xaaf6,
        flags: 0,
    },
    RangeFlags {
        start: 0xaaf7,
        end: 0xab00,
        flags: 2,
    },
    RangeFlags {
        start: 0xab01,
        end: 0xab06,
        flags: 0,
    },
    RangeFlags {
        start: 0xab07,
        end: 0xab08,
        flags: 2,
    },
    RangeFlags {
        start: 0xab09,
        end: 0xab0e,
        flags: 0,
    },
    RangeFlags {
        start: 0xab0f,
        end: 0xab10,
        flags: 2,
    },
    RangeFlags {
        start: 0xab11,
        end: 0xab16,
        flags: 0,
    },
    RangeFlags {
        start: 0xab17,
        end: 0xab1f,
        flags: 2,
    },
    RangeFlags {
        start: 0xab20,
        end: 0xab26,
        flags: 0,
    },
    RangeFlags {
        start: 0xab27,
        end: 0xab27,
        flags: 2,
    },
    RangeFlags {
        start: 0xab28,
        end: 0xab2e,
        flags: 0,
    },
    RangeFlags {
        start: 0xab2f,
        end: 0xab2f,
        flags: 2,
    },
    RangeFlags {
        start: 0xab30,
        end: 0xab5a,
        flags: 1,
    },
    RangeFlags {
        start: 0xab5b,
        end: 0xab5b,
        flags: 0,
    },
    RangeFlags {
        start: 0xab5c,
        end: 0xab69,
        flags: 1,
    },
    RangeFlags {
        start: 0xab6a,
        end: 0xab6f,
        flags: 2,
    },
    RangeFlags {
        start: 0xab70,
        end: 0xabbf,
        flags: 1,
    },
    RangeFlags {
        start: 0xabc0,
        end: 0xabea,
        flags: 0,
    },
    RangeFlags {
        start: 0xabeb,
        end: 0xabeb,
        flags: 2,
    },
    RangeFlags {
        start: 0xabec,
        end: 0xabed,
        flags: 0,
    },
    RangeFlags {
        start: 0xabee,
        end: 0xabef,
        flags: 2,
    },
    RangeFlags {
        start: 0xabf0,
        end: 0xabf9,
        flags: 0,
    },
    RangeFlags {
        start: 0xabfa,
        end: 0xabff,
        flags: 2,
    },
    RangeFlags {
        start: 0xac00,
        end: 0xd7a3,
        flags: 0,
    },
    RangeFlags {
        start: 0xd7a4,
        end: 0xd7af,
        flags: 2,
    },
    RangeFlags {
        start: 0xd7b0,
        end: 0xd7c6,
        flags: 0,
    },
    RangeFlags {
        start: 0xd7c7,
        end: 0xd7ca,
        flags: 2,
    },
    RangeFlags {
        start: 0xd7cb,
        end: 0xd7fb,
        flags: 0,
    },
    RangeFlags {
        start: 0xd7fc,
        end: 0xfaff,
        flags: 2,
    },
    RangeFlags {
        start: 0xfb00,
        end: 0xfb06,
        flags: 1,
    },
    RangeFlags {
        start: 0xfb07,
        end: 0xfb12,
        flags: 2,
    },
    RangeFlags {
        start: 0xfb13,
        end: 0xfb17,
        flags: 1,
    },
    RangeFlags {
        start: 0xfb18,
        end: 0xfb1c,
        flags: 2,
    },
    RangeFlags {
        start: 0xfb1d,
        end: 0xfb28,
        flags: 0,
    },
    RangeFlags {
        start: 0xfb29,
        end: 0xfb29,
        flags: 2,
    },
    RangeFlags {
        start: 0xfb2a,
        end: 0xfb36,
        flags: 0,
    },
    RangeFlags {
        start: 0xfb37,
        end: 0xfb37,
        flags: 2,
    },
    RangeFlags {
        start: 0xfb38,
        end: 0xfb3c,
        flags: 0,
    },
    RangeFlags {
        start: 0xfb3d,
        end: 0xfb3d,
        flags: 2,
    },
    RangeFlags {
        start: 0xfb3e,
        end: 0xfb3e,
        flags: 0,
    },
    RangeFlags {
        start: 0xfb3f,
        end: 0xfb3f,
        flags: 2,
    },
    RangeFlags {
        start: 0xfb40,
        end: 0xfb41,
        flags: 0,
    },
    RangeFlags {
        start: 0xfb42,
        end: 0xfb42,
        flags: 2,
    },
    RangeFlags {
        start: 0xfb43,
        end: 0xfb44,
        flags: 0,
    },
    RangeFlags {
        start: 0xfb45,
        end: 0xfb45,
        flags: 2,
    },
    RangeFlags {
        start: 0xfb46,
        end: 0xfbb1,
        flags: 0,
    },
    RangeFlags {
        start: 0xfbb2,
        end: 0xfbd2,
        flags: 2,
    },
    RangeFlags {
        start: 0xfbd3,
        end: 0xfd3d,
        flags: 0,
    },
    RangeFlags {
        start: 0xfd3e,
        end: 0xfd4f,
        flags: 2,
    },
    RangeFlags {
        start: 0xfd50,
        end: 0xfd8f,
        flags: 0,
    },
    RangeFlags {
        start: 0xfd90,
        end: 0xfd91,
        flags: 2,
    },
    RangeFlags {
        start: 0xfd92,
        end: 0xfdc7,
        flags: 0,
    },
    RangeFlags {
        start: 0xfdc8,
        end: 0xfdef,
        flags: 2,
    },
    RangeFlags {
        start: 0xfdf0,
        end: 0xfdfb,
        flags: 0,
    },
    RangeFlags {
        start: 0xfdfc,
        end: 0xfdff,
        flags: 2,
    },
    RangeFlags {
        start: 0xfe00,
        end: 0xfe0f,
        flags: 0,
    },
    RangeFlags {
        start: 0xfe10,
        end: 0xfe12,
        flags: 2,
    },
    RangeFlags {
        start: 0xfe13,
        end: 0xfe13,
        flags: 4,
    },
    RangeFlags {
        start: 0xfe14,
        end: 0xfe1f,
        flags: 2,
    },
    RangeFlags {
        start: 0xfe20,
        end: 0xfe2f,
        flags: 0,
    },
    RangeFlags {
        start: 0xfe30,
        end: 0xfe32,
        flags: 2,
    },
    RangeFlags {
        start: 0xfe33,
        end: 0xfe34,
        flags: 0,
    },
    RangeFlags {
        start: 0xfe35,
        end: 0xfe4c,
        flags: 2,
    },
    RangeFlags {
        start: 0xfe4d,
        end: 0xfe4f,
        flags: 0,
    },
    RangeFlags {
        start: 0xfe50,
        end: 0xfe51,
        flags: 2,
    },
    RangeFlags {
        start: 0xfe52,
        end: 0xfe52,
        flags: 4,
    },
    RangeFlags {
        start: 0xfe53,
        end: 0xfe54,
        flags: 2,
    },
    RangeFlags {
        start: 0xfe55,
        end: 0xfe55,
        flags: 4,
    },
    RangeFlags {
        start: 0xfe56,
        end: 0xfe6f,
        flags: 2,
    },
    RangeFlags {
        start: 0xfe70,
        end: 0xfe74,
        flags: 0,
    },
    RangeFlags {
        start: 0xfe75,
        end: 0xfe75,
        flags: 2,
    },
    RangeFlags {
        start: 0xfe76,
        end: 0xfefc,
        flags: 0,
    },
    RangeFlags {
        start: 0xfefd,
        end: 0xfefe,
        flags: 2,
    },
    RangeFlags {
        start: 0xfeff,
        end: 0xfeff,
        flags: 0,
    },
    RangeFlags {
        start: 0xff00,
        end: 0xff06,
        flags: 2,
    },
    RangeFlags {
        start: 0xff07,
        end: 0xff07,
        flags: 4,
    },
    RangeFlags {
        start: 0xff08,
        end: 0xff0d,
        flags: 2,
    },
    RangeFlags {
        start: 0xff0e,
        end: 0xff0e,
        flags: 4,
    },
    RangeFlags {
        start: 0xff0f,
        end: 0xff0f,
        flags: 2,
    },
    RangeFlags {
        start: 0xff10,
        end: 0xff19,
        flags: 0,
    },
    RangeFlags {
        start: 0xff1a,
        end: 0xff1a,
        flags: 4,
    },
    RangeFlags {
        start: 0xff1b,
        end: 0xff20,
        flags: 2,
    },
    RangeFlags {
        start: 0xff21,
        end: 0xff3a,
        flags: 1,
    },
    RangeFlags {
        start: 0xff3b,
        end: 0xff3e,
        flags: 2,
    },
    RangeFlags {
        start: 0xff3f,
        end: 0xff3f,
        flags: 0,
    },
    RangeFlags {
        start: 0xff40,
        end: 0xff40,
        flags: 2,
    },
    RangeFlags {
        start: 0xff41,
        end: 0xff5a,
        flags: 1,
    },
    RangeFlags {
        start: 0xff5b,
        end: 0xff9d,
        flags: 2,
    },
    RangeFlags {
        start: 0xff9e,
        end: 0xffbe,
        flags: 0,
    },
    RangeFlags {
        start: 0xffbf,
        end: 0xffc1,
        flags: 2,
    },
    RangeFlags {
        start: 0xffc2,
        end: 0xffc7,
        flags: 0,
    },
    RangeFlags {
        start: 0xffc8,
        end: 0xffc9,
        flags: 2,
    },
    RangeFlags {
        start: 0xffca,
        end: 0xffcf,
        flags: 0,
    },
    RangeFlags {
        start: 0xffd0,
        end: 0xffd1,
        flags: 2,
    },
    RangeFlags {
        start: 0xffd2,
        end: 0xffd7,
        flags: 0,
    },
    RangeFlags {
        start: 0xffd8,
        end: 0xffd9,
        flags: 2,
    },
    RangeFlags {
        start: 0xffda,
        end: 0xffdc,
        flags: 0,
    },
    RangeFlags {
        start: 0xffdd,
        end: 0xfff8,
        flags: 2,
    },
    RangeFlags {
        start: 0xfff9,
        end: 0xfffb,
        flags: 0,
    },
    RangeFlags {
        start: 0xfffc,
        end: 0xffff,
        flags: 2,
    },
    RangeFlags {
        start: 0x10000,
        end: 0x1000b,
        flags: 0,
    },
    RangeFlags {
        start: 0x1000c,
        end: 0x1000c,
        flags: 2,
    },
    RangeFlags {
        start: 0x1000d,
        end: 0x10026,
        flags: 0,
    },
    RangeFlags {
        start: 0x10027,
        end: 0x10027,
        flags: 2,
    },
    RangeFlags {
        start: 0x10028,
        end: 0x1003a,
        flags: 0,
    },
    RangeFlags {
        start: 0x1003b,
        end: 0x1003b,
        flags: 2,
    },
    RangeFlags {
        start: 0x1003c,
        end: 0x1003d,
        flags: 0,
    },
    RangeFlags {
        start: 0x1003e,
        end: 0x1003e,
        flags: 2,
    },
    RangeFlags {
        start: 0x1003f,
        end: 0x1004d,
        flags: 0,
    },
    RangeFlags {
        start: 0x1004e,
        end: 0x1004f,
        flags: 2,
    },
    RangeFlags {
        start: 0x10050,
        end: 0x1005d,
        flags: 0,
    },
    RangeFlags {
        start: 0x1005e,
        end: 0x1007f,
        flags: 2,
    },
    RangeFlags {
        start: 0x10080,
        end: 0x100fa,
        flags: 0,
    },
    RangeFlags {
        start: 0x100fb,
        end: 0x1013f,
        flags: 2,
    },
    RangeFlags {
        start: 0x10140,
        end: 0x10174,
        flags: 0,
    },
    RangeFlags {
        start: 0x10175,
        end: 0x101fc,
        flags: 2,
    },
    RangeFlags {
        start: 0x101fd,
        end: 0x101fd,
        flags: 0,
    },
    RangeFlags {
        start: 0x101fe,
        end: 0x1027f,
        flags: 2,
    },
    RangeFlags {
        start: 0x10280,
        end: 0x1029c,
        flags: 0,
    },
    RangeFlags {
        start: 0x1029d,
        end: 0x1029f,
        flags: 2,
    },
    RangeFlags {
        start: 0x102a0,
        end: 0x102d0,
        flags: 0,
    },
    RangeFlags {
        start: 0x102d1,
        end: 0x102df,
        flags: 2,
    },
    RangeFlags {
        start: 0x102e0,
        end: 0x102e0,
        flags: 0,
    },
    RangeFlags {
        start: 0x102e1,
        end: 0x102ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x10300,
        end: 0x1031f,
        flags: 0,
    },
    RangeFlags {
        start: 0x10320,
        end: 0x1032c,
        flags: 2,
    },
    RangeFlags {
        start: 0x1032d,
        end: 0x1034a,
        flags: 0,
    },
    RangeFlags {
        start: 0x1034b,
        end: 0x1034f,
        flags: 2,
    },
    RangeFlags {
        start: 0x10350,
        end: 0x1037a,
        flags: 0,
    },
    RangeFlags {
        start: 0x1037b,
        end: 0x1037f,
        flags: 2,
    },
    RangeFlags {
        start: 0x10380,
        end: 0x1039d,
        flags: 0,
    },
    RangeFlags {
        start: 0x1039e,
        end: 0x1039f,
        flags: 2,
    },
    RangeFlags {
        start: 0x103a0,
        end: 0x103c3,
        flags: 0,
    },
    RangeFlags {
        start: 0x103c4,
        end: 0x103c7,
        flags: 2,
    },
    RangeFlags {
        start: 0x103c8,
        end: 0x103cf,
        flags: 0,
    },
    RangeFlags {
        start: 0x103d0,
        end: 0x103d0,
        flags: 2,
    },
    RangeFlags {
        start: 0x103d1,
        end: 0x103d5,
        flags: 0,
    },
    RangeFlags {
        start: 0x103d6,
        end: 0x103ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x10400,
        end: 0x1044f,
        flags: 1,
    },
    RangeFlags {
        start: 0x10450,
        end: 0x1049d,
        flags: 0,
    },
    RangeFlags {
        start: 0x1049e,
        end: 0x1049f,
        flags: 2,
    },
    RangeFlags {
        start: 0x104a0,
        end: 0x104a9,
        flags: 0,
    },
    RangeFlags {
        start: 0x104aa,
        end: 0x104af,
        flags: 2,
    },
    RangeFlags {
        start: 0x104b0,
        end: 0x104d3,
        flags: 1,
    },
    RangeFlags {
        start: 0x104d4,
        end: 0x104d7,
        flags: 2,
    },
    RangeFlags {
        start: 0x104d8,
        end: 0x104fb,
        flags: 1,
    },
    RangeFlags {
        start: 0x104fc,
        end: 0x104ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x10500,
        end: 0x10527,
        flags: 0,
    },
    RangeFlags {
        start: 0x10528,
        end: 0x1052f,
        flags: 2,
    },
    RangeFlags {
        start: 0x10530,
        end: 0x10563,
        flags: 0,
    },
    RangeFlags {
        start: 0x10564,
        end: 0x1056f,
        flags: 2,
    },
    RangeFlags {
        start: 0x10570,
        end: 0x1057a,
        flags: 1,
    },
    RangeFlags {
        start: 0x1057b,
        end: 0x1057b,
        flags: 2,
    },
    RangeFlags {
        start: 0x1057c,
        end: 0x1058a,
        flags: 1,
    },
    RangeFlags {
        start: 0x1058b,
        end: 0x1058b,
        flags: 2,
    },
    RangeFlags {
        start: 0x1058c,
        end: 0x10592,
        flags: 1,
    },
    RangeFlags {
        start: 0x10593,
        end: 0x10593,
        flags: 2,
    },
    RangeFlags {
        start: 0x10594,
        end: 0x10595,
        flags: 1,
    },
    RangeFlags {
        start: 0x10596,
        end: 0x10596,
        flags: 2,
    },
    RangeFlags {
        start: 0x10597,
        end: 0x105a1,
        flags: 1,
    },
    RangeFlags {
        start: 0x105a2,
        end: 0x105a2,
        flags: 2,
    },
    RangeFlags {
        start: 0x105a3,
        end: 0x105b1,
        flags: 1,
    },
    RangeFlags {
        start: 0x105b2,
        end: 0x105b2,
        flags: 2,
    },
    RangeFlags {
        start: 0x105b3,
        end: 0x105b9,
        flags: 1,
    },
    RangeFlags {
        start: 0x105ba,
        end: 0x105ba,
        flags: 2,
    },
    RangeFlags {
        start: 0x105bb,
        end: 0x105bc,
        flags: 1,
    },
    RangeFlags {
        start: 0x105bd,
        end: 0x105ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x10600,
        end: 0x10736,
        flags: 0,
    },
    RangeFlags {
        start: 0x10737,
        end: 0x1073f,
        flags: 2,
    },
    RangeFlags {
        start: 0x10740,
        end: 0x10755,
        flags: 0,
    },
    RangeFlags {
        start: 0x10756,
        end: 0x1075f,
        flags: 2,
    },
    RangeFlags {
        start: 0x10760,
        end: 0x10767,
        flags: 0,
    },
    RangeFlags {
        start: 0x10768,
        end: 0x1077f,
        flags: 2,
    },
    RangeFlags {
        start: 0x10780,
        end: 0x10780,
        flags: 1,
    },
    RangeFlags {
        start: 0x10781,
        end: 0x10782,
        flags: 0,
    },
    RangeFlags {
        start: 0x10783,
        end: 0x10785,
        flags: 1,
    },
    RangeFlags {
        start: 0x10786,
        end: 0x10786,
        flags: 2,
    },
    RangeFlags {
        start: 0x10787,
        end: 0x107b0,
        flags: 1,
    },
    RangeFlags {
        start: 0x107b1,
        end: 0x107b1,
        flags: 2,
    },
    RangeFlags {
        start: 0x107b2,
        end: 0x107ba,
        flags: 1,
    },
    RangeFlags {
        start: 0x107bb,
        end: 0x107ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x10800,
        end: 0x10805,
        flags: 0,
    },
    RangeFlags {
        start: 0x10806,
        end: 0x10807,
        flags: 2,
    },
    RangeFlags {
        start: 0x10808,
        end: 0x10808,
        flags: 0,
    },
    RangeFlags {
        start: 0x10809,
        end: 0x10809,
        flags: 2,
    },
    RangeFlags {
        start: 0x1080a,
        end: 0x10835,
        flags: 0,
    },
    RangeFlags {
        start: 0x10836,
        end: 0x10836,
        flags: 2,
    },
    RangeFlags {
        start: 0x10837,
        end: 0x10838,
        flags: 0,
    },
    RangeFlags {
        start: 0x10839,
        end: 0x1083b,
        flags: 2,
    },
    RangeFlags {
        start: 0x1083c,
        end: 0x1083c,
        flags: 0,
    },
    RangeFlags {
        start: 0x1083d,
        end: 0x1083e,
        flags: 2,
    },
    RangeFlags {
        start: 0x1083f,
        end: 0x10855,
        flags: 0,
    },
    RangeFlags {
        start: 0x10856,
        end: 0x1085f,
        flags: 2,
    },
    RangeFlags {
        start: 0x10860,
        end: 0x10876,
        flags: 0,
    },
    RangeFlags {
        start: 0x10877,
        end: 0x1087f,
        flags: 2,
    },
    RangeFlags {
        start: 0x10880,
        end: 0x1089e,
        flags: 0,
    },
    RangeFlags {
        start: 0x1089f,
        end: 0x108df,
        flags: 2,
    },
    RangeFlags {
        start: 0x108e0,
        end: 0x108f2,
        flags: 0,
    },
    RangeFlags {
        start: 0x108f3,
        end: 0x108f3,
        flags: 2,
    },
    RangeFlags {
        start: 0x108f4,
        end: 0x108f5,
        flags: 0,
    },
    RangeFlags {
        start: 0x108f6,
        end: 0x108ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x10900,
        end: 0x10915,
        flags: 0,
    },
    RangeFlags {
        start: 0x10916,
        end: 0x1091f,
        flags: 2,
    },
    RangeFlags {
        start: 0x10920,
        end: 0x10939,
        flags: 0,
    },
    RangeFlags {
        start: 0x1093a,
        end: 0x1097f,
        flags: 2,
    },
    RangeFlags {
        start: 0x10980,
        end: 0x109b7,
        flags: 0,
    },
    RangeFlags {
        start: 0x109b8,
        end: 0x109bd,
        flags: 2,
    },
    RangeFlags {
        start: 0x109be,
        end: 0x109bf,
        flags: 0,
    },
    RangeFlags {
        start: 0x109c0,
        end: 0x109ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x10a00,
        end: 0x10a03,
        flags: 0,
    },
    RangeFlags {
        start: 0x10a04,
        end: 0x10a04,
        flags: 2,
    },
    RangeFlags {
        start: 0x10a05,
        end: 0x10a06,
        flags: 0,
    },
    RangeFlags {
        start: 0x10a07,
        end: 0x10a0b,
        flags: 2,
    },
    RangeFlags {
        start: 0x10a0c,
        end: 0x10a13,
        flags: 0,
    },
    RangeFlags {
        start: 0x10a14,
        end: 0x10a14,
        flags: 2,
    },
    RangeFlags {
        start: 0x10a15,
        end: 0x10a17,
        flags: 0,
    },
    RangeFlags {
        start: 0x10a18,
        end: 0x10a18,
        flags: 2,
    },
    RangeFlags {
        start: 0x10a19,
        end: 0x10a35,
        flags: 0,
    },
    RangeFlags {
        start: 0x10a36,
        end: 0x10a37,
        flags: 2,
    },
    RangeFlags {
        start: 0x10a38,
        end: 0x10a3a,
        flags: 0,
    },
    RangeFlags {
        start: 0x10a3b,
        end: 0x10a3e,
        flags: 2,
    },
    RangeFlags {
        start: 0x10a3f,
        end: 0x10a3f,
        flags: 0,
    },
    RangeFlags {
        start: 0x10a40,
        end: 0x10a5f,
        flags: 2,
    },
    RangeFlags {
        start: 0x10a60,
        end: 0x10a7c,
        flags: 0,
    },
    RangeFlags {
        start: 0x10a7d,
        end: 0x10a7f,
        flags: 2,
    },
    RangeFlags {
        start: 0x10a80,
        end: 0x10a9c,
        flags: 0,
    },
    RangeFlags {
        start: 0x10a9d,
        end: 0x10abf,
        flags: 2,
    },
    RangeFlags {
        start: 0x10ac0,
        end: 0x10ac7,
        flags: 0,
    },
    RangeFlags {
        start: 0x10ac8,
        end: 0x10ac8,
        flags: 2,
    },
    RangeFlags {
        start: 0x10ac9,
        end: 0x10ae6,
        flags: 0,
    },
    RangeFlags {
        start: 0x10ae7,
        end: 0x10aff,
        flags: 2,
    },
    RangeFlags {
        start: 0x10b00,
        end: 0x10b35,
        flags: 0,
    },
    RangeFlags {
        start: 0x10b36,
        end: 0x10b3f,
        flags: 2,
    },
    RangeFlags {
        start: 0x10b40,
        end: 0x10b55,
        flags: 0,
    },
    RangeFlags {
        start: 0x10b56,
        end: 0x10b5f,
        flags: 2,
    },
    RangeFlags {
        start: 0x10b60,
        end: 0x10b72,
        flags: 0,
    },
    RangeFlags {
        start: 0x10b73,
        end: 0x10b7f,
        flags: 2,
    },
    RangeFlags {
        start: 0x10b80,
        end: 0x10b91,
        flags: 0,
    },
    RangeFlags {
        start: 0x10b92,
        end: 0x10bff,
        flags: 2,
    },
    RangeFlags {
        start: 0x10c00,
        end: 0x10c48,
        flags: 0,
    },
    RangeFlags {
        start: 0x10c49,
        end: 0x10c7f,
        flags: 2,
    },
    RangeFlags {
        start: 0x10c80,
        end: 0x10cb2,
        flags: 1,
    },
    RangeFlags {
        start: 0x10cb3,
        end: 0x10cbf,
        flags: 2,
    },
    RangeFlags {
        start: 0x10cc0,
        end: 0x10cf2,
        flags: 1,
    },
    RangeFlags {
        start: 0x10cf3,
        end: 0x10cff,
        flags: 2,
    },
    RangeFlags {
        start: 0x10d00,
        end: 0x10d27,
        flags: 0,
    },
    RangeFlags {
        start: 0x10d28,
        end: 0x10d2f,
        flags: 2,
    },
    RangeFlags {
        start: 0x10d30,
        end: 0x10d39,
        flags: 0,
    },
    RangeFlags {
        start: 0x10d3a,
        end: 0x10e7f,
        flags: 2,
    },
    RangeFlags {
        start: 0x10e80,
        end: 0x10ea9,
        flags: 0,
    },
    RangeFlags {
        start: 0x10eaa,
        end: 0x10eaa,
        flags: 2,
    },
    RangeFlags {
        start: 0x10eab,
        end: 0x10eac,
        flags: 0,
    },
    RangeFlags {
        start: 0x10ead,
        end: 0x10eaf,
        flags: 2,
    },
    RangeFlags {
        start: 0x10eb0,
        end: 0x10eb1,
        flags: 0,
    },
    RangeFlags {
        start: 0x10eb2,
        end: 0x10efc,
        flags: 2,
    },
    RangeFlags {
        start: 0x10efd,
        end: 0x10f1c,
        flags: 0,
    },
    RangeFlags {
        start: 0x10f1d,
        end: 0x10f26,
        flags: 2,
    },
    RangeFlags {
        start: 0x10f27,
        end: 0x10f27,
        flags: 0,
    },
    RangeFlags {
        start: 0x10f28,
        end: 0x10f2f,
        flags: 2,
    },
    RangeFlags {
        start: 0x10f30,
        end: 0x10f50,
        flags: 0,
    },
    RangeFlags {
        start: 0x10f51,
        end: 0x10f6f,
        flags: 2,
    },
    RangeFlags {
        start: 0x10f70,
        end: 0x10f85,
        flags: 0,
    },
    RangeFlags {
        start: 0x10f86,
        end: 0x10faf,
        flags: 2,
    },
    RangeFlags {
        start: 0x10fb0,
        end: 0x10fc4,
        flags: 0,
    },
    RangeFlags {
        start: 0x10fc5,
        end: 0x10fdf,
        flags: 2,
    },
    RangeFlags {
        start: 0x10fe0,
        end: 0x10ff6,
        flags: 0,
    },
    RangeFlags {
        start: 0x10ff7,
        end: 0x10fff,
        flags: 2,
    },
    RangeFlags {
        start: 0x11000,
        end: 0x11046,
        flags: 0,
    },
    RangeFlags {
        start: 0x11047,
        end: 0x11065,
        flags: 2,
    },
    RangeFlags {
        start: 0x11066,
        end: 0x11075,
        flags: 0,
    },
    RangeFlags {
        start: 0x11076,
        end: 0x1107e,
        flags: 2,
    },
    RangeFlags {
        start: 0x1107f,
        end: 0x110ba,
        flags: 0,
    },
    RangeFlags {
        start: 0x110bb,
        end: 0x110bc,
        flags: 2,
    },
    RangeFlags {
        start: 0x110bd,
        end: 0x110bd,
        flags: 0,
    },
    RangeFlags {
        start: 0x110be,
        end: 0x110c1,
        flags: 2,
    },
    RangeFlags {
        start: 0x110c2,
        end: 0x110c2,
        flags: 0,
    },
    RangeFlags {
        start: 0x110c3,
        end: 0x110cc,
        flags: 2,
    },
    RangeFlags {
        start: 0x110cd,
        end: 0x110cd,
        flags: 0,
    },
    RangeFlags {
        start: 0x110ce,
        end: 0x110cf,
        flags: 2,
    },
    RangeFlags {
        start: 0x110d0,
        end: 0x110e8,
        flags: 0,
    },
    RangeFlags {
        start: 0x110e9,
        end: 0x110ef,
        flags: 2,
    },
    RangeFlags {
        start: 0x110f0,
        end: 0x110f9,
        flags: 0,
    },
    RangeFlags {
        start: 0x110fa,
        end: 0x110ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x11100,
        end: 0x11134,
        flags: 0,
    },
    RangeFlags {
        start: 0x11135,
        end: 0x11135,
        flags: 2,
    },
    RangeFlags {
        start: 0x11136,
        end: 0x1113f,
        flags: 0,
    },
    RangeFlags {
        start: 0x11140,
        end: 0x11143,
        flags: 2,
    },
    RangeFlags {
        start: 0x11144,
        end: 0x11147,
        flags: 0,
    },
    RangeFlags {
        start: 0x11148,
        end: 0x1114f,
        flags: 2,
    },
    RangeFlags {
        start: 0x11150,
        end: 0x11173,
        flags: 0,
    },
    RangeFlags {
        start: 0x11174,
        end: 0x11175,
        flags: 2,
    },
    RangeFlags {
        start: 0x11176,
        end: 0x11176,
        flags: 0,
    },
    RangeFlags {
        start: 0x11177,
        end: 0x1117f,
        flags: 2,
    },
    RangeFlags {
        start: 0x11180,
        end: 0x111c4,
        flags: 0,
    },
    RangeFlags {
        start: 0x111c5,
        end: 0x111c8,
        flags: 2,
    },
    RangeFlags {
        start: 0x111c9,
        end: 0x111cc,
        flags: 0,
    },
    RangeFlags {
        start: 0x111cd,
        end: 0x111cd,
        flags: 2,
    },
    RangeFlags {
        start: 0x111ce,
        end: 0x111da,
        flags: 0,
    },
    RangeFlags {
        start: 0x111db,
        end: 0x111db,
        flags: 2,
    },
    RangeFlags {
        start: 0x111dc,
        end: 0x111dc,
        flags: 0,
    },
    RangeFlags {
        start: 0x111dd,
        end: 0x111ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x11200,
        end: 0x11211,
        flags: 0,
    },
    RangeFlags {
        start: 0x11212,
        end: 0x11212,
        flags: 2,
    },
    RangeFlags {
        start: 0x11213,
        end: 0x11237,
        flags: 0,
    },
    RangeFlags {
        start: 0x11238,
        end: 0x1123d,
        flags: 2,
    },
    RangeFlags {
        start: 0x1123e,
        end: 0x11241,
        flags: 0,
    },
    RangeFlags {
        start: 0x11242,
        end: 0x1127f,
        flags: 2,
    },
    RangeFlags {
        start: 0x11280,
        end: 0x11286,
        flags: 0,
    },
    RangeFlags {
        start: 0x11287,
        end: 0x11287,
        flags: 2,
    },
    RangeFlags {
        start: 0x11288,
        end: 0x11288,
        flags: 0,
    },
    RangeFlags {
        start: 0x11289,
        end: 0x11289,
        flags: 2,
    },
    RangeFlags {
        start: 0x1128a,
        end: 0x1128d,
        flags: 0,
    },
    RangeFlags {
        start: 0x1128e,
        end: 0x1128e,
        flags: 2,
    },
    RangeFlags {
        start: 0x1128f,
        end: 0x1129d,
        flags: 0,
    },
    RangeFlags {
        start: 0x1129e,
        end: 0x1129e,
        flags: 2,
    },
    RangeFlags {
        start: 0x1129f,
        end: 0x112a8,
        flags: 0,
    },
    RangeFlags {
        start: 0x112a9,
        end: 0x112af,
        flags: 2,
    },
    RangeFlags {
        start: 0x112b0,
        end: 0x112ea,
        flags: 0,
    },
    RangeFlags {
        start: 0x112eb,
        end: 0x112ef,
        flags: 2,
    },
    RangeFlags {
        start: 0x112f0,
        end: 0x112f9,
        flags: 0,
    },
    RangeFlags {
        start: 0x112fa,
        end: 0x112ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x11300,
        end: 0x11303,
        flags: 0,
    },
    RangeFlags {
        start: 0x11304,
        end: 0x11304,
        flags: 2,
    },
    RangeFlags {
        start: 0x11305,
        end: 0x1130c,
        flags: 0,
    },
    RangeFlags {
        start: 0x1130d,
        end: 0x1130e,
        flags: 2,
    },
    RangeFlags {
        start: 0x1130f,
        end: 0x11310,
        flags: 0,
    },
    RangeFlags {
        start: 0x11311,
        end: 0x11312,
        flags: 2,
    },
    RangeFlags {
        start: 0x11313,
        end: 0x11328,
        flags: 0,
    },
    RangeFlags {
        start: 0x11329,
        end: 0x11329,
        flags: 2,
    },
    RangeFlags {
        start: 0x1132a,
        end: 0x11330,
        flags: 0,
    },
    RangeFlags {
        start: 0x11331,
        end: 0x11331,
        flags: 2,
    },
    RangeFlags {
        start: 0x11332,
        end: 0x11333,
        flags: 0,
    },
    RangeFlags {
        start: 0x11334,
        end: 0x11334,
        flags: 2,
    },
    RangeFlags {
        start: 0x11335,
        end: 0x11339,
        flags: 0,
    },
    RangeFlags {
        start: 0x1133a,
        end: 0x1133a,
        flags: 2,
    },
    RangeFlags {
        start: 0x1133b,
        end: 0x11344,
        flags: 0,
    },
    RangeFlags {
        start: 0x11345,
        end: 0x11346,
        flags: 2,
    },
    RangeFlags {
        start: 0x11347,
        end: 0x11348,
        flags: 0,
    },
    RangeFlags {
        start: 0x11349,
        end: 0x1134a,
        flags: 2,
    },
    RangeFlags {
        start: 0x1134b,
        end: 0x1134d,
        flags: 0,
    },
    RangeFlags {
        start: 0x1134e,
        end: 0x1134f,
        flags: 2,
    },
    RangeFlags {
        start: 0x11350,
        end: 0x11350,
        flags: 0,
    },
    RangeFlags {
        start: 0x11351,
        end: 0x11356,
        flags: 2,
    },
    RangeFlags {
        start: 0x11357,
        end: 0x11357,
        flags: 0,
    },
    RangeFlags {
        start: 0x11358,
        end: 0x1135c,
        flags: 2,
    },
    RangeFlags {
        start: 0x1135d,
        end: 0x11363,
        flags: 0,
    },
    RangeFlags {
        start: 0x11364,
        end: 0x11365,
        flags: 2,
    },
    RangeFlags {
        start: 0x11366,
        end: 0x1136c,
        flags: 0,
    },
    RangeFlags {
        start: 0x1136d,
        end: 0x1136f,
        flags: 2,
    },
    RangeFlags {
        start: 0x11370,
        end: 0x11374,
        flags: 0,
    },
    RangeFlags {
        start: 0x11375,
        end: 0x113ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x11400,
        end: 0x1144a,
        flags: 0,
    },
    RangeFlags {
        start: 0x1144b,
        end: 0x1144f,
        flags: 2,
    },
    RangeFlags {
        start: 0x11450,
        end: 0x11459,
        flags: 0,
    },
    RangeFlags {
        start: 0x1145a,
        end: 0x1145d,
        flags: 2,
    },
    RangeFlags {
        start: 0x1145e,
        end: 0x11461,
        flags: 0,
    },
    RangeFlags {
        start: 0x11462,
        end: 0x1147f,
        flags: 2,
    },
    RangeFlags {
        start: 0x11480,
        end: 0x114c5,
        flags: 0,
    },
    RangeFlags {
        start: 0x114c6,
        end: 0x114c6,
        flags: 2,
    },
    RangeFlags {
        start: 0x114c7,
        end: 0x114c7,
        flags: 0,
    },
    RangeFlags {
        start: 0x114c8,
        end: 0x114cf,
        flags: 2,
    },
    RangeFlags {
        start: 0x114d0,
        end: 0x114d9,
        flags: 0,
    },
    RangeFlags {
        start: 0x114da,
        end: 0x1157f,
        flags: 2,
    },
    RangeFlags {
        start: 0x11580,
        end: 0x115b5,
        flags: 0,
    },
    RangeFlags {
        start: 0x115b6,
        end: 0x115b7,
        flags: 2,
    },
    RangeFlags {
        start: 0x115b8,
        end: 0x115c0,
        flags: 0,
    },
    RangeFlags {
        start: 0x115c1,
        end: 0x115d7,
        flags: 2,
    },
    RangeFlags {
        start: 0x115d8,
        end: 0x115dd,
        flags: 0,
    },
    RangeFlags {
        start: 0x115de,
        end: 0x115ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x11600,
        end: 0x11640,
        flags: 0,
    },
    RangeFlags {
        start: 0x11641,
        end: 0x11643,
        flags: 2,
    },
    RangeFlags {
        start: 0x11644,
        end: 0x11644,
        flags: 0,
    },
    RangeFlags {
        start: 0x11645,
        end: 0x1164f,
        flags: 2,
    },
    RangeFlags {
        start: 0x11650,
        end: 0x11659,
        flags: 0,
    },
    RangeFlags {
        start: 0x1165a,
        end: 0x1167f,
        flags: 2,
    },
    RangeFlags {
        start: 0x11680,
        end: 0x116b8,
        flags: 0,
    },
    RangeFlags {
        start: 0x116b9,
        end: 0x116bf,
        flags: 2,
    },
    RangeFlags {
        start: 0x116c0,
        end: 0x116c9,
        flags: 0,
    },
    RangeFlags {
        start: 0x116ca,
        end: 0x1171c,
        flags: 2,
    },
    RangeFlags {
        start: 0x1171d,
        end: 0x1172b,
        flags: 0,
    },
    RangeFlags {
        start: 0x1172c,
        end: 0x1172f,
        flags: 2,
    },
    RangeFlags {
        start: 0x11730,
        end: 0x11739,
        flags: 0,
    },
    RangeFlags {
        start: 0x1173a,
        end: 0x117ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x11800,
        end: 0x1183a,
        flags: 0,
    },
    RangeFlags {
        start: 0x1183b,
        end: 0x1189f,
        flags: 2,
    },
    RangeFlags {
        start: 0x118a0,
        end: 0x118df,
        flags: 1,
    },
    RangeFlags {
        start: 0x118e0,
        end: 0x118e9,
        flags: 0,
    },
    RangeFlags {
        start: 0x118ea,
        end: 0x118fe,
        flags: 2,
    },
    RangeFlags {
        start: 0x118ff,
        end: 0x11906,
        flags: 0,
    },
    RangeFlags {
        start: 0x11907,
        end: 0x11908,
        flags: 2,
    },
    RangeFlags {
        start: 0x11909,
        end: 0x11909,
        flags: 0,
    },
    RangeFlags {
        start: 0x1190a,
        end: 0x1190b,
        flags: 2,
    },
    RangeFlags {
        start: 0x1190c,
        end: 0x11913,
        flags: 0,
    },
    RangeFlags {
        start: 0x11914,
        end: 0x11914,
        flags: 2,
    },
    RangeFlags {
        start: 0x11915,
        end: 0x11916,
        flags: 0,
    },
    RangeFlags {
        start: 0x11917,
        end: 0x11917,
        flags: 2,
    },
    RangeFlags {
        start: 0x11918,
        end: 0x11935,
        flags: 0,
    },
    RangeFlags {
        start: 0x11936,
        end: 0x11936,
        flags: 2,
    },
    RangeFlags {
        start: 0x11937,
        end: 0x11938,
        flags: 0,
    },
    RangeFlags {
        start: 0x11939,
        end: 0x1193a,
        flags: 2,
    },
    RangeFlags {
        start: 0x1193b,
        end: 0x11943,
        flags: 0,
    },
    RangeFlags {
        start: 0x11944,
        end: 0x1194f,
        flags: 2,
    },
    RangeFlags {
        start: 0x11950,
        end: 0x11959,
        flags: 0,
    },
    RangeFlags {
        start: 0x1195a,
        end: 0x1199f,
        flags: 2,
    },
    RangeFlags {
        start: 0x119a0,
        end: 0x119a7,
        flags: 0,
    },
    RangeFlags {
        start: 0x119a8,
        end: 0x119a9,
        flags: 2,
    },
    RangeFlags {
        start: 0x119aa,
        end: 0x119d7,
        flags: 0,
    },
    RangeFlags {
        start: 0x119d8,
        end: 0x119d9,
        flags: 2,
    },
    RangeFlags {
        start: 0x119da,
        end: 0x119e1,
        flags: 0,
    },
    RangeFlags {
        start: 0x119e2,
        end: 0x119e2,
        flags: 2,
    },
    RangeFlags {
        start: 0x119e3,
        end: 0x119e4,
        flags: 0,
    },
    RangeFlags {
        start: 0x119e5,
        end: 0x119ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x11a00,
        end: 0x11a3e,
        flags: 0,
    },
    RangeFlags {
        start: 0x11a3f,
        end: 0x11a46,
        flags: 2,
    },
    RangeFlags {
        start: 0x11a47,
        end: 0x11a47,
        flags: 0,
    },
    RangeFlags {
        start: 0x11a48,
        end: 0x11a4f,
        flags: 2,
    },
    RangeFlags {
        start: 0x11a50,
        end: 0x11a99,
        flags: 0,
    },
    RangeFlags {
        start: 0x11a9a,
        end: 0x11a9c,
        flags: 2,
    },
    RangeFlags {
        start: 0x11a9d,
        end: 0x11a9d,
        flags: 0,
    },
    RangeFlags {
        start: 0x11a9e,
        end: 0x11aaf,
        flags: 2,
    },
    RangeFlags {
        start: 0x11ab0,
        end: 0x11af8,
        flags: 0,
    },
    RangeFlags {
        start: 0x11af9,
        end: 0x11bff,
        flags: 2,
    },
    RangeFlags {
        start: 0x11c00,
        end: 0x11c08,
        flags: 0,
    },
    RangeFlags {
        start: 0x11c09,
        end: 0x11c09,
        flags: 2,
    },
    RangeFlags {
        start: 0x11c0a,
        end: 0x11c36,
        flags: 0,
    },
    RangeFlags {
        start: 0x11c37,
        end: 0x11c37,
        flags: 2,
    },
    RangeFlags {
        start: 0x11c38,
        end: 0x11c40,
        flags: 0,
    },
    RangeFlags {
        start: 0x11c41,
        end: 0x11c4f,
        flags: 2,
    },
    RangeFlags {
        start: 0x11c50,
        end: 0x11c59,
        flags: 0,
    },
    RangeFlags {
        start: 0x11c5a,
        end: 0x11c71,
        flags: 2,
    },
    RangeFlags {
        start: 0x11c72,
        end: 0x11c8f,
        flags: 0,
    },
    RangeFlags {
        start: 0x11c90,
        end: 0x11c91,
        flags: 2,
    },
    RangeFlags {
        start: 0x11c92,
        end: 0x11ca7,
        flags: 0,
    },
    RangeFlags {
        start: 0x11ca8,
        end: 0x11ca8,
        flags: 2,
    },
    RangeFlags {
        start: 0x11ca9,
        end: 0x11cb6,
        flags: 0,
    },
    RangeFlags {
        start: 0x11cb7,
        end: 0x11cff,
        flags: 2,
    },
    RangeFlags {
        start: 0x11d00,
        end: 0x11d06,
        flags: 0,
    },
    RangeFlags {
        start: 0x11d07,
        end: 0x11d07,
        flags: 2,
    },
    RangeFlags {
        start: 0x11d08,
        end: 0x11d09,
        flags: 0,
    },
    RangeFlags {
        start: 0x11d0a,
        end: 0x11d0a,
        flags: 2,
    },
    RangeFlags {
        start: 0x11d0b,
        end: 0x11d36,
        flags: 0,
    },
    RangeFlags {
        start: 0x11d37,
        end: 0x11d39,
        flags: 2,
    },
    RangeFlags {
        start: 0x11d3a,
        end: 0x11d3a,
        flags: 0,
    },
    RangeFlags {
        start: 0x11d3b,
        end: 0x11d3b,
        flags: 2,
    },
    RangeFlags {
        start: 0x11d3c,
        end: 0x11d3d,
        flags: 0,
    },
    RangeFlags {
        start: 0x11d3e,
        end: 0x11d3e,
        flags: 2,
    },
    RangeFlags {
        start: 0x11d3f,
        end: 0x11d47,
        flags: 0,
    },
    RangeFlags {
        start: 0x11d48,
        end: 0x11d4f,
        flags: 2,
    },
    RangeFlags {
        start: 0x11d50,
        end: 0x11d59,
        flags: 0,
    },
    RangeFlags {
        start: 0x11d5a,
        end: 0x11d5f,
        flags: 2,
    },
    RangeFlags {
        start: 0x11d60,
        end: 0x11d65,
        flags: 0,
    },
    RangeFlags {
        start: 0x11d66,
        end: 0x11d66,
        flags: 2,
    },
    RangeFlags {
        start: 0x11d67,
        end: 0x11d68,
        flags: 0,
    },
    RangeFlags {
        start: 0x11d69,
        end: 0x11d69,
        flags: 2,
    },
    RangeFlags {
        start: 0x11d6a,
        end: 0x11d8e,
        flags: 0,
    },
    RangeFlags {
        start: 0x11d8f,
        end: 0x11d8f,
        flags: 2,
    },
    RangeFlags {
        start: 0x11d90,
        end: 0x11d91,
        flags: 0,
    },
    RangeFlags {
        start: 0x11d92,
        end: 0x11d92,
        flags: 2,
    },
    RangeFlags {
        start: 0x11d93,
        end: 0x11d98,
        flags: 0,
    },
    RangeFlags {
        start: 0x11d99,
        end: 0x11d9f,
        flags: 2,
    },
    RangeFlags {
        start: 0x11da0,
        end: 0x11da9,
        flags: 0,
    },
    RangeFlags {
        start: 0x11daa,
        end: 0x11edf,
        flags: 2,
    },
    RangeFlags {
        start: 0x11ee0,
        end: 0x11ef6,
        flags: 0,
    },
    RangeFlags {
        start: 0x11ef7,
        end: 0x11eff,
        flags: 2,
    },
    RangeFlags {
        start: 0x11f00,
        end: 0x11f10,
        flags: 0,
    },
    RangeFlags {
        start: 0x11f11,
        end: 0x11f11,
        flags: 2,
    },
    RangeFlags {
        start: 0x11f12,
        end: 0x11f3a,
        flags: 0,
    },
    RangeFlags {
        start: 0x11f3b,
        end: 0x11f3d,
        flags: 2,
    },
    RangeFlags {
        start: 0x11f3e,
        end: 0x11f42,
        flags: 0,
    },
    RangeFlags {
        start: 0x11f43,
        end: 0x11f4f,
        flags: 2,
    },
    RangeFlags {
        start: 0x11f50,
        end: 0x11f59,
        flags: 0,
    },
    RangeFlags {
        start: 0x11f5a,
        end: 0x11faf,
        flags: 2,
    },
    RangeFlags {
        start: 0x11fb0,
        end: 0x11fb0,
        flags: 0,
    },
    RangeFlags {
        start: 0x11fb1,
        end: 0x11fff,
        flags: 2,
    },
    RangeFlags {
        start: 0x12000,
        end: 0x12399,
        flags: 0,
    },
    RangeFlags {
        start: 0x1239a,
        end: 0x123ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x12400,
        end: 0x1246e,
        flags: 0,
    },
    RangeFlags {
        start: 0x1246f,
        end: 0x1247f,
        flags: 2,
    },
    RangeFlags {
        start: 0x12480,
        end: 0x12543,
        flags: 0,
    },
    RangeFlags {
        start: 0x12544,
        end: 0x12f8f,
        flags: 2,
    },
    RangeFlags {
        start: 0x12f90,
        end: 0x12ff0,
        flags: 0,
    },
    RangeFlags {
        start: 0x12ff1,
        end: 0x12fff,
        flags: 2,
    },
    RangeFlags {
        start: 0x13000,
        end: 0x13455,
        flags: 0,
    },
    RangeFlags {
        start: 0x13456,
        end: 0x143ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x14400,
        end: 0x14646,
        flags: 0,
    },
    RangeFlags {
        start: 0x14647,
        end: 0x167ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x16800,
        end: 0x16a38,
        flags: 0,
    },
    RangeFlags {
        start: 0x16a39,
        end: 0x16a3f,
        flags: 2,
    },
    RangeFlags {
        start: 0x16a40,
        end: 0x16a5e,
        flags: 0,
    },
    RangeFlags {
        start: 0x16a5f,
        end: 0x16a5f,
        flags: 2,
    },
    RangeFlags {
        start: 0x16a60,
        end: 0x16a69,
        flags: 0,
    },
    RangeFlags {
        start: 0x16a6a,
        end: 0x16a6f,
        flags: 2,
    },
    RangeFlags {
        start: 0x16a70,
        end: 0x16abe,
        flags: 0,
    },
    RangeFlags {
        start: 0x16abf,
        end: 0x16abf,
        flags: 2,
    },
    RangeFlags {
        start: 0x16ac0,
        end: 0x16ac9,
        flags: 0,
    },
    RangeFlags {
        start: 0x16aca,
        end: 0x16acf,
        flags: 2,
    },
    RangeFlags {
        start: 0x16ad0,
        end: 0x16aed,
        flags: 0,
    },
    RangeFlags {
        start: 0x16aee,
        end: 0x16aef,
        flags: 2,
    },
    RangeFlags {
        start: 0x16af0,
        end: 0x16af4,
        flags: 0,
    },
    RangeFlags {
        start: 0x16af5,
        end: 0x16aff,
        flags: 2,
    },
    RangeFlags {
        start: 0x16b00,
        end: 0x16b36,
        flags: 0,
    },
    RangeFlags {
        start: 0x16b37,
        end: 0x16b3f,
        flags: 2,
    },
    RangeFlags {
        start: 0x16b40,
        end: 0x16b43,
        flags: 0,
    },
    RangeFlags {
        start: 0x16b44,
        end: 0x16b4f,
        flags: 2,
    },
    RangeFlags {
        start: 0x16b50,
        end: 0x16b59,
        flags: 0,
    },
    RangeFlags {
        start: 0x16b5a,
        end: 0x16b62,
        flags: 2,
    },
    RangeFlags {
        start: 0x16b63,
        end: 0x16b77,
        flags: 0,
    },
    RangeFlags {
        start: 0x16b78,
        end: 0x16b7c,
        flags: 2,
    },
    RangeFlags {
        start: 0x16b7d,
        end: 0x16b8f,
        flags: 0,
    },
    RangeFlags {
        start: 0x16b90,
        end: 0x16e3f,
        flags: 2,
    },
    RangeFlags {
        start: 0x16e40,
        end: 0x16e7f,
        flags: 1,
    },
    RangeFlags {
        start: 0x16e80,
        end: 0x16eff,
        flags: 2,
    },
    RangeFlags {
        start: 0x16f00,
        end: 0x16f4a,
        flags: 0,
    },
    RangeFlags {
        start: 0x16f4b,
        end: 0x16f4e,
        flags: 2,
    },
    RangeFlags {
        start: 0x16f4f,
        end: 0x16f87,
        flags: 0,
    },
    RangeFlags {
        start: 0x16f88,
        end: 0x16f8e,
        flags: 2,
    },
    RangeFlags {
        start: 0x16f8f,
        end: 0x16f9f,
        flags: 0,
    },
    RangeFlags {
        start: 0x16fa0,
        end: 0x16fdf,
        flags: 2,
    },
    RangeFlags {
        start: 0x16fe0,
        end: 0x16fe1,
        flags: 0,
    },
    RangeFlags {
        start: 0x16fe2,
        end: 0x16fe2,
        flags: 2,
    },
    RangeFlags {
        start: 0x16fe3,
        end: 0x16fe4,
        flags: 0,
    },
    RangeFlags {
        start: 0x16fe5,
        end: 0x16fef,
        flags: 2,
    },
    RangeFlags {
        start: 0x16ff0,
        end: 0x16ff1,
        flags: 0,
    },
    RangeFlags {
        start: 0x16ff2,
        end: 0x1bbff,
        flags: 2,
    },
    RangeFlags {
        start: 0x1bc00,
        end: 0x1bc6a,
        flags: 0,
    },
    RangeFlags {
        start: 0x1bc6b,
        end: 0x1bc6f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1bc70,
        end: 0x1bc7c,
        flags: 0,
    },
    RangeFlags {
        start: 0x1bc7d,
        end: 0x1bc7f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1bc80,
        end: 0x1bc88,
        flags: 0,
    },
    RangeFlags {
        start: 0x1bc89,
        end: 0x1bc8f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1bc90,
        end: 0x1bc99,
        flags: 0,
    },
    RangeFlags {
        start: 0x1bc9a,
        end: 0x1bc9c,
        flags: 2,
    },
    RangeFlags {
        start: 0x1bc9d,
        end: 0x1bc9e,
        flags: 0,
    },
    RangeFlags {
        start: 0x1bc9f,
        end: 0x1bc9f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1bca0,
        end: 0x1bca3,
        flags: 0,
    },
    RangeFlags {
        start: 0x1bca4,
        end: 0x1ceff,
        flags: 2,
    },
    RangeFlags {
        start: 0x1cf00,
        end: 0x1cf2d,
        flags: 0,
    },
    RangeFlags {
        start: 0x1cf2e,
        end: 0x1cf2f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1cf30,
        end: 0x1cf46,
        flags: 0,
    },
    RangeFlags {
        start: 0x1cf47,
        end: 0x1d164,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d165,
        end: 0x1d169,
        flags: 0,
    },
    RangeFlags {
        start: 0x1d16a,
        end: 0x1d16c,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d16d,
        end: 0x1d182,
        flags: 0,
    },
    RangeFlags {
        start: 0x1d183,
        end: 0x1d184,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d185,
        end: 0x1d18b,
        flags: 0,
    },
    RangeFlags {
        start: 0x1d18c,
        end: 0x1d1a9,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d1aa,
        end: 0x1d1ad,
        flags: 0,
    },
    RangeFlags {
        start: 0x1d1ae,
        end: 0x1d241,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d242,
        end: 0x1d244,
        flags: 0,
    },
    RangeFlags {
        start: 0x1d245,
        end: 0x1d3ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d400,
        end: 0x1d454,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d455,
        end: 0x1d455,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d456,
        end: 0x1d49c,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d49d,
        end: 0x1d49d,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d49e,
        end: 0x1d49f,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d4a0,
        end: 0x1d4a1,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d4a2,
        end: 0x1d4a2,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d4a3,
        end: 0x1d4a4,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d4a5,
        end: 0x1d4a6,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d4a7,
        end: 0x1d4a8,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d4a9,
        end: 0x1d4ac,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d4ad,
        end: 0x1d4ad,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d4ae,
        end: 0x1d4b9,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d4ba,
        end: 0x1d4ba,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d4bb,
        end: 0x1d4bb,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d4bc,
        end: 0x1d4bc,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d4bd,
        end: 0x1d4c3,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d4c4,
        end: 0x1d4c4,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d4c5,
        end: 0x1d505,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d506,
        end: 0x1d506,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d507,
        end: 0x1d50a,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d50b,
        end: 0x1d50c,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d50d,
        end: 0x1d514,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d515,
        end: 0x1d515,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d516,
        end: 0x1d51c,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d51d,
        end: 0x1d51d,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d51e,
        end: 0x1d539,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d53a,
        end: 0x1d53a,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d53b,
        end: 0x1d53e,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d53f,
        end: 0x1d53f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d540,
        end: 0x1d544,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d545,
        end: 0x1d545,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d546,
        end: 0x1d546,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d547,
        end: 0x1d549,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d54a,
        end: 0x1d550,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d551,
        end: 0x1d551,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d552,
        end: 0x1d6a5,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d6a6,
        end: 0x1d6a7,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d6a8,
        end: 0x1d6c0,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d6c1,
        end: 0x1d6c1,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d6c2,
        end: 0x1d6da,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d6db,
        end: 0x1d6db,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d6dc,
        end: 0x1d6fa,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d6fb,
        end: 0x1d6fb,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d6fc,
        end: 0x1d714,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d715,
        end: 0x1d715,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d716,
        end: 0x1d734,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d735,
        end: 0x1d735,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d736,
        end: 0x1d74e,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d74f,
        end: 0x1d74f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d750,
        end: 0x1d76e,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d76f,
        end: 0x1d76f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d770,
        end: 0x1d788,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d789,
        end: 0x1d789,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d78a,
        end: 0x1d7a8,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d7a9,
        end: 0x1d7a9,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d7aa,
        end: 0x1d7c2,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d7c3,
        end: 0x1d7c3,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d7c4,
        end: 0x1d7cb,
        flags: 1,
    },
    RangeFlags {
        start: 0x1d7cc,
        end: 0x1d7cd,
        flags: 2,
    },
    RangeFlags {
        start: 0x1d7ce,
        end: 0x1d7ff,
        flags: 0,
    },
    RangeFlags {
        start: 0x1d800,
        end: 0x1d9ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x1da00,
        end: 0x1da36,
        flags: 0,
    },
    RangeFlags {
        start: 0x1da37,
        end: 0x1da3a,
        flags: 2,
    },
    RangeFlags {
        start: 0x1da3b,
        end: 0x1da6c,
        flags: 0,
    },
    RangeFlags {
        start: 0x1da6d,
        end: 0x1da74,
        flags: 2,
    },
    RangeFlags {
        start: 0x1da75,
        end: 0x1da75,
        flags: 0,
    },
    RangeFlags {
        start: 0x1da76,
        end: 0x1da83,
        flags: 2,
    },
    RangeFlags {
        start: 0x1da84,
        end: 0x1da84,
        flags: 0,
    },
    RangeFlags {
        start: 0x1da85,
        end: 0x1da9a,
        flags: 2,
    },
    RangeFlags {
        start: 0x1da9b,
        end: 0x1da9f,
        flags: 0,
    },
    RangeFlags {
        start: 0x1daa0,
        end: 0x1daa0,
        flags: 2,
    },
    RangeFlags {
        start: 0x1daa1,
        end: 0x1daaf,
        flags: 0,
    },
    RangeFlags {
        start: 0x1dab0,
        end: 0x1deff,
        flags: 2,
    },
    RangeFlags {
        start: 0x1df00,
        end: 0x1df09,
        flags: 1,
    },
    RangeFlags {
        start: 0x1df0a,
        end: 0x1df0a,
        flags: 0,
    },
    RangeFlags {
        start: 0x1df0b,
        end: 0x1df1e,
        flags: 1,
    },
    RangeFlags {
        start: 0x1df1f,
        end: 0x1df24,
        flags: 2,
    },
    RangeFlags {
        start: 0x1df25,
        end: 0x1df2a,
        flags: 1,
    },
    RangeFlags {
        start: 0x1df2b,
        end: 0x1dfff,
        flags: 2,
    },
    RangeFlags {
        start: 0x1e000,
        end: 0x1e006,
        flags: 0,
    },
    RangeFlags {
        start: 0x1e007,
        end: 0x1e007,
        flags: 2,
    },
    RangeFlags {
        start: 0x1e008,
        end: 0x1e018,
        flags: 0,
    },
    RangeFlags {
        start: 0x1e019,
        end: 0x1e01a,
        flags: 2,
    },
    RangeFlags {
        start: 0x1e01b,
        end: 0x1e021,
        flags: 0,
    },
    RangeFlags {
        start: 0x1e022,
        end: 0x1e022,
        flags: 2,
    },
    RangeFlags {
        start: 0x1e023,
        end: 0x1e024,
        flags: 0,
    },
    RangeFlags {
        start: 0x1e025,
        end: 0x1e025,
        flags: 2,
    },
    RangeFlags {
        start: 0x1e026,
        end: 0x1e02a,
        flags: 0,
    },
    RangeFlags {
        start: 0x1e02b,
        end: 0x1e02f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1e030,
        end: 0x1e06d,
        flags: 1,
    },
    RangeFlags {
        start: 0x1e06e,
        end: 0x1e08e,
        flags: 2,
    },
    RangeFlags {
        start: 0x1e08f,
        end: 0x1e08f,
        flags: 0,
    },
    RangeFlags {
        start: 0x1e090,
        end: 0x1e0ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x1e100,
        end: 0x1e12c,
        flags: 0,
    },
    RangeFlags {
        start: 0x1e12d,
        end: 0x1e12f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1e130,
        end: 0x1e13d,
        flags: 0,
    },
    RangeFlags {
        start: 0x1e13e,
        end: 0x1e13f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1e140,
        end: 0x1e149,
        flags: 0,
    },
    RangeFlags {
        start: 0x1e14a,
        end: 0x1e14d,
        flags: 2,
    },
    RangeFlags {
        start: 0x1e14e,
        end: 0x1e14e,
        flags: 0,
    },
    RangeFlags {
        start: 0x1e14f,
        end: 0x1e28f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1e290,
        end: 0x1e2ae,
        flags: 0,
    },
    RangeFlags {
        start: 0x1e2af,
        end: 0x1e2bf,
        flags: 2,
    },
    RangeFlags {
        start: 0x1e2c0,
        end: 0x1e2f9,
        flags: 0,
    },
    RangeFlags {
        start: 0x1e2fa,
        end: 0x1e4cf,
        flags: 2,
    },
    RangeFlags {
        start: 0x1e4d0,
        end: 0x1e4f9,
        flags: 0,
    },
    RangeFlags {
        start: 0x1e4fa,
        end: 0x1e7df,
        flags: 2,
    },
    RangeFlags {
        start: 0x1e7e0,
        end: 0x1e7e6,
        flags: 0,
    },
    RangeFlags {
        start: 0x1e7e7,
        end: 0x1e7e7,
        flags: 2,
    },
    RangeFlags {
        start: 0x1e7e8,
        end: 0x1e7eb,
        flags: 0,
    },
    RangeFlags {
        start: 0x1e7ec,
        end: 0x1e7ec,
        flags: 2,
    },
    RangeFlags {
        start: 0x1e7ed,
        end: 0x1e7ee,
        flags: 0,
    },
    RangeFlags {
        start: 0x1e7ef,
        end: 0x1e7ef,
        flags: 2,
    },
    RangeFlags {
        start: 0x1e7f0,
        end: 0x1e7fe,
        flags: 0,
    },
    RangeFlags {
        start: 0x1e7ff,
        end: 0x1e7ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x1e800,
        end: 0x1e8c4,
        flags: 0,
    },
    RangeFlags {
        start: 0x1e8c5,
        end: 0x1e8cf,
        flags: 2,
    },
    RangeFlags {
        start: 0x1e8d0,
        end: 0x1e8d6,
        flags: 0,
    },
    RangeFlags {
        start: 0x1e8d7,
        end: 0x1e8ff,
        flags: 2,
    },
    RangeFlags {
        start: 0x1e900,
        end: 0x1e943,
        flags: 1,
    },
    RangeFlags {
        start: 0x1e944,
        end: 0x1e94b,
        flags: 0,
    },
    RangeFlags {
        start: 0x1e94c,
        end: 0x1e94f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1e950,
        end: 0x1e959,
        flags: 0,
    },
    RangeFlags {
        start: 0x1e95a,
        end: 0x1edff,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee00,
        end: 0x1ee03,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee04,
        end: 0x1ee04,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee05,
        end: 0x1ee1f,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee20,
        end: 0x1ee20,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee21,
        end: 0x1ee22,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee23,
        end: 0x1ee23,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee24,
        end: 0x1ee24,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee25,
        end: 0x1ee26,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee27,
        end: 0x1ee27,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee28,
        end: 0x1ee28,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee29,
        end: 0x1ee32,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee33,
        end: 0x1ee33,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee34,
        end: 0x1ee37,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee38,
        end: 0x1ee38,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee39,
        end: 0x1ee39,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee3a,
        end: 0x1ee3a,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee3b,
        end: 0x1ee3b,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee3c,
        end: 0x1ee41,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee42,
        end: 0x1ee42,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee43,
        end: 0x1ee46,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee47,
        end: 0x1ee47,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee48,
        end: 0x1ee48,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee49,
        end: 0x1ee49,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee4a,
        end: 0x1ee4a,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee4b,
        end: 0x1ee4b,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee4c,
        end: 0x1ee4c,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee4d,
        end: 0x1ee4f,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee50,
        end: 0x1ee50,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee51,
        end: 0x1ee52,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee53,
        end: 0x1ee53,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee54,
        end: 0x1ee54,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee55,
        end: 0x1ee56,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee57,
        end: 0x1ee57,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee58,
        end: 0x1ee58,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee59,
        end: 0x1ee59,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee5a,
        end: 0x1ee5a,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee5b,
        end: 0x1ee5b,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee5c,
        end: 0x1ee5c,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee5d,
        end: 0x1ee5d,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee5e,
        end: 0x1ee5e,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee5f,
        end: 0x1ee5f,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee60,
        end: 0x1ee60,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee61,
        end: 0x1ee62,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee63,
        end: 0x1ee63,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee64,
        end: 0x1ee64,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee65,
        end: 0x1ee66,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee67,
        end: 0x1ee6a,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee6b,
        end: 0x1ee6b,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee6c,
        end: 0x1ee72,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee73,
        end: 0x1ee73,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee74,
        end: 0x1ee77,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee78,
        end: 0x1ee78,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee79,
        end: 0x1ee7c,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee7d,
        end: 0x1ee7d,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee7e,
        end: 0x1ee7e,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee7f,
        end: 0x1ee7f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee80,
        end: 0x1ee89,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee8a,
        end: 0x1ee8a,
        flags: 2,
    },
    RangeFlags {
        start: 0x1ee8b,
        end: 0x1ee9b,
        flags: 0,
    },
    RangeFlags {
        start: 0x1ee9c,
        end: 0x1eea0,
        flags: 2,
    },
    RangeFlags {
        start: 0x1eea1,
        end: 0x1eea3,
        flags: 0,
    },
    RangeFlags {
        start: 0x1eea4,
        end: 0x1eea4,
        flags: 2,
    },
    RangeFlags {
        start: 0x1eea5,
        end: 0x1eea9,
        flags: 0,
    },
    RangeFlags {
        start: 0x1eeaa,
        end: 0x1eeaa,
        flags: 2,
    },
    RangeFlags {
        start: 0x1eeab,
        end: 0x1eebb,
        flags: 0,
    },
    RangeFlags {
        start: 0x1eebc,
        end: 0x1f12f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1f130,
        end: 0x1f149,
        flags: 1,
    },
    RangeFlags {
        start: 0x1f14a,
        end: 0x1f14f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1f150,
        end: 0x1f169,
        flags: 1,
    },
    RangeFlags {
        start: 0x1f16a,
        end: 0x1f16f,
        flags: 2,
    },
    RangeFlags {
        start: 0x1f170,
        end: 0x1f189,
        flags: 1,
    },
    RangeFlags {
        start: 0x1f18a,
        end: 0x1f3fa,
        flags: 2,
    },
    RangeFlags {
        start: 0x1f3fb,
        end: 0x1f3ff,
        flags: 0,
    },
    RangeFlags {
        start: 0x1f400,
        end: 0x1fbef,
        flags: 2,
    },
    RangeFlags {
        start: 0x1fbf0,
        end: 0x1fbf9,
        flags: 0,
    },
    RangeFlags {
        start: 0x1fbfa,
        end: 0xe0000,
        flags: 2,
    },
    RangeFlags {
        start: 0xe0001,
        end: 0xe0001,
        flags: 0,
    },
    RangeFlags {
        start: 0xe0002,
        end: 0xe001f,
        flags: 2,
    },
    RangeFlags {
        start: 0xe0020,
        end: 0xe007f,
        flags: 0,
    },
    RangeFlags {
        start: 0xe0080,
        end: 0xe00ff,
        flags: 2,
    },
    RangeFlags {
        start: 0xe0100,
        end: 0xe01ef,
        flags: 0,
    },
    RangeFlags {
        start: 0xe01f0,
        end: 0x0010_ffff,
        flags: 2,
    },
];
#[derive(Clone, Copy)]
pub(super) struct TitleMapping {
    pub code: u32,
    pub replacement: &'static str,
}

pub(super) const MAPPINGS: &[TitleMapping] = &[
    TitleMapping {
        code: 0x61,
        replacement: "A",
    },
    TitleMapping {
        code: 0x62,
        replacement: "B",
    },
    TitleMapping {
        code: 0x63,
        replacement: "C",
    },
    TitleMapping {
        code: 0x64,
        replacement: "D",
    },
    TitleMapping {
        code: 0x65,
        replacement: "E",
    },
    TitleMapping {
        code: 0x66,
        replacement: "F",
    },
    TitleMapping {
        code: 0x67,
        replacement: "G",
    },
    TitleMapping {
        code: 0x68,
        replacement: "H",
    },
    TitleMapping {
        code: 0x69,
        replacement: "I",
    },
    TitleMapping {
        code: 0x6a,
        replacement: "J",
    },
    TitleMapping {
        code: 0x6b,
        replacement: "K",
    },
    TitleMapping {
        code: 0x6c,
        replacement: "L",
    },
    TitleMapping {
        code: 0x6d,
        replacement: "M",
    },
    TitleMapping {
        code: 0x6e,
        replacement: "N",
    },
    TitleMapping {
        code: 0x6f,
        replacement: "O",
    },
    TitleMapping {
        code: 0x70,
        replacement: "P",
    },
    TitleMapping {
        code: 0x71,
        replacement: "Q",
    },
    TitleMapping {
        code: 0x72,
        replacement: "R",
    },
    TitleMapping {
        code: 0x73,
        replacement: "S",
    },
    TitleMapping {
        code: 0x74,
        replacement: "T",
    },
    TitleMapping {
        code: 0x75,
        replacement: "U",
    },
    TitleMapping {
        code: 0x76,
        replacement: "V",
    },
    TitleMapping {
        code: 0x77,
        replacement: "W",
    },
    TitleMapping {
        code: 0x78,
        replacement: "X",
    },
    TitleMapping {
        code: 0x79,
        replacement: "Y",
    },
    TitleMapping {
        code: 0x7a,
        replacement: "Z",
    },
    TitleMapping {
        code: 0xb5,
        replacement: "Μ",
    },
    TitleMapping {
        code: 0xdf,
        replacement: "Ss",
    },
    TitleMapping {
        code: 0xe0,
        replacement: "À",
    },
    TitleMapping {
        code: 0xe1,
        replacement: "Á",
    },
    TitleMapping {
        code: 0xe2,
        replacement: "Â",
    },
    TitleMapping {
        code: 0xe3,
        replacement: "Ã",
    },
    TitleMapping {
        code: 0xe4,
        replacement: "Ä",
    },
    TitleMapping {
        code: 0xe5,
        replacement: "Å",
    },
    TitleMapping {
        code: 0xe6,
        replacement: "Æ",
    },
    TitleMapping {
        code: 0xe7,
        replacement: "Ç",
    },
    TitleMapping {
        code: 0xe8,
        replacement: "È",
    },
    TitleMapping {
        code: 0xe9,
        replacement: "É",
    },
    TitleMapping {
        code: 0xea,
        replacement: "Ê",
    },
    TitleMapping {
        code: 0xeb,
        replacement: "Ë",
    },
    TitleMapping {
        code: 0xec,
        replacement: "Ì",
    },
    TitleMapping {
        code: 0xed,
        replacement: "Í",
    },
    TitleMapping {
        code: 0xee,
        replacement: "Î",
    },
    TitleMapping {
        code: 0xef,
        replacement: "Ï",
    },
    TitleMapping {
        code: 0xf0,
        replacement: "Ð",
    },
    TitleMapping {
        code: 0xf1,
        replacement: "Ñ",
    },
    TitleMapping {
        code: 0xf2,
        replacement: "Ò",
    },
    TitleMapping {
        code: 0xf3,
        replacement: "Ó",
    },
    TitleMapping {
        code: 0xf4,
        replacement: "Ô",
    },
    TitleMapping {
        code: 0xf5,
        replacement: "Õ",
    },
    TitleMapping {
        code: 0xf6,
        replacement: "Ö",
    },
    TitleMapping {
        code: 0xf8,
        replacement: "Ø",
    },
    TitleMapping {
        code: 0xf9,
        replacement: "Ù",
    },
    TitleMapping {
        code: 0xfa,
        replacement: "Ú",
    },
    TitleMapping {
        code: 0xfb,
        replacement: "Û",
    },
    TitleMapping {
        code: 0xfc,
        replacement: "Ü",
    },
    TitleMapping {
        code: 0xfd,
        replacement: "Ý",
    },
    TitleMapping {
        code: 0xfe,
        replacement: "Þ",
    },
    TitleMapping {
        code: 0xff,
        replacement: "Ÿ",
    },
    TitleMapping {
        code: 0x101,
        replacement: "Ā",
    },
    TitleMapping {
        code: 0x103,
        replacement: "Ă",
    },
    TitleMapping {
        code: 0x105,
        replacement: "Ą",
    },
    TitleMapping {
        code: 0x107,
        replacement: "Ć",
    },
    TitleMapping {
        code: 0x109,
        replacement: "Ĉ",
    },
    TitleMapping {
        code: 0x10b,
        replacement: "Ċ",
    },
    TitleMapping {
        code: 0x10d,
        replacement: "Č",
    },
    TitleMapping {
        code: 0x10f,
        replacement: "Ď",
    },
    TitleMapping {
        code: 0x111,
        replacement: "Đ",
    },
    TitleMapping {
        code: 0x113,
        replacement: "Ē",
    },
    TitleMapping {
        code: 0x115,
        replacement: "Ĕ",
    },
    TitleMapping {
        code: 0x117,
        replacement: "Ė",
    },
    TitleMapping {
        code: 0x119,
        replacement: "Ę",
    },
    TitleMapping {
        code: 0x11b,
        replacement: "Ě",
    },
    TitleMapping {
        code: 0x11d,
        replacement: "Ĝ",
    },
    TitleMapping {
        code: 0x11f,
        replacement: "Ğ",
    },
    TitleMapping {
        code: 0x121,
        replacement: "Ġ",
    },
    TitleMapping {
        code: 0x123,
        replacement: "Ģ",
    },
    TitleMapping {
        code: 0x125,
        replacement: "Ĥ",
    },
    TitleMapping {
        code: 0x127,
        replacement: "Ħ",
    },
    TitleMapping {
        code: 0x129,
        replacement: "Ĩ",
    },
    TitleMapping {
        code: 0x12b,
        replacement: "Ī",
    },
    TitleMapping {
        code: 0x12d,
        replacement: "Ĭ",
    },
    TitleMapping {
        code: 0x12f,
        replacement: "Į",
    },
    TitleMapping {
        code: 0x131,
        replacement: "I",
    },
    TitleMapping {
        code: 0x133,
        replacement: "Ĳ",
    },
    TitleMapping {
        code: 0x135,
        replacement: "Ĵ",
    },
    TitleMapping {
        code: 0x137,
        replacement: "Ķ",
    },
    TitleMapping {
        code: 0x13a,
        replacement: "Ĺ",
    },
    TitleMapping {
        code: 0x13c,
        replacement: "Ļ",
    },
    TitleMapping {
        code: 0x13e,
        replacement: "Ľ",
    },
    TitleMapping {
        code: 0x140,
        replacement: "Ŀ",
    },
    TitleMapping {
        code: 0x142,
        replacement: "Ł",
    },
    TitleMapping {
        code: 0x144,
        replacement: "Ń",
    },
    TitleMapping {
        code: 0x146,
        replacement: "Ņ",
    },
    TitleMapping {
        code: 0x148,
        replacement: "Ň",
    },
    TitleMapping {
        code: 0x149,
        replacement: "ʼN",
    },
    TitleMapping {
        code: 0x14b,
        replacement: "Ŋ",
    },
    TitleMapping {
        code: 0x14d,
        replacement: "Ō",
    },
    TitleMapping {
        code: 0x14f,
        replacement: "Ŏ",
    },
    TitleMapping {
        code: 0x151,
        replacement: "Ő",
    },
    TitleMapping {
        code: 0x153,
        replacement: "Œ",
    },
    TitleMapping {
        code: 0x155,
        replacement: "Ŕ",
    },
    TitleMapping {
        code: 0x157,
        replacement: "Ŗ",
    },
    TitleMapping {
        code: 0x159,
        replacement: "Ř",
    },
    TitleMapping {
        code: 0x15b,
        replacement: "Ś",
    },
    TitleMapping {
        code: 0x15d,
        replacement: "Ŝ",
    },
    TitleMapping {
        code: 0x15f,
        replacement: "Ş",
    },
    TitleMapping {
        code: 0x161,
        replacement: "Š",
    },
    TitleMapping {
        code: 0x163,
        replacement: "Ţ",
    },
    TitleMapping {
        code: 0x165,
        replacement: "Ť",
    },
    TitleMapping {
        code: 0x167,
        replacement: "Ŧ",
    },
    TitleMapping {
        code: 0x169,
        replacement: "Ũ",
    },
    TitleMapping {
        code: 0x16b,
        replacement: "Ū",
    },
    TitleMapping {
        code: 0x16d,
        replacement: "Ŭ",
    },
    TitleMapping {
        code: 0x16f,
        replacement: "Ů",
    },
    TitleMapping {
        code: 0x171,
        replacement: "Ű",
    },
    TitleMapping {
        code: 0x173,
        replacement: "Ų",
    },
    TitleMapping {
        code: 0x175,
        replacement: "Ŵ",
    },
    TitleMapping {
        code: 0x177,
        replacement: "Ŷ",
    },
    TitleMapping {
        code: 0x17a,
        replacement: "Ź",
    },
    TitleMapping {
        code: 0x17c,
        replacement: "Ż",
    },
    TitleMapping {
        code: 0x17e,
        replacement: "Ž",
    },
    TitleMapping {
        code: 0x17f,
        replacement: "S",
    },
    TitleMapping {
        code: 0x180,
        replacement: "Ƀ",
    },
    TitleMapping {
        code: 0x183,
        replacement: "Ƃ",
    },
    TitleMapping {
        code: 0x185,
        replacement: "Ƅ",
    },
    TitleMapping {
        code: 0x188,
        replacement: "Ƈ",
    },
    TitleMapping {
        code: 0x18c,
        replacement: "Ƌ",
    },
    TitleMapping {
        code: 0x192,
        replacement: "Ƒ",
    },
    TitleMapping {
        code: 0x195,
        replacement: "Ƕ",
    },
    TitleMapping {
        code: 0x199,
        replacement: "Ƙ",
    },
    TitleMapping {
        code: 0x19a,
        replacement: "Ƚ",
    },
    TitleMapping {
        code: 0x19e,
        replacement: "Ƞ",
    },
    TitleMapping {
        code: 0x1a1,
        replacement: "Ơ",
    },
    TitleMapping {
        code: 0x1a3,
        replacement: "Ƣ",
    },
    TitleMapping {
        code: 0x1a5,
        replacement: "Ƥ",
    },
    TitleMapping {
        code: 0x1a8,
        replacement: "Ƨ",
    },
    TitleMapping {
        code: 0x1ad,
        replacement: "Ƭ",
    },
    TitleMapping {
        code: 0x1b0,
        replacement: "Ư",
    },
    TitleMapping {
        code: 0x1b4,
        replacement: "Ƴ",
    },
    TitleMapping {
        code: 0x1b6,
        replacement: "Ƶ",
    },
    TitleMapping {
        code: 0x1b9,
        replacement: "Ƹ",
    },
    TitleMapping {
        code: 0x1bd,
        replacement: "Ƽ",
    },
    TitleMapping {
        code: 0x1bf,
        replacement: "Ƿ",
    },
    TitleMapping {
        code: 0x1c4,
        replacement: "ǅ",
    },
    TitleMapping {
        code: 0x1c6,
        replacement: "ǅ",
    },
    TitleMapping {
        code: 0x1c7,
        replacement: "ǈ",
    },
    TitleMapping {
        code: 0x1c9,
        replacement: "ǈ",
    },
    TitleMapping {
        code: 0x1ca,
        replacement: "ǋ",
    },
    TitleMapping {
        code: 0x1cc,
        replacement: "ǋ",
    },
    TitleMapping {
        code: 0x1ce,
        replacement: "Ǎ",
    },
    TitleMapping {
        code: 0x1d0,
        replacement: "Ǐ",
    },
    TitleMapping {
        code: 0x1d2,
        replacement: "Ǒ",
    },
    TitleMapping {
        code: 0x1d4,
        replacement: "Ǔ",
    },
    TitleMapping {
        code: 0x1d6,
        replacement: "Ǖ",
    },
    TitleMapping {
        code: 0x1d8,
        replacement: "Ǘ",
    },
    TitleMapping {
        code: 0x1da,
        replacement: "Ǚ",
    },
    TitleMapping {
        code: 0x1dc,
        replacement: "Ǜ",
    },
    TitleMapping {
        code: 0x1dd,
        replacement: "Ǝ",
    },
    TitleMapping {
        code: 0x1df,
        replacement: "Ǟ",
    },
    TitleMapping {
        code: 0x1e1,
        replacement: "Ǡ",
    },
    TitleMapping {
        code: 0x1e3,
        replacement: "Ǣ",
    },
    TitleMapping {
        code: 0x1e5,
        replacement: "Ǥ",
    },
    TitleMapping {
        code: 0x1e7,
        replacement: "Ǧ",
    },
    TitleMapping {
        code: 0x1e9,
        replacement: "Ǩ",
    },
    TitleMapping {
        code: 0x1eb,
        replacement: "Ǫ",
    },
    TitleMapping {
        code: 0x1ed,
        replacement: "Ǭ",
    },
    TitleMapping {
        code: 0x1ef,
        replacement: "Ǯ",
    },
    TitleMapping {
        code: 0x1f0,
        replacement: "J̌",
    },
    TitleMapping {
        code: 0x1f1,
        replacement: "ǲ",
    },
    TitleMapping {
        code: 0x1f3,
        replacement: "ǲ",
    },
    TitleMapping {
        code: 0x1f5,
        replacement: "Ǵ",
    },
    TitleMapping {
        code: 0x1f9,
        replacement: "Ǹ",
    },
    TitleMapping {
        code: 0x1fb,
        replacement: "Ǻ",
    },
    TitleMapping {
        code: 0x1fd,
        replacement: "Ǽ",
    },
    TitleMapping {
        code: 0x1ff,
        replacement: "Ǿ",
    },
    TitleMapping {
        code: 0x201,
        replacement: "Ȁ",
    },
    TitleMapping {
        code: 0x203,
        replacement: "Ȃ",
    },
    TitleMapping {
        code: 0x205,
        replacement: "Ȅ",
    },
    TitleMapping {
        code: 0x207,
        replacement: "Ȇ",
    },
    TitleMapping {
        code: 0x209,
        replacement: "Ȉ",
    },
    TitleMapping {
        code: 0x20b,
        replacement: "Ȋ",
    },
    TitleMapping {
        code: 0x20d,
        replacement: "Ȍ",
    },
    TitleMapping {
        code: 0x20f,
        replacement: "Ȏ",
    },
    TitleMapping {
        code: 0x211,
        replacement: "Ȑ",
    },
    TitleMapping {
        code: 0x213,
        replacement: "Ȓ",
    },
    TitleMapping {
        code: 0x215,
        replacement: "Ȕ",
    },
    TitleMapping {
        code: 0x217,
        replacement: "Ȗ",
    },
    TitleMapping {
        code: 0x219,
        replacement: "Ș",
    },
    TitleMapping {
        code: 0x21b,
        replacement: "Ț",
    },
    TitleMapping {
        code: 0x21d,
        replacement: "Ȝ",
    },
    TitleMapping {
        code: 0x21f,
        replacement: "Ȟ",
    },
    TitleMapping {
        code: 0x223,
        replacement: "Ȣ",
    },
    TitleMapping {
        code: 0x225,
        replacement: "Ȥ",
    },
    TitleMapping {
        code: 0x227,
        replacement: "Ȧ",
    },
    TitleMapping {
        code: 0x229,
        replacement: "Ȩ",
    },
    TitleMapping {
        code: 0x22b,
        replacement: "Ȫ",
    },
    TitleMapping {
        code: 0x22d,
        replacement: "Ȭ",
    },
    TitleMapping {
        code: 0x22f,
        replacement: "Ȯ",
    },
    TitleMapping {
        code: 0x231,
        replacement: "Ȱ",
    },
    TitleMapping {
        code: 0x233,
        replacement: "Ȳ",
    },
    TitleMapping {
        code: 0x23c,
        replacement: "Ȼ",
    },
    TitleMapping {
        code: 0x23f,
        replacement: "Ȿ",
    },
    TitleMapping {
        code: 0x240,
        replacement: "Ɀ",
    },
    TitleMapping {
        code: 0x242,
        replacement: "Ɂ",
    },
    TitleMapping {
        code: 0x247,
        replacement: "Ɇ",
    },
    TitleMapping {
        code: 0x249,
        replacement: "Ɉ",
    },
    TitleMapping {
        code: 0x24b,
        replacement: "Ɋ",
    },
    TitleMapping {
        code: 0x24d,
        replacement: "Ɍ",
    },
    TitleMapping {
        code: 0x24f,
        replacement: "Ɏ",
    },
    TitleMapping {
        code: 0x250,
        replacement: "Ɐ",
    },
    TitleMapping {
        code: 0x251,
        replacement: "Ɑ",
    },
    TitleMapping {
        code: 0x252,
        replacement: "Ɒ",
    },
    TitleMapping {
        code: 0x253,
        replacement: "Ɓ",
    },
    TitleMapping {
        code: 0x254,
        replacement: "Ɔ",
    },
    TitleMapping {
        code: 0x256,
        replacement: "Ɖ",
    },
    TitleMapping {
        code: 0x257,
        replacement: "Ɗ",
    },
    TitleMapping {
        code: 0x259,
        replacement: "Ə",
    },
    TitleMapping {
        code: 0x25b,
        replacement: "Ɛ",
    },
    TitleMapping {
        code: 0x25c,
        replacement: "Ɜ",
    },
    TitleMapping {
        code: 0x260,
        replacement: "Ɠ",
    },
    TitleMapping {
        code: 0x261,
        replacement: "Ɡ",
    },
    TitleMapping {
        code: 0x263,
        replacement: "Ɣ",
    },
    TitleMapping {
        code: 0x265,
        replacement: "Ɥ",
    },
    TitleMapping {
        code: 0x266,
        replacement: "Ɦ",
    },
    TitleMapping {
        code: 0x268,
        replacement: "Ɨ",
    },
    TitleMapping {
        code: 0x269,
        replacement: "Ɩ",
    },
    TitleMapping {
        code: 0x26a,
        replacement: "Ɪ",
    },
    TitleMapping {
        code: 0x26b,
        replacement: "Ɫ",
    },
    TitleMapping {
        code: 0x26c,
        replacement: "Ɬ",
    },
    TitleMapping {
        code: 0x26f,
        replacement: "Ɯ",
    },
    TitleMapping {
        code: 0x271,
        replacement: "Ɱ",
    },
    TitleMapping {
        code: 0x272,
        replacement: "Ɲ",
    },
    TitleMapping {
        code: 0x275,
        replacement: "Ɵ",
    },
    TitleMapping {
        code: 0x27d,
        replacement: "Ɽ",
    },
    TitleMapping {
        code: 0x280,
        replacement: "Ʀ",
    },
    TitleMapping {
        code: 0x282,
        replacement: "Ʂ",
    },
    TitleMapping {
        code: 0x283,
        replacement: "Ʃ",
    },
    TitleMapping {
        code: 0x287,
        replacement: "Ʇ",
    },
    TitleMapping {
        code: 0x288,
        replacement: "Ʈ",
    },
    TitleMapping {
        code: 0x289,
        replacement: "Ʉ",
    },
    TitleMapping {
        code: 0x28a,
        replacement: "Ʊ",
    },
    TitleMapping {
        code: 0x28b,
        replacement: "Ʋ",
    },
    TitleMapping {
        code: 0x28c,
        replacement: "Ʌ",
    },
    TitleMapping {
        code: 0x292,
        replacement: "Ʒ",
    },
    TitleMapping {
        code: 0x29d,
        replacement: "Ʝ",
    },
    TitleMapping {
        code: 0x29e,
        replacement: "Ʞ",
    },
    TitleMapping {
        code: 0x345,
        replacement: "Ι",
    },
    TitleMapping {
        code: 0x371,
        replacement: "Ͱ",
    },
    TitleMapping {
        code: 0x373,
        replacement: "Ͳ",
    },
    TitleMapping {
        code: 0x377,
        replacement: "Ͷ",
    },
    TitleMapping {
        code: 0x37b,
        replacement: "Ͻ",
    },
    TitleMapping {
        code: 0x37c,
        replacement: "Ͼ",
    },
    TitleMapping {
        code: 0x37d,
        replacement: "Ͽ",
    },
    TitleMapping {
        code: 0x390,
        replacement: "\u{399}\u{308}\u{301}",
    },
    TitleMapping {
        code: 0x3ac,
        replacement: "Ά",
    },
    TitleMapping {
        code: 0x3ad,
        replacement: "Έ",
    },
    TitleMapping {
        code: 0x3ae,
        replacement: "Ή",
    },
    TitleMapping {
        code: 0x3af,
        replacement: "Ί",
    },
    TitleMapping {
        code: 0x3b0,
        replacement: "\u{3a5}\u{308}\u{301}",
    },
    TitleMapping {
        code: 0x3b1,
        replacement: "Α",
    },
    TitleMapping {
        code: 0x3b2,
        replacement: "Β",
    },
    TitleMapping {
        code: 0x3b3,
        replacement: "Γ",
    },
    TitleMapping {
        code: 0x3b4,
        replacement: "Δ",
    },
    TitleMapping {
        code: 0x3b5,
        replacement: "Ε",
    },
    TitleMapping {
        code: 0x3b6,
        replacement: "Ζ",
    },
    TitleMapping {
        code: 0x3b7,
        replacement: "Η",
    },
    TitleMapping {
        code: 0x3b8,
        replacement: "Θ",
    },
    TitleMapping {
        code: 0x3b9,
        replacement: "Ι",
    },
    TitleMapping {
        code: 0x3ba,
        replacement: "Κ",
    },
    TitleMapping {
        code: 0x3bb,
        replacement: "Λ",
    },
    TitleMapping {
        code: 0x3bc,
        replacement: "Μ",
    },
    TitleMapping {
        code: 0x3bd,
        replacement: "Ν",
    },
    TitleMapping {
        code: 0x3be,
        replacement: "Ξ",
    },
    TitleMapping {
        code: 0x3bf,
        replacement: "Ο",
    },
    TitleMapping {
        code: 0x3c0,
        replacement: "Π",
    },
    TitleMapping {
        code: 0x3c1,
        replacement: "Ρ",
    },
    TitleMapping {
        code: 0x3c2,
        replacement: "Σ",
    },
    TitleMapping {
        code: 0x3c3,
        replacement: "Σ",
    },
    TitleMapping {
        code: 0x3c4,
        replacement: "Τ",
    },
    TitleMapping {
        code: 0x3c5,
        replacement: "Υ",
    },
    TitleMapping {
        code: 0x3c6,
        replacement: "Φ",
    },
    TitleMapping {
        code: 0x3c7,
        replacement: "Χ",
    },
    TitleMapping {
        code: 0x3c8,
        replacement: "Ψ",
    },
    TitleMapping {
        code: 0x3c9,
        replacement: "Ω",
    },
    TitleMapping {
        code: 0x3ca,
        replacement: "Ϊ",
    },
    TitleMapping {
        code: 0x3cb,
        replacement: "Ϋ",
    },
    TitleMapping {
        code: 0x3cc,
        replacement: "Ό",
    },
    TitleMapping {
        code: 0x3cd,
        replacement: "Ύ",
    },
    TitleMapping {
        code: 0x3ce,
        replacement: "Ώ",
    },
    TitleMapping {
        code: 0x3d0,
        replacement: "Β",
    },
    TitleMapping {
        code: 0x3d1,
        replacement: "Θ",
    },
    TitleMapping {
        code: 0x3d5,
        replacement: "Φ",
    },
    TitleMapping {
        code: 0x3d6,
        replacement: "Π",
    },
    TitleMapping {
        code: 0x3d7,
        replacement: "Ϗ",
    },
    TitleMapping {
        code: 0x3d9,
        replacement: "Ϙ",
    },
    TitleMapping {
        code: 0x3db,
        replacement: "Ϛ",
    },
    TitleMapping {
        code: 0x3dd,
        replacement: "Ϝ",
    },
    TitleMapping {
        code: 0x3df,
        replacement: "Ϟ",
    },
    TitleMapping {
        code: 0x3e1,
        replacement: "Ϡ",
    },
    TitleMapping {
        code: 0x3e3,
        replacement: "Ϣ",
    },
    TitleMapping {
        code: 0x3e5,
        replacement: "Ϥ",
    },
    TitleMapping {
        code: 0x3e7,
        replacement: "Ϧ",
    },
    TitleMapping {
        code: 0x3e9,
        replacement: "Ϩ",
    },
    TitleMapping {
        code: 0x3eb,
        replacement: "Ϫ",
    },
    TitleMapping {
        code: 0x3ed,
        replacement: "Ϭ",
    },
    TitleMapping {
        code: 0x3ef,
        replacement: "Ϯ",
    },
    TitleMapping {
        code: 0x3f0,
        replacement: "Κ",
    },
    TitleMapping {
        code: 0x3f1,
        replacement: "Ρ",
    },
    TitleMapping {
        code: 0x3f2,
        replacement: "Ϲ",
    },
    TitleMapping {
        code: 0x3f3,
        replacement: "Ϳ",
    },
    TitleMapping {
        code: 0x3f5,
        replacement: "Ε",
    },
    TitleMapping {
        code: 0x3f8,
        replacement: "Ϸ",
    },
    TitleMapping {
        code: 0x3fb,
        replacement: "Ϻ",
    },
    TitleMapping {
        code: 0x430,
        replacement: "А",
    },
    TitleMapping {
        code: 0x431,
        replacement: "Б",
    },
    TitleMapping {
        code: 0x432,
        replacement: "В",
    },
    TitleMapping {
        code: 0x433,
        replacement: "Г",
    },
    TitleMapping {
        code: 0x434,
        replacement: "Д",
    },
    TitleMapping {
        code: 0x435,
        replacement: "Е",
    },
    TitleMapping {
        code: 0x436,
        replacement: "Ж",
    },
    TitleMapping {
        code: 0x437,
        replacement: "З",
    },
    TitleMapping {
        code: 0x438,
        replacement: "И",
    },
    TitleMapping {
        code: 0x439,
        replacement: "Й",
    },
    TitleMapping {
        code: 0x43a,
        replacement: "К",
    },
    TitleMapping {
        code: 0x43b,
        replacement: "Л",
    },
    TitleMapping {
        code: 0x43c,
        replacement: "М",
    },
    TitleMapping {
        code: 0x43d,
        replacement: "Н",
    },
    TitleMapping {
        code: 0x43e,
        replacement: "О",
    },
    TitleMapping {
        code: 0x43f,
        replacement: "П",
    },
    TitleMapping {
        code: 0x440,
        replacement: "Р",
    },
    TitleMapping {
        code: 0x441,
        replacement: "С",
    },
    TitleMapping {
        code: 0x442,
        replacement: "Т",
    },
    TitleMapping {
        code: 0x443,
        replacement: "У",
    },
    TitleMapping {
        code: 0x444,
        replacement: "Ф",
    },
    TitleMapping {
        code: 0x445,
        replacement: "Х",
    },
    TitleMapping {
        code: 0x446,
        replacement: "Ц",
    },
    TitleMapping {
        code: 0x447,
        replacement: "Ч",
    },
    TitleMapping {
        code: 0x448,
        replacement: "Ш",
    },
    TitleMapping {
        code: 0x449,
        replacement: "Щ",
    },
    TitleMapping {
        code: 0x44a,
        replacement: "Ъ",
    },
    TitleMapping {
        code: 0x44b,
        replacement: "Ы",
    },
    TitleMapping {
        code: 0x44c,
        replacement: "Ь",
    },
    TitleMapping {
        code: 0x44d,
        replacement: "Э",
    },
    TitleMapping {
        code: 0x44e,
        replacement: "Ю",
    },
    TitleMapping {
        code: 0x44f,
        replacement: "Я",
    },
    TitleMapping {
        code: 0x450,
        replacement: "Ѐ",
    },
    TitleMapping {
        code: 0x451,
        replacement: "Ё",
    },
    TitleMapping {
        code: 0x452,
        replacement: "Ђ",
    },
    TitleMapping {
        code: 0x453,
        replacement: "Ѓ",
    },
    TitleMapping {
        code: 0x454,
        replacement: "Є",
    },
    TitleMapping {
        code: 0x455,
        replacement: "Ѕ",
    },
    TitleMapping {
        code: 0x456,
        replacement: "І",
    },
    TitleMapping {
        code: 0x457,
        replacement: "Ї",
    },
    TitleMapping {
        code: 0x458,
        replacement: "Ј",
    },
    TitleMapping {
        code: 0x459,
        replacement: "Љ",
    },
    TitleMapping {
        code: 0x45a,
        replacement: "Њ",
    },
    TitleMapping {
        code: 0x45b,
        replacement: "Ћ",
    },
    TitleMapping {
        code: 0x45c,
        replacement: "Ќ",
    },
    TitleMapping {
        code: 0x45d,
        replacement: "Ѝ",
    },
    TitleMapping {
        code: 0x45e,
        replacement: "Ў",
    },
    TitleMapping {
        code: 0x45f,
        replacement: "Џ",
    },
    TitleMapping {
        code: 0x461,
        replacement: "Ѡ",
    },
    TitleMapping {
        code: 0x463,
        replacement: "Ѣ",
    },
    TitleMapping {
        code: 0x465,
        replacement: "Ѥ",
    },
    TitleMapping {
        code: 0x467,
        replacement: "Ѧ",
    },
    TitleMapping {
        code: 0x469,
        replacement: "Ѩ",
    },
    TitleMapping {
        code: 0x46b,
        replacement: "Ѫ",
    },
    TitleMapping {
        code: 0x46d,
        replacement: "Ѭ",
    },
    TitleMapping {
        code: 0x46f,
        replacement: "Ѯ",
    },
    TitleMapping {
        code: 0x471,
        replacement: "Ѱ",
    },
    TitleMapping {
        code: 0x473,
        replacement: "Ѳ",
    },
    TitleMapping {
        code: 0x475,
        replacement: "Ѵ",
    },
    TitleMapping {
        code: 0x477,
        replacement: "Ѷ",
    },
    TitleMapping {
        code: 0x479,
        replacement: "Ѹ",
    },
    TitleMapping {
        code: 0x47b,
        replacement: "Ѻ",
    },
    TitleMapping {
        code: 0x47d,
        replacement: "Ѽ",
    },
    TitleMapping {
        code: 0x47f,
        replacement: "Ѿ",
    },
    TitleMapping {
        code: 0x481,
        replacement: "Ҁ",
    },
    TitleMapping {
        code: 0x48b,
        replacement: "Ҋ",
    },
    TitleMapping {
        code: 0x48d,
        replacement: "Ҍ",
    },
    TitleMapping {
        code: 0x48f,
        replacement: "Ҏ",
    },
    TitleMapping {
        code: 0x491,
        replacement: "Ґ",
    },
    TitleMapping {
        code: 0x493,
        replacement: "Ғ",
    },
    TitleMapping {
        code: 0x495,
        replacement: "Ҕ",
    },
    TitleMapping {
        code: 0x497,
        replacement: "Җ",
    },
    TitleMapping {
        code: 0x499,
        replacement: "Ҙ",
    },
    TitleMapping {
        code: 0x49b,
        replacement: "Қ",
    },
    TitleMapping {
        code: 0x49d,
        replacement: "Ҝ",
    },
    TitleMapping {
        code: 0x49f,
        replacement: "Ҟ",
    },
    TitleMapping {
        code: 0x4a1,
        replacement: "Ҡ",
    },
    TitleMapping {
        code: 0x4a3,
        replacement: "Ң",
    },
    TitleMapping {
        code: 0x4a5,
        replacement: "Ҥ",
    },
    TitleMapping {
        code: 0x4a7,
        replacement: "Ҧ",
    },
    TitleMapping {
        code: 0x4a9,
        replacement: "Ҩ",
    },
    TitleMapping {
        code: 0x4ab,
        replacement: "Ҫ",
    },
    TitleMapping {
        code: 0x4ad,
        replacement: "Ҭ",
    },
    TitleMapping {
        code: 0x4af,
        replacement: "Ү",
    },
    TitleMapping {
        code: 0x4b1,
        replacement: "Ұ",
    },
    TitleMapping {
        code: 0x4b3,
        replacement: "Ҳ",
    },
    TitleMapping {
        code: 0x4b5,
        replacement: "Ҵ",
    },
    TitleMapping {
        code: 0x4b7,
        replacement: "Ҷ",
    },
    TitleMapping {
        code: 0x4b9,
        replacement: "Ҹ",
    },
    TitleMapping {
        code: 0x4bb,
        replacement: "Һ",
    },
    TitleMapping {
        code: 0x4bd,
        replacement: "Ҽ",
    },
    TitleMapping {
        code: 0x4bf,
        replacement: "Ҿ",
    },
    TitleMapping {
        code: 0x4c2,
        replacement: "Ӂ",
    },
    TitleMapping {
        code: 0x4c4,
        replacement: "Ӄ",
    },
    TitleMapping {
        code: 0x4c6,
        replacement: "Ӆ",
    },
    TitleMapping {
        code: 0x4c8,
        replacement: "Ӈ",
    },
    TitleMapping {
        code: 0x4ca,
        replacement: "Ӊ",
    },
    TitleMapping {
        code: 0x4cc,
        replacement: "Ӌ",
    },
    TitleMapping {
        code: 0x4ce,
        replacement: "Ӎ",
    },
    TitleMapping {
        code: 0x4cf,
        replacement: "Ӏ",
    },
    TitleMapping {
        code: 0x4d1,
        replacement: "Ӑ",
    },
    TitleMapping {
        code: 0x4d3,
        replacement: "Ӓ",
    },
    TitleMapping {
        code: 0x4d5,
        replacement: "Ӕ",
    },
    TitleMapping {
        code: 0x4d7,
        replacement: "Ӗ",
    },
    TitleMapping {
        code: 0x4d9,
        replacement: "Ә",
    },
    TitleMapping {
        code: 0x4db,
        replacement: "Ӛ",
    },
    TitleMapping {
        code: 0x4dd,
        replacement: "Ӝ",
    },
    TitleMapping {
        code: 0x4df,
        replacement: "Ӟ",
    },
    TitleMapping {
        code: 0x4e1,
        replacement: "Ӡ",
    },
    TitleMapping {
        code: 0x4e3,
        replacement: "Ӣ",
    },
    TitleMapping {
        code: 0x4e5,
        replacement: "Ӥ",
    },
    TitleMapping {
        code: 0x4e7,
        replacement: "Ӧ",
    },
    TitleMapping {
        code: 0x4e9,
        replacement: "Ө",
    },
    TitleMapping {
        code: 0x4eb,
        replacement: "Ӫ",
    },
    TitleMapping {
        code: 0x4ed,
        replacement: "Ӭ",
    },
    TitleMapping {
        code: 0x4ef,
        replacement: "Ӯ",
    },
    TitleMapping {
        code: 0x4f1,
        replacement: "Ӱ",
    },
    TitleMapping {
        code: 0x4f3,
        replacement: "Ӳ",
    },
    TitleMapping {
        code: 0x4f5,
        replacement: "Ӵ",
    },
    TitleMapping {
        code: 0x4f7,
        replacement: "Ӷ",
    },
    TitleMapping {
        code: 0x4f9,
        replacement: "Ӹ",
    },
    TitleMapping {
        code: 0x4fb,
        replacement: "Ӻ",
    },
    TitleMapping {
        code: 0x4fd,
        replacement: "Ӽ",
    },
    TitleMapping {
        code: 0x4ff,
        replacement: "Ӿ",
    },
    TitleMapping {
        code: 0x501,
        replacement: "Ԁ",
    },
    TitleMapping {
        code: 0x503,
        replacement: "Ԃ",
    },
    TitleMapping {
        code: 0x505,
        replacement: "Ԅ",
    },
    TitleMapping {
        code: 0x507,
        replacement: "Ԇ",
    },
    TitleMapping {
        code: 0x509,
        replacement: "Ԉ",
    },
    TitleMapping {
        code: 0x50b,
        replacement: "Ԋ",
    },
    TitleMapping {
        code: 0x50d,
        replacement: "Ԍ",
    },
    TitleMapping {
        code: 0x50f,
        replacement: "Ԏ",
    },
    TitleMapping {
        code: 0x511,
        replacement: "Ԑ",
    },
    TitleMapping {
        code: 0x513,
        replacement: "Ԓ",
    },
    TitleMapping {
        code: 0x515,
        replacement: "Ԕ",
    },
    TitleMapping {
        code: 0x517,
        replacement: "Ԗ",
    },
    TitleMapping {
        code: 0x519,
        replacement: "Ԙ",
    },
    TitleMapping {
        code: 0x51b,
        replacement: "Ԛ",
    },
    TitleMapping {
        code: 0x51d,
        replacement: "Ԝ",
    },
    TitleMapping {
        code: 0x51f,
        replacement: "Ԟ",
    },
    TitleMapping {
        code: 0x521,
        replacement: "Ԡ",
    },
    TitleMapping {
        code: 0x523,
        replacement: "Ԣ",
    },
    TitleMapping {
        code: 0x525,
        replacement: "Ԥ",
    },
    TitleMapping {
        code: 0x527,
        replacement: "Ԧ",
    },
    TitleMapping {
        code: 0x529,
        replacement: "Ԩ",
    },
    TitleMapping {
        code: 0x52b,
        replacement: "Ԫ",
    },
    TitleMapping {
        code: 0x52d,
        replacement: "Ԭ",
    },
    TitleMapping {
        code: 0x52f,
        replacement: "Ԯ",
    },
    TitleMapping {
        code: 0x561,
        replacement: "Ա",
    },
    TitleMapping {
        code: 0x562,
        replacement: "Բ",
    },
    TitleMapping {
        code: 0x563,
        replacement: "Գ",
    },
    TitleMapping {
        code: 0x564,
        replacement: "Դ",
    },
    TitleMapping {
        code: 0x565,
        replacement: "Ե",
    },
    TitleMapping {
        code: 0x566,
        replacement: "Զ",
    },
    TitleMapping {
        code: 0x567,
        replacement: "Է",
    },
    TitleMapping {
        code: 0x568,
        replacement: "Ը",
    },
    TitleMapping {
        code: 0x569,
        replacement: "Թ",
    },
    TitleMapping {
        code: 0x56a,
        replacement: "Ժ",
    },
    TitleMapping {
        code: 0x56b,
        replacement: "Ի",
    },
    TitleMapping {
        code: 0x56c,
        replacement: "Լ",
    },
    TitleMapping {
        code: 0x56d,
        replacement: "Խ",
    },
    TitleMapping {
        code: 0x56e,
        replacement: "Ծ",
    },
    TitleMapping {
        code: 0x56f,
        replacement: "Կ",
    },
    TitleMapping {
        code: 0x570,
        replacement: "Հ",
    },
    TitleMapping {
        code: 0x571,
        replacement: "Ձ",
    },
    TitleMapping {
        code: 0x572,
        replacement: "Ղ",
    },
    TitleMapping {
        code: 0x573,
        replacement: "Ճ",
    },
    TitleMapping {
        code: 0x574,
        replacement: "Մ",
    },
    TitleMapping {
        code: 0x575,
        replacement: "Յ",
    },
    TitleMapping {
        code: 0x576,
        replacement: "Ն",
    },
    TitleMapping {
        code: 0x577,
        replacement: "Շ",
    },
    TitleMapping {
        code: 0x578,
        replacement: "Ո",
    },
    TitleMapping {
        code: 0x579,
        replacement: "Չ",
    },
    TitleMapping {
        code: 0x57a,
        replacement: "Պ",
    },
    TitleMapping {
        code: 0x57b,
        replacement: "Ջ",
    },
    TitleMapping {
        code: 0x57c,
        replacement: "Ռ",
    },
    TitleMapping {
        code: 0x57d,
        replacement: "Ս",
    },
    TitleMapping {
        code: 0x57e,
        replacement: "Վ",
    },
    TitleMapping {
        code: 0x57f,
        replacement: "Տ",
    },
    TitleMapping {
        code: 0x580,
        replacement: "Ր",
    },
    TitleMapping {
        code: 0x581,
        replacement: "Ց",
    },
    TitleMapping {
        code: 0x582,
        replacement: "Ւ",
    },
    TitleMapping {
        code: 0x583,
        replacement: "Փ",
    },
    TitleMapping {
        code: 0x584,
        replacement: "Ք",
    },
    TitleMapping {
        code: 0x585,
        replacement: "Օ",
    },
    TitleMapping {
        code: 0x586,
        replacement: "Ֆ",
    },
    TitleMapping {
        code: 0x587,
        replacement: "Եւ",
    },
    TitleMapping {
        code: 0x13f8,
        replacement: "Ᏸ",
    },
    TitleMapping {
        code: 0x13f9,
        replacement: "Ᏹ",
    },
    TitleMapping {
        code: 0x13fa,
        replacement: "Ᏺ",
    },
    TitleMapping {
        code: 0x13fb,
        replacement: "Ᏻ",
    },
    TitleMapping {
        code: 0x13fc,
        replacement: "Ᏼ",
    },
    TitleMapping {
        code: 0x13fd,
        replacement: "Ᏽ",
    },
    TitleMapping {
        code: 0x1c80,
        replacement: "В",
    },
    TitleMapping {
        code: 0x1c81,
        replacement: "Д",
    },
    TitleMapping {
        code: 0x1c82,
        replacement: "О",
    },
    TitleMapping {
        code: 0x1c83,
        replacement: "С",
    },
    TitleMapping {
        code: 0x1c84,
        replacement: "Т",
    },
    TitleMapping {
        code: 0x1c85,
        replacement: "Т",
    },
    TitleMapping {
        code: 0x1c86,
        replacement: "Ъ",
    },
    TitleMapping {
        code: 0x1c87,
        replacement: "Ѣ",
    },
    TitleMapping {
        code: 0x1c88,
        replacement: "Ꙋ",
    },
    TitleMapping {
        code: 0x1d79,
        replacement: "Ᵹ",
    },
    TitleMapping {
        code: 0x1d7d,
        replacement: "Ᵽ",
    },
    TitleMapping {
        code: 0x1d8e,
        replacement: "Ᶎ",
    },
    TitleMapping {
        code: 0x1e01,
        replacement: "Ḁ",
    },
    TitleMapping {
        code: 0x1e03,
        replacement: "Ḃ",
    },
    TitleMapping {
        code: 0x1e05,
        replacement: "Ḅ",
    },
    TitleMapping {
        code: 0x1e07,
        replacement: "Ḇ",
    },
    TitleMapping {
        code: 0x1e09,
        replacement: "Ḉ",
    },
    TitleMapping {
        code: 0x1e0b,
        replacement: "Ḋ",
    },
    TitleMapping {
        code: 0x1e0d,
        replacement: "Ḍ",
    },
    TitleMapping {
        code: 0x1e0f,
        replacement: "Ḏ",
    },
    TitleMapping {
        code: 0x1e11,
        replacement: "Ḑ",
    },
    TitleMapping {
        code: 0x1e13,
        replacement: "Ḓ",
    },
    TitleMapping {
        code: 0x1e15,
        replacement: "Ḕ",
    },
    TitleMapping {
        code: 0x1e17,
        replacement: "Ḗ",
    },
    TitleMapping {
        code: 0x1e19,
        replacement: "Ḙ",
    },
    TitleMapping {
        code: 0x1e1b,
        replacement: "Ḛ",
    },
    TitleMapping {
        code: 0x1e1d,
        replacement: "Ḝ",
    },
    TitleMapping {
        code: 0x1e1f,
        replacement: "Ḟ",
    },
    TitleMapping {
        code: 0x1e21,
        replacement: "Ḡ",
    },
    TitleMapping {
        code: 0x1e23,
        replacement: "Ḣ",
    },
    TitleMapping {
        code: 0x1e25,
        replacement: "Ḥ",
    },
    TitleMapping {
        code: 0x1e27,
        replacement: "Ḧ",
    },
    TitleMapping {
        code: 0x1e29,
        replacement: "Ḩ",
    },
    TitleMapping {
        code: 0x1e2b,
        replacement: "Ḫ",
    },
    TitleMapping {
        code: 0x1e2d,
        replacement: "Ḭ",
    },
    TitleMapping {
        code: 0x1e2f,
        replacement: "Ḯ",
    },
    TitleMapping {
        code: 0x1e31,
        replacement: "Ḱ",
    },
    TitleMapping {
        code: 0x1e33,
        replacement: "Ḳ",
    },
    TitleMapping {
        code: 0x1e35,
        replacement: "Ḵ",
    },
    TitleMapping {
        code: 0x1e37,
        replacement: "Ḷ",
    },
    TitleMapping {
        code: 0x1e39,
        replacement: "Ḹ",
    },
    TitleMapping {
        code: 0x1e3b,
        replacement: "Ḻ",
    },
    TitleMapping {
        code: 0x1e3d,
        replacement: "Ḽ",
    },
    TitleMapping {
        code: 0x1e3f,
        replacement: "Ḿ",
    },
    TitleMapping {
        code: 0x1e41,
        replacement: "Ṁ",
    },
    TitleMapping {
        code: 0x1e43,
        replacement: "Ṃ",
    },
    TitleMapping {
        code: 0x1e45,
        replacement: "Ṅ",
    },
    TitleMapping {
        code: 0x1e47,
        replacement: "Ṇ",
    },
    TitleMapping {
        code: 0x1e49,
        replacement: "Ṉ",
    },
    TitleMapping {
        code: 0x1e4b,
        replacement: "Ṋ",
    },
    TitleMapping {
        code: 0x1e4d,
        replacement: "Ṍ",
    },
    TitleMapping {
        code: 0x1e4f,
        replacement: "Ṏ",
    },
    TitleMapping {
        code: 0x1e51,
        replacement: "Ṑ",
    },
    TitleMapping {
        code: 0x1e53,
        replacement: "Ṓ",
    },
    TitleMapping {
        code: 0x1e55,
        replacement: "Ṕ",
    },
    TitleMapping {
        code: 0x1e57,
        replacement: "Ṗ",
    },
    TitleMapping {
        code: 0x1e59,
        replacement: "Ṙ",
    },
    TitleMapping {
        code: 0x1e5b,
        replacement: "Ṛ",
    },
    TitleMapping {
        code: 0x1e5d,
        replacement: "Ṝ",
    },
    TitleMapping {
        code: 0x1e5f,
        replacement: "Ṟ",
    },
    TitleMapping {
        code: 0x1e61,
        replacement: "Ṡ",
    },
    TitleMapping {
        code: 0x1e63,
        replacement: "Ṣ",
    },
    TitleMapping {
        code: 0x1e65,
        replacement: "Ṥ",
    },
    TitleMapping {
        code: 0x1e67,
        replacement: "Ṧ",
    },
    TitleMapping {
        code: 0x1e69,
        replacement: "Ṩ",
    },
    TitleMapping {
        code: 0x1e6b,
        replacement: "Ṫ",
    },
    TitleMapping {
        code: 0x1e6d,
        replacement: "Ṭ",
    },
    TitleMapping {
        code: 0x1e6f,
        replacement: "Ṯ",
    },
    TitleMapping {
        code: 0x1e71,
        replacement: "Ṱ",
    },
    TitleMapping {
        code: 0x1e73,
        replacement: "Ṳ",
    },
    TitleMapping {
        code: 0x1e75,
        replacement: "Ṵ",
    },
    TitleMapping {
        code: 0x1e77,
        replacement: "Ṷ",
    },
    TitleMapping {
        code: 0x1e79,
        replacement: "Ṹ",
    },
    TitleMapping {
        code: 0x1e7b,
        replacement: "Ṻ",
    },
    TitleMapping {
        code: 0x1e7d,
        replacement: "Ṽ",
    },
    TitleMapping {
        code: 0x1e7f,
        replacement: "Ṿ",
    },
    TitleMapping {
        code: 0x1e81,
        replacement: "Ẁ",
    },
    TitleMapping {
        code: 0x1e83,
        replacement: "Ẃ",
    },
    TitleMapping {
        code: 0x1e85,
        replacement: "Ẅ",
    },
    TitleMapping {
        code: 0x1e87,
        replacement: "Ẇ",
    },
    TitleMapping {
        code: 0x1e89,
        replacement: "Ẉ",
    },
    TitleMapping {
        code: 0x1e8b,
        replacement: "Ẋ",
    },
    TitleMapping {
        code: 0x1e8d,
        replacement: "Ẍ",
    },
    TitleMapping {
        code: 0x1e8f,
        replacement: "Ẏ",
    },
    TitleMapping {
        code: 0x1e91,
        replacement: "Ẑ",
    },
    TitleMapping {
        code: 0x1e93,
        replacement: "Ẓ",
    },
    TitleMapping {
        code: 0x1e95,
        replacement: "Ẕ",
    },
    TitleMapping {
        code: 0x1e96,
        replacement: "H̱",
    },
    TitleMapping {
        code: 0x1e97,
        replacement: "T̈",
    },
    TitleMapping {
        code: 0x1e98,
        replacement: "W̊",
    },
    TitleMapping {
        code: 0x1e99,
        replacement: "Y̊",
    },
    TitleMapping {
        code: 0x1e9a,
        replacement: "Aʾ",
    },
    TitleMapping {
        code: 0x1e9b,
        replacement: "Ṡ",
    },
    TitleMapping {
        code: 0x1ea1,
        replacement: "Ạ",
    },
    TitleMapping {
        code: 0x1ea3,
        replacement: "Ả",
    },
    TitleMapping {
        code: 0x1ea5,
        replacement: "Ấ",
    },
    TitleMapping {
        code: 0x1ea7,
        replacement: "Ầ",
    },
    TitleMapping {
        code: 0x1ea9,
        replacement: "Ẩ",
    },
    TitleMapping {
        code: 0x1eab,
        replacement: "Ẫ",
    },
    TitleMapping {
        code: 0x1ead,
        replacement: "Ậ",
    },
    TitleMapping {
        code: 0x1eaf,
        replacement: "Ắ",
    },
    TitleMapping {
        code: 0x1eb1,
        replacement: "Ằ",
    },
    TitleMapping {
        code: 0x1eb3,
        replacement: "Ẳ",
    },
    TitleMapping {
        code: 0x1eb5,
        replacement: "Ẵ",
    },
    TitleMapping {
        code: 0x1eb7,
        replacement: "Ặ",
    },
    TitleMapping {
        code: 0x1eb9,
        replacement: "Ẹ",
    },
    TitleMapping {
        code: 0x1ebb,
        replacement: "Ẻ",
    },
    TitleMapping {
        code: 0x1ebd,
        replacement: "Ẽ",
    },
    TitleMapping {
        code: 0x1ebf,
        replacement: "Ế",
    },
    TitleMapping {
        code: 0x1ec1,
        replacement: "Ề",
    },
    TitleMapping {
        code: 0x1ec3,
        replacement: "Ể",
    },
    TitleMapping {
        code: 0x1ec5,
        replacement: "Ễ",
    },
    TitleMapping {
        code: 0x1ec7,
        replacement: "Ệ",
    },
    TitleMapping {
        code: 0x1ec9,
        replacement: "Ỉ",
    },
    TitleMapping {
        code: 0x1ecb,
        replacement: "Ị",
    },
    TitleMapping {
        code: 0x1ecd,
        replacement: "Ọ",
    },
    TitleMapping {
        code: 0x1ecf,
        replacement: "Ỏ",
    },
    TitleMapping {
        code: 0x1ed1,
        replacement: "Ố",
    },
    TitleMapping {
        code: 0x1ed3,
        replacement: "Ồ",
    },
    TitleMapping {
        code: 0x1ed5,
        replacement: "Ổ",
    },
    TitleMapping {
        code: 0x1ed7,
        replacement: "Ỗ",
    },
    TitleMapping {
        code: 0x1ed9,
        replacement: "Ộ",
    },
    TitleMapping {
        code: 0x1edb,
        replacement: "Ớ",
    },
    TitleMapping {
        code: 0x1edd,
        replacement: "Ờ",
    },
    TitleMapping {
        code: 0x1edf,
        replacement: "Ở",
    },
    TitleMapping {
        code: 0x1ee1,
        replacement: "Ỡ",
    },
    TitleMapping {
        code: 0x1ee3,
        replacement: "Ợ",
    },
    TitleMapping {
        code: 0x1ee5,
        replacement: "Ụ",
    },
    TitleMapping {
        code: 0x1ee7,
        replacement: "Ủ",
    },
    TitleMapping {
        code: 0x1ee9,
        replacement: "Ứ",
    },
    TitleMapping {
        code: 0x1eeb,
        replacement: "Ừ",
    },
    TitleMapping {
        code: 0x1eed,
        replacement: "Ử",
    },
    TitleMapping {
        code: 0x1eef,
        replacement: "Ữ",
    },
    TitleMapping {
        code: 0x1ef1,
        replacement: "Ự",
    },
    TitleMapping {
        code: 0x1ef3,
        replacement: "Ỳ",
    },
    TitleMapping {
        code: 0x1ef5,
        replacement: "Ỵ",
    },
    TitleMapping {
        code: 0x1ef7,
        replacement: "Ỷ",
    },
    TitleMapping {
        code: 0x1ef9,
        replacement: "Ỹ",
    },
    TitleMapping {
        code: 0x1efb,
        replacement: "Ỻ",
    },
    TitleMapping {
        code: 0x1efd,
        replacement: "Ỽ",
    },
    TitleMapping {
        code: 0x1eff,
        replacement: "Ỿ",
    },
    TitleMapping {
        code: 0x1f00,
        replacement: "Ἀ",
    },
    TitleMapping {
        code: 0x1f01,
        replacement: "Ἁ",
    },
    TitleMapping {
        code: 0x1f02,
        replacement: "Ἂ",
    },
    TitleMapping {
        code: 0x1f03,
        replacement: "Ἃ",
    },
    TitleMapping {
        code: 0x1f04,
        replacement: "Ἄ",
    },
    TitleMapping {
        code: 0x1f05,
        replacement: "Ἅ",
    },
    TitleMapping {
        code: 0x1f06,
        replacement: "Ἆ",
    },
    TitleMapping {
        code: 0x1f07,
        replacement: "Ἇ",
    },
    TitleMapping {
        code: 0x1f10,
        replacement: "Ἐ",
    },
    TitleMapping {
        code: 0x1f11,
        replacement: "Ἑ",
    },
    TitleMapping {
        code: 0x1f12,
        replacement: "Ἒ",
    },
    TitleMapping {
        code: 0x1f13,
        replacement: "Ἓ",
    },
    TitleMapping {
        code: 0x1f14,
        replacement: "Ἔ",
    },
    TitleMapping {
        code: 0x1f15,
        replacement: "Ἕ",
    },
    TitleMapping {
        code: 0x1f20,
        replacement: "Ἠ",
    },
    TitleMapping {
        code: 0x1f21,
        replacement: "Ἡ",
    },
    TitleMapping {
        code: 0x1f22,
        replacement: "Ἢ",
    },
    TitleMapping {
        code: 0x1f23,
        replacement: "Ἣ",
    },
    TitleMapping {
        code: 0x1f24,
        replacement: "Ἤ",
    },
    TitleMapping {
        code: 0x1f25,
        replacement: "Ἥ",
    },
    TitleMapping {
        code: 0x1f26,
        replacement: "Ἦ",
    },
    TitleMapping {
        code: 0x1f27,
        replacement: "Ἧ",
    },
    TitleMapping {
        code: 0x1f30,
        replacement: "Ἰ",
    },
    TitleMapping {
        code: 0x1f31,
        replacement: "Ἱ",
    },
    TitleMapping {
        code: 0x1f32,
        replacement: "Ἲ",
    },
    TitleMapping {
        code: 0x1f33,
        replacement: "Ἳ",
    },
    TitleMapping {
        code: 0x1f34,
        replacement: "Ἴ",
    },
    TitleMapping {
        code: 0x1f35,
        replacement: "Ἵ",
    },
    TitleMapping {
        code: 0x1f36,
        replacement: "Ἶ",
    },
    TitleMapping {
        code: 0x1f37,
        replacement: "Ἷ",
    },
    TitleMapping {
        code: 0x1f40,
        replacement: "Ὀ",
    },
    TitleMapping {
        code: 0x1f41,
        replacement: "Ὁ",
    },
    TitleMapping {
        code: 0x1f42,
        replacement: "Ὂ",
    },
    TitleMapping {
        code: 0x1f43,
        replacement: "Ὃ",
    },
    TitleMapping {
        code: 0x1f44,
        replacement: "Ὄ",
    },
    TitleMapping {
        code: 0x1f45,
        replacement: "Ὅ",
    },
    TitleMapping {
        code: 0x1f50,
        replacement: "Υ̓",
    },
    TitleMapping {
        code: 0x1f51,
        replacement: "Ὑ",
    },
    TitleMapping {
        code: 0x1f52,
        replacement: "Υ̓̀",
    },
    TitleMapping {
        code: 0x1f53,
        replacement: "Ὓ",
    },
    TitleMapping {
        code: 0x1f54,
        replacement: "Υ̓́",
    },
    TitleMapping {
        code: 0x1f55,
        replacement: "Ὕ",
    },
    TitleMapping {
        code: 0x1f56,
        replacement: "Υ̓͂",
    },
    TitleMapping {
        code: 0x1f57,
        replacement: "Ὗ",
    },
    TitleMapping {
        code: 0x1f60,
        replacement: "Ὠ",
    },
    TitleMapping {
        code: 0x1f61,
        replacement: "Ὡ",
    },
    TitleMapping {
        code: 0x1f62,
        replacement: "Ὢ",
    },
    TitleMapping {
        code: 0x1f63,
        replacement: "Ὣ",
    },
    TitleMapping {
        code: 0x1f64,
        replacement: "Ὤ",
    },
    TitleMapping {
        code: 0x1f65,
        replacement: "Ὥ",
    },
    TitleMapping {
        code: 0x1f66,
        replacement: "Ὦ",
    },
    TitleMapping {
        code: 0x1f67,
        replacement: "Ὧ",
    },
    TitleMapping {
        code: 0x1f70,
        replacement: "Ὰ",
    },
    TitleMapping {
        code: 0x1f71,
        replacement: "\u{1fbb}",
    },
    TitleMapping {
        code: 0x1f72,
        replacement: "Ὲ",
    },
    TitleMapping {
        code: 0x1f73,
        replacement: "\u{1fc9}",
    },
    TitleMapping {
        code: 0x1f74,
        replacement: "Ὴ",
    },
    TitleMapping {
        code: 0x1f75,
        replacement: "\u{1fcb}",
    },
    TitleMapping {
        code: 0x1f76,
        replacement: "Ὶ",
    },
    TitleMapping {
        code: 0x1f77,
        replacement: "\u{1fdb}",
    },
    TitleMapping {
        code: 0x1f78,
        replacement: "Ὸ",
    },
    TitleMapping {
        code: 0x1f79,
        replacement: "\u{1ff9}",
    },
    TitleMapping {
        code: 0x1f7a,
        replacement: "Ὺ",
    },
    TitleMapping {
        code: 0x1f7b,
        replacement: "\u{1feb}",
    },
    TitleMapping {
        code: 0x1f7c,
        replacement: "Ὼ",
    },
    TitleMapping {
        code: 0x1f7d,
        replacement: "\u{1ffb}",
    },
    TitleMapping {
        code: 0x1f80,
        replacement: "ᾈ",
    },
    TitleMapping {
        code: 0x1f81,
        replacement: "ᾉ",
    },
    TitleMapping {
        code: 0x1f82,
        replacement: "ᾊ",
    },
    TitleMapping {
        code: 0x1f83,
        replacement: "ᾋ",
    },
    TitleMapping {
        code: 0x1f84,
        replacement: "ᾌ",
    },
    TitleMapping {
        code: 0x1f85,
        replacement: "ᾍ",
    },
    TitleMapping {
        code: 0x1f86,
        replacement: "ᾎ",
    },
    TitleMapping {
        code: 0x1f87,
        replacement: "ᾏ",
    },
    TitleMapping {
        code: 0x1f90,
        replacement: "ᾘ",
    },
    TitleMapping {
        code: 0x1f91,
        replacement: "ᾙ",
    },
    TitleMapping {
        code: 0x1f92,
        replacement: "ᾚ",
    },
    TitleMapping {
        code: 0x1f93,
        replacement: "ᾛ",
    },
    TitleMapping {
        code: 0x1f94,
        replacement: "ᾜ",
    },
    TitleMapping {
        code: 0x1f95,
        replacement: "ᾝ",
    },
    TitleMapping {
        code: 0x1f96,
        replacement: "ᾞ",
    },
    TitleMapping {
        code: 0x1f97,
        replacement: "ᾟ",
    },
    TitleMapping {
        code: 0x1fa0,
        replacement: "ᾨ",
    },
    TitleMapping {
        code: 0x1fa1,
        replacement: "ᾩ",
    },
    TitleMapping {
        code: 0x1fa2,
        replacement: "ᾪ",
    },
    TitleMapping {
        code: 0x1fa3,
        replacement: "ᾫ",
    },
    TitleMapping {
        code: 0x1fa4,
        replacement: "ᾬ",
    },
    TitleMapping {
        code: 0x1fa5,
        replacement: "ᾭ",
    },
    TitleMapping {
        code: 0x1fa6,
        replacement: "ᾮ",
    },
    TitleMapping {
        code: 0x1fa7,
        replacement: "ᾯ",
    },
    TitleMapping {
        code: 0x1fb0,
        replacement: "Ᾰ",
    },
    TitleMapping {
        code: 0x1fb1,
        replacement: "Ᾱ",
    },
    TitleMapping {
        code: 0x1fb2,
        replacement: "Ὰͅ",
    },
    TitleMapping {
        code: 0x1fb3,
        replacement: "ᾼ",
    },
    TitleMapping {
        code: 0x1fb4,
        replacement: "Άͅ",
    },
    TitleMapping {
        code: 0x1fb6,
        replacement: "Α͂",
    },
    TitleMapping {
        code: 0x1fb7,
        replacement: "\u{391}\u{342}\u{345}",
    },
    TitleMapping {
        code: 0x1fbe,
        replacement: "Ι",
    },
    TitleMapping {
        code: 0x1fc2,
        replacement: "Ὴͅ",
    },
    TitleMapping {
        code: 0x1fc3,
        replacement: "ῌ",
    },
    TitleMapping {
        code: 0x1fc4,
        replacement: "Ήͅ",
    },
    TitleMapping {
        code: 0x1fc6,
        replacement: "Η͂",
    },
    TitleMapping {
        code: 0x1fc7,
        replacement: "\u{397}\u{342}\u{345}",
    },
    TitleMapping {
        code: 0x1fd0,
        replacement: "Ῐ",
    },
    TitleMapping {
        code: 0x1fd1,
        replacement: "Ῑ",
    },
    TitleMapping {
        code: 0x1fd2,
        replacement: "\u{399}\u{308}\u{300}",
    },
    TitleMapping {
        code: 0x1fd3,
        replacement: "\u{399}\u{308}\u{301}",
    },
    TitleMapping {
        code: 0x1fd6,
        replacement: "Ι͂",
    },
    TitleMapping {
        code: 0x1fd7,
        replacement: "\u{399}\u{308}\u{342}",
    },
    TitleMapping {
        code: 0x1fe0,
        replacement: "Ῠ",
    },
    TitleMapping {
        code: 0x1fe1,
        replacement: "Ῡ",
    },
    TitleMapping {
        code: 0x1fe2,
        replacement: "\u{3a5}\u{308}\u{300}",
    },
    TitleMapping {
        code: 0x1fe3,
        replacement: "\u{3a5}\u{308}\u{301}",
    },
    TitleMapping {
        code: 0x1fe4,
        replacement: "Ρ̓",
    },
    TitleMapping {
        code: 0x1fe5,
        replacement: "Ῥ",
    },
    TitleMapping {
        code: 0x1fe6,
        replacement: "Υ͂",
    },
    TitleMapping {
        code: 0x1fe7,
        replacement: "\u{3a5}\u{308}\u{342}",
    },
    TitleMapping {
        code: 0x1ff2,
        replacement: "Ὼͅ",
    },
    TitleMapping {
        code: 0x1ff3,
        replacement: "ῼ",
    },
    TitleMapping {
        code: 0x1ff4,
        replacement: "Ώͅ",
    },
    TitleMapping {
        code: 0x1ff6,
        replacement: "Ω͂",
    },
    TitleMapping {
        code: 0x1ff7,
        replacement: "\u{3a9}\u{342}\u{345}",
    },
    TitleMapping {
        code: 0x214e,
        replacement: "Ⅎ",
    },
    TitleMapping {
        code: 0x2170,
        replacement: "Ⅰ",
    },
    TitleMapping {
        code: 0x2171,
        replacement: "Ⅱ",
    },
    TitleMapping {
        code: 0x2172,
        replacement: "Ⅲ",
    },
    TitleMapping {
        code: 0x2173,
        replacement: "Ⅳ",
    },
    TitleMapping {
        code: 0x2174,
        replacement: "Ⅴ",
    },
    TitleMapping {
        code: 0x2175,
        replacement: "Ⅵ",
    },
    TitleMapping {
        code: 0x2176,
        replacement: "Ⅶ",
    },
    TitleMapping {
        code: 0x2177,
        replacement: "Ⅷ",
    },
    TitleMapping {
        code: 0x2178,
        replacement: "Ⅸ",
    },
    TitleMapping {
        code: 0x2179,
        replacement: "Ⅹ",
    },
    TitleMapping {
        code: 0x217a,
        replacement: "Ⅺ",
    },
    TitleMapping {
        code: 0x217b,
        replacement: "Ⅻ",
    },
    TitleMapping {
        code: 0x217c,
        replacement: "Ⅼ",
    },
    TitleMapping {
        code: 0x217d,
        replacement: "Ⅽ",
    },
    TitleMapping {
        code: 0x217e,
        replacement: "Ⅾ",
    },
    TitleMapping {
        code: 0x217f,
        replacement: "Ⅿ",
    },
    TitleMapping {
        code: 0x2184,
        replacement: "Ↄ",
    },
    TitleMapping {
        code: 0x24d0,
        replacement: "Ⓐ",
    },
    TitleMapping {
        code: 0x24d1,
        replacement: "Ⓑ",
    },
    TitleMapping {
        code: 0x24d2,
        replacement: "Ⓒ",
    },
    TitleMapping {
        code: 0x24d3,
        replacement: "Ⓓ",
    },
    TitleMapping {
        code: 0x24d4,
        replacement: "Ⓔ",
    },
    TitleMapping {
        code: 0x24d5,
        replacement: "Ⓕ",
    },
    TitleMapping {
        code: 0x24d6,
        replacement: "Ⓖ",
    },
    TitleMapping {
        code: 0x24d7,
        replacement: "Ⓗ",
    },
    TitleMapping {
        code: 0x24d8,
        replacement: "Ⓘ",
    },
    TitleMapping {
        code: 0x24d9,
        replacement: "Ⓙ",
    },
    TitleMapping {
        code: 0x24da,
        replacement: "Ⓚ",
    },
    TitleMapping {
        code: 0x24db,
        replacement: "Ⓛ",
    },
    TitleMapping {
        code: 0x24dc,
        replacement: "Ⓜ",
    },
    TitleMapping {
        code: 0x24dd,
        replacement: "Ⓝ",
    },
    TitleMapping {
        code: 0x24de,
        replacement: "Ⓞ",
    },
    TitleMapping {
        code: 0x24df,
        replacement: "Ⓟ",
    },
    TitleMapping {
        code: 0x24e0,
        replacement: "Ⓠ",
    },
    TitleMapping {
        code: 0x24e1,
        replacement: "Ⓡ",
    },
    TitleMapping {
        code: 0x24e2,
        replacement: "Ⓢ",
    },
    TitleMapping {
        code: 0x24e3,
        replacement: "Ⓣ",
    },
    TitleMapping {
        code: 0x24e4,
        replacement: "Ⓤ",
    },
    TitleMapping {
        code: 0x24e5,
        replacement: "Ⓥ",
    },
    TitleMapping {
        code: 0x24e6,
        replacement: "Ⓦ",
    },
    TitleMapping {
        code: 0x24e7,
        replacement: "Ⓧ",
    },
    TitleMapping {
        code: 0x24e8,
        replacement: "Ⓨ",
    },
    TitleMapping {
        code: 0x24e9,
        replacement: "Ⓩ",
    },
    TitleMapping {
        code: 0x2c30,
        replacement: "Ⰰ",
    },
    TitleMapping {
        code: 0x2c31,
        replacement: "Ⰱ",
    },
    TitleMapping {
        code: 0x2c32,
        replacement: "Ⰲ",
    },
    TitleMapping {
        code: 0x2c33,
        replacement: "Ⰳ",
    },
    TitleMapping {
        code: 0x2c34,
        replacement: "Ⰴ",
    },
    TitleMapping {
        code: 0x2c35,
        replacement: "Ⰵ",
    },
    TitleMapping {
        code: 0x2c36,
        replacement: "Ⰶ",
    },
    TitleMapping {
        code: 0x2c37,
        replacement: "Ⰷ",
    },
    TitleMapping {
        code: 0x2c38,
        replacement: "Ⰸ",
    },
    TitleMapping {
        code: 0x2c39,
        replacement: "Ⰹ",
    },
    TitleMapping {
        code: 0x2c3a,
        replacement: "Ⰺ",
    },
    TitleMapping {
        code: 0x2c3b,
        replacement: "Ⰻ",
    },
    TitleMapping {
        code: 0x2c3c,
        replacement: "Ⰼ",
    },
    TitleMapping {
        code: 0x2c3d,
        replacement: "Ⰽ",
    },
    TitleMapping {
        code: 0x2c3e,
        replacement: "Ⰾ",
    },
    TitleMapping {
        code: 0x2c3f,
        replacement: "Ⰿ",
    },
    TitleMapping {
        code: 0x2c40,
        replacement: "Ⱀ",
    },
    TitleMapping {
        code: 0x2c41,
        replacement: "Ⱁ",
    },
    TitleMapping {
        code: 0x2c42,
        replacement: "Ⱂ",
    },
    TitleMapping {
        code: 0x2c43,
        replacement: "Ⱃ",
    },
    TitleMapping {
        code: 0x2c44,
        replacement: "Ⱄ",
    },
    TitleMapping {
        code: 0x2c45,
        replacement: "Ⱅ",
    },
    TitleMapping {
        code: 0x2c46,
        replacement: "Ⱆ",
    },
    TitleMapping {
        code: 0x2c47,
        replacement: "Ⱇ",
    },
    TitleMapping {
        code: 0x2c48,
        replacement: "Ⱈ",
    },
    TitleMapping {
        code: 0x2c49,
        replacement: "Ⱉ",
    },
    TitleMapping {
        code: 0x2c4a,
        replacement: "Ⱊ",
    },
    TitleMapping {
        code: 0x2c4b,
        replacement: "Ⱋ",
    },
    TitleMapping {
        code: 0x2c4c,
        replacement: "Ⱌ",
    },
    TitleMapping {
        code: 0x2c4d,
        replacement: "Ⱍ",
    },
    TitleMapping {
        code: 0x2c4e,
        replacement: "Ⱎ",
    },
    TitleMapping {
        code: 0x2c4f,
        replacement: "Ⱏ",
    },
    TitleMapping {
        code: 0x2c50,
        replacement: "Ⱐ",
    },
    TitleMapping {
        code: 0x2c51,
        replacement: "Ⱑ",
    },
    TitleMapping {
        code: 0x2c52,
        replacement: "Ⱒ",
    },
    TitleMapping {
        code: 0x2c53,
        replacement: "Ⱓ",
    },
    TitleMapping {
        code: 0x2c54,
        replacement: "Ⱔ",
    },
    TitleMapping {
        code: 0x2c55,
        replacement: "Ⱕ",
    },
    TitleMapping {
        code: 0x2c56,
        replacement: "Ⱖ",
    },
    TitleMapping {
        code: 0x2c57,
        replacement: "Ⱗ",
    },
    TitleMapping {
        code: 0x2c58,
        replacement: "Ⱘ",
    },
    TitleMapping {
        code: 0x2c59,
        replacement: "Ⱙ",
    },
    TitleMapping {
        code: 0x2c5a,
        replacement: "Ⱚ",
    },
    TitleMapping {
        code: 0x2c5b,
        replacement: "Ⱛ",
    },
    TitleMapping {
        code: 0x2c5c,
        replacement: "Ⱜ",
    },
    TitleMapping {
        code: 0x2c5d,
        replacement: "Ⱝ",
    },
    TitleMapping {
        code: 0x2c5e,
        replacement: "Ⱞ",
    },
    TitleMapping {
        code: 0x2c5f,
        replacement: "Ⱟ",
    },
    TitleMapping {
        code: 0x2c61,
        replacement: "Ⱡ",
    },
    TitleMapping {
        code: 0x2c65,
        replacement: "Ⱥ",
    },
    TitleMapping {
        code: 0x2c66,
        replacement: "Ⱦ",
    },
    TitleMapping {
        code: 0x2c68,
        replacement: "Ⱨ",
    },
    TitleMapping {
        code: 0x2c6a,
        replacement: "Ⱪ",
    },
    TitleMapping {
        code: 0x2c6c,
        replacement: "Ⱬ",
    },
    TitleMapping {
        code: 0x2c73,
        replacement: "Ⱳ",
    },
    TitleMapping {
        code: 0x2c76,
        replacement: "Ⱶ",
    },
    TitleMapping {
        code: 0x2c81,
        replacement: "Ⲁ",
    },
    TitleMapping {
        code: 0x2c83,
        replacement: "Ⲃ",
    },
    TitleMapping {
        code: 0x2c85,
        replacement: "Ⲅ",
    },
    TitleMapping {
        code: 0x2c87,
        replacement: "Ⲇ",
    },
    TitleMapping {
        code: 0x2c89,
        replacement: "Ⲉ",
    },
    TitleMapping {
        code: 0x2c8b,
        replacement: "Ⲋ",
    },
    TitleMapping {
        code: 0x2c8d,
        replacement: "Ⲍ",
    },
    TitleMapping {
        code: 0x2c8f,
        replacement: "Ⲏ",
    },
    TitleMapping {
        code: 0x2c91,
        replacement: "Ⲑ",
    },
    TitleMapping {
        code: 0x2c93,
        replacement: "Ⲓ",
    },
    TitleMapping {
        code: 0x2c95,
        replacement: "Ⲕ",
    },
    TitleMapping {
        code: 0x2c97,
        replacement: "Ⲗ",
    },
    TitleMapping {
        code: 0x2c99,
        replacement: "Ⲙ",
    },
    TitleMapping {
        code: 0x2c9b,
        replacement: "Ⲛ",
    },
    TitleMapping {
        code: 0x2c9d,
        replacement: "Ⲝ",
    },
    TitleMapping {
        code: 0x2c9f,
        replacement: "Ⲟ",
    },
    TitleMapping {
        code: 0x2ca1,
        replacement: "Ⲡ",
    },
    TitleMapping {
        code: 0x2ca3,
        replacement: "Ⲣ",
    },
    TitleMapping {
        code: 0x2ca5,
        replacement: "Ⲥ",
    },
    TitleMapping {
        code: 0x2ca7,
        replacement: "Ⲧ",
    },
    TitleMapping {
        code: 0x2ca9,
        replacement: "Ⲩ",
    },
    TitleMapping {
        code: 0x2cab,
        replacement: "Ⲫ",
    },
    TitleMapping {
        code: 0x2cad,
        replacement: "Ⲭ",
    },
    TitleMapping {
        code: 0x2caf,
        replacement: "Ⲯ",
    },
    TitleMapping {
        code: 0x2cb1,
        replacement: "Ⲱ",
    },
    TitleMapping {
        code: 0x2cb3,
        replacement: "Ⲳ",
    },
    TitleMapping {
        code: 0x2cb5,
        replacement: "Ⲵ",
    },
    TitleMapping {
        code: 0x2cb7,
        replacement: "Ⲷ",
    },
    TitleMapping {
        code: 0x2cb9,
        replacement: "Ⲹ",
    },
    TitleMapping {
        code: 0x2cbb,
        replacement: "Ⲻ",
    },
    TitleMapping {
        code: 0x2cbd,
        replacement: "Ⲽ",
    },
    TitleMapping {
        code: 0x2cbf,
        replacement: "Ⲿ",
    },
    TitleMapping {
        code: 0x2cc1,
        replacement: "Ⳁ",
    },
    TitleMapping {
        code: 0x2cc3,
        replacement: "Ⳃ",
    },
    TitleMapping {
        code: 0x2cc5,
        replacement: "Ⳅ",
    },
    TitleMapping {
        code: 0x2cc7,
        replacement: "Ⳇ",
    },
    TitleMapping {
        code: 0x2cc9,
        replacement: "Ⳉ",
    },
    TitleMapping {
        code: 0x2ccb,
        replacement: "Ⳋ",
    },
    TitleMapping {
        code: 0x2ccd,
        replacement: "Ⳍ",
    },
    TitleMapping {
        code: 0x2ccf,
        replacement: "Ⳏ",
    },
    TitleMapping {
        code: 0x2cd1,
        replacement: "Ⳑ",
    },
    TitleMapping {
        code: 0x2cd3,
        replacement: "Ⳓ",
    },
    TitleMapping {
        code: 0x2cd5,
        replacement: "Ⳕ",
    },
    TitleMapping {
        code: 0x2cd7,
        replacement: "Ⳗ",
    },
    TitleMapping {
        code: 0x2cd9,
        replacement: "Ⳙ",
    },
    TitleMapping {
        code: 0x2cdb,
        replacement: "Ⳛ",
    },
    TitleMapping {
        code: 0x2cdd,
        replacement: "Ⳝ",
    },
    TitleMapping {
        code: 0x2cdf,
        replacement: "Ⳟ",
    },
    TitleMapping {
        code: 0x2ce1,
        replacement: "Ⳡ",
    },
    TitleMapping {
        code: 0x2ce3,
        replacement: "Ⳣ",
    },
    TitleMapping {
        code: 0x2cec,
        replacement: "Ⳬ",
    },
    TitleMapping {
        code: 0x2cee,
        replacement: "Ⳮ",
    },
    TitleMapping {
        code: 0x2cf3,
        replacement: "Ⳳ",
    },
    TitleMapping {
        code: 0x2d00,
        replacement: "Ⴀ",
    },
    TitleMapping {
        code: 0x2d01,
        replacement: "Ⴁ",
    },
    TitleMapping {
        code: 0x2d02,
        replacement: "Ⴂ",
    },
    TitleMapping {
        code: 0x2d03,
        replacement: "Ⴃ",
    },
    TitleMapping {
        code: 0x2d04,
        replacement: "Ⴄ",
    },
    TitleMapping {
        code: 0x2d05,
        replacement: "Ⴅ",
    },
    TitleMapping {
        code: 0x2d06,
        replacement: "Ⴆ",
    },
    TitleMapping {
        code: 0x2d07,
        replacement: "Ⴇ",
    },
    TitleMapping {
        code: 0x2d08,
        replacement: "Ⴈ",
    },
    TitleMapping {
        code: 0x2d09,
        replacement: "Ⴉ",
    },
    TitleMapping {
        code: 0x2d0a,
        replacement: "Ⴊ",
    },
    TitleMapping {
        code: 0x2d0b,
        replacement: "Ⴋ",
    },
    TitleMapping {
        code: 0x2d0c,
        replacement: "Ⴌ",
    },
    TitleMapping {
        code: 0x2d0d,
        replacement: "Ⴍ",
    },
    TitleMapping {
        code: 0x2d0e,
        replacement: "Ⴎ",
    },
    TitleMapping {
        code: 0x2d0f,
        replacement: "Ⴏ",
    },
    TitleMapping {
        code: 0x2d10,
        replacement: "Ⴐ",
    },
    TitleMapping {
        code: 0x2d11,
        replacement: "Ⴑ",
    },
    TitleMapping {
        code: 0x2d12,
        replacement: "Ⴒ",
    },
    TitleMapping {
        code: 0x2d13,
        replacement: "Ⴓ",
    },
    TitleMapping {
        code: 0x2d14,
        replacement: "Ⴔ",
    },
    TitleMapping {
        code: 0x2d15,
        replacement: "Ⴕ",
    },
    TitleMapping {
        code: 0x2d16,
        replacement: "Ⴖ",
    },
    TitleMapping {
        code: 0x2d17,
        replacement: "Ⴗ",
    },
    TitleMapping {
        code: 0x2d18,
        replacement: "Ⴘ",
    },
    TitleMapping {
        code: 0x2d19,
        replacement: "Ⴙ",
    },
    TitleMapping {
        code: 0x2d1a,
        replacement: "Ⴚ",
    },
    TitleMapping {
        code: 0x2d1b,
        replacement: "Ⴛ",
    },
    TitleMapping {
        code: 0x2d1c,
        replacement: "Ⴜ",
    },
    TitleMapping {
        code: 0x2d1d,
        replacement: "Ⴝ",
    },
    TitleMapping {
        code: 0x2d1e,
        replacement: "Ⴞ",
    },
    TitleMapping {
        code: 0x2d1f,
        replacement: "Ⴟ",
    },
    TitleMapping {
        code: 0x2d20,
        replacement: "Ⴠ",
    },
    TitleMapping {
        code: 0x2d21,
        replacement: "Ⴡ",
    },
    TitleMapping {
        code: 0x2d22,
        replacement: "Ⴢ",
    },
    TitleMapping {
        code: 0x2d23,
        replacement: "Ⴣ",
    },
    TitleMapping {
        code: 0x2d24,
        replacement: "Ⴤ",
    },
    TitleMapping {
        code: 0x2d25,
        replacement: "Ⴥ",
    },
    TitleMapping {
        code: 0x2d27,
        replacement: "Ⴧ",
    },
    TitleMapping {
        code: 0x2d2d,
        replacement: "Ⴭ",
    },
    TitleMapping {
        code: 0xa641,
        replacement: "Ꙁ",
    },
    TitleMapping {
        code: 0xa643,
        replacement: "Ꙃ",
    },
    TitleMapping {
        code: 0xa645,
        replacement: "Ꙅ",
    },
    TitleMapping {
        code: 0xa647,
        replacement: "Ꙇ",
    },
    TitleMapping {
        code: 0xa649,
        replacement: "Ꙉ",
    },
    TitleMapping {
        code: 0xa64b,
        replacement: "Ꙋ",
    },
    TitleMapping {
        code: 0xa64d,
        replacement: "Ꙍ",
    },
    TitleMapping {
        code: 0xa64f,
        replacement: "Ꙏ",
    },
    TitleMapping {
        code: 0xa651,
        replacement: "Ꙑ",
    },
    TitleMapping {
        code: 0xa653,
        replacement: "Ꙓ",
    },
    TitleMapping {
        code: 0xa655,
        replacement: "Ꙕ",
    },
    TitleMapping {
        code: 0xa657,
        replacement: "Ꙗ",
    },
    TitleMapping {
        code: 0xa659,
        replacement: "Ꙙ",
    },
    TitleMapping {
        code: 0xa65b,
        replacement: "Ꙛ",
    },
    TitleMapping {
        code: 0xa65d,
        replacement: "Ꙝ",
    },
    TitleMapping {
        code: 0xa65f,
        replacement: "Ꙟ",
    },
    TitleMapping {
        code: 0xa661,
        replacement: "Ꙡ",
    },
    TitleMapping {
        code: 0xa663,
        replacement: "Ꙣ",
    },
    TitleMapping {
        code: 0xa665,
        replacement: "Ꙥ",
    },
    TitleMapping {
        code: 0xa667,
        replacement: "Ꙧ",
    },
    TitleMapping {
        code: 0xa669,
        replacement: "Ꙩ",
    },
    TitleMapping {
        code: 0xa66b,
        replacement: "Ꙫ",
    },
    TitleMapping {
        code: 0xa66d,
        replacement: "Ꙭ",
    },
    TitleMapping {
        code: 0xa681,
        replacement: "Ꚁ",
    },
    TitleMapping {
        code: 0xa683,
        replacement: "Ꚃ",
    },
    TitleMapping {
        code: 0xa685,
        replacement: "Ꚅ",
    },
    TitleMapping {
        code: 0xa687,
        replacement: "Ꚇ",
    },
    TitleMapping {
        code: 0xa689,
        replacement: "Ꚉ",
    },
    TitleMapping {
        code: 0xa68b,
        replacement: "Ꚋ",
    },
    TitleMapping {
        code: 0xa68d,
        replacement: "Ꚍ",
    },
    TitleMapping {
        code: 0xa68f,
        replacement: "Ꚏ",
    },
    TitleMapping {
        code: 0xa691,
        replacement: "Ꚑ",
    },
    TitleMapping {
        code: 0xa693,
        replacement: "Ꚓ",
    },
    TitleMapping {
        code: 0xa695,
        replacement: "Ꚕ",
    },
    TitleMapping {
        code: 0xa697,
        replacement: "Ꚗ",
    },
    TitleMapping {
        code: 0xa699,
        replacement: "Ꚙ",
    },
    TitleMapping {
        code: 0xa69b,
        replacement: "Ꚛ",
    },
    TitleMapping {
        code: 0xa723,
        replacement: "Ꜣ",
    },
    TitleMapping {
        code: 0xa725,
        replacement: "Ꜥ",
    },
    TitleMapping {
        code: 0xa727,
        replacement: "Ꜧ",
    },
    TitleMapping {
        code: 0xa729,
        replacement: "Ꜩ",
    },
    TitleMapping {
        code: 0xa72b,
        replacement: "Ꜫ",
    },
    TitleMapping {
        code: 0xa72d,
        replacement: "Ꜭ",
    },
    TitleMapping {
        code: 0xa72f,
        replacement: "Ꜯ",
    },
    TitleMapping {
        code: 0xa733,
        replacement: "Ꜳ",
    },
    TitleMapping {
        code: 0xa735,
        replacement: "Ꜵ",
    },
    TitleMapping {
        code: 0xa737,
        replacement: "Ꜷ",
    },
    TitleMapping {
        code: 0xa739,
        replacement: "Ꜹ",
    },
    TitleMapping {
        code: 0xa73b,
        replacement: "Ꜻ",
    },
    TitleMapping {
        code: 0xa73d,
        replacement: "Ꜽ",
    },
    TitleMapping {
        code: 0xa73f,
        replacement: "Ꜿ",
    },
    TitleMapping {
        code: 0xa741,
        replacement: "Ꝁ",
    },
    TitleMapping {
        code: 0xa743,
        replacement: "Ꝃ",
    },
    TitleMapping {
        code: 0xa745,
        replacement: "Ꝅ",
    },
    TitleMapping {
        code: 0xa747,
        replacement: "Ꝇ",
    },
    TitleMapping {
        code: 0xa749,
        replacement: "Ꝉ",
    },
    TitleMapping {
        code: 0xa74b,
        replacement: "Ꝋ",
    },
    TitleMapping {
        code: 0xa74d,
        replacement: "Ꝍ",
    },
    TitleMapping {
        code: 0xa74f,
        replacement: "Ꝏ",
    },
    TitleMapping {
        code: 0xa751,
        replacement: "Ꝑ",
    },
    TitleMapping {
        code: 0xa753,
        replacement: "Ꝓ",
    },
    TitleMapping {
        code: 0xa755,
        replacement: "Ꝕ",
    },
    TitleMapping {
        code: 0xa757,
        replacement: "Ꝗ",
    },
    TitleMapping {
        code: 0xa759,
        replacement: "Ꝙ",
    },
    TitleMapping {
        code: 0xa75b,
        replacement: "Ꝛ",
    },
    TitleMapping {
        code: 0xa75d,
        replacement: "Ꝝ",
    },
    TitleMapping {
        code: 0xa75f,
        replacement: "Ꝟ",
    },
    TitleMapping {
        code: 0xa761,
        replacement: "Ꝡ",
    },
    TitleMapping {
        code: 0xa763,
        replacement: "Ꝣ",
    },
    TitleMapping {
        code: 0xa765,
        replacement: "Ꝥ",
    },
    TitleMapping {
        code: 0xa767,
        replacement: "Ꝧ",
    },
    TitleMapping {
        code: 0xa769,
        replacement: "Ꝩ",
    },
    TitleMapping {
        code: 0xa76b,
        replacement: "Ꝫ",
    },
    TitleMapping {
        code: 0xa76d,
        replacement: "Ꝭ",
    },
    TitleMapping {
        code: 0xa76f,
        replacement: "Ꝯ",
    },
    TitleMapping {
        code: 0xa77a,
        replacement: "Ꝺ",
    },
    TitleMapping {
        code: 0xa77c,
        replacement: "Ꝼ",
    },
    TitleMapping {
        code: 0xa77f,
        replacement: "Ꝿ",
    },
    TitleMapping {
        code: 0xa781,
        replacement: "Ꞁ",
    },
    TitleMapping {
        code: 0xa783,
        replacement: "Ꞃ",
    },
    TitleMapping {
        code: 0xa785,
        replacement: "Ꞅ",
    },
    TitleMapping {
        code: 0xa787,
        replacement: "Ꞇ",
    },
    TitleMapping {
        code: 0xa78c,
        replacement: "Ꞌ",
    },
    TitleMapping {
        code: 0xa791,
        replacement: "Ꞑ",
    },
    TitleMapping {
        code: 0xa793,
        replacement: "Ꞓ",
    },
    TitleMapping {
        code: 0xa794,
        replacement: "Ꞔ",
    },
    TitleMapping {
        code: 0xa797,
        replacement: "Ꞗ",
    },
    TitleMapping {
        code: 0xa799,
        replacement: "Ꞙ",
    },
    TitleMapping {
        code: 0xa79b,
        replacement: "Ꞛ",
    },
    TitleMapping {
        code: 0xa79d,
        replacement: "Ꞝ",
    },
    TitleMapping {
        code: 0xa79f,
        replacement: "Ꞟ",
    },
    TitleMapping {
        code: 0xa7a1,
        replacement: "Ꞡ",
    },
    TitleMapping {
        code: 0xa7a3,
        replacement: "Ꞣ",
    },
    TitleMapping {
        code: 0xa7a5,
        replacement: "Ꞥ",
    },
    TitleMapping {
        code: 0xa7a7,
        replacement: "Ꞧ",
    },
    TitleMapping {
        code: 0xa7a9,
        replacement: "Ꞩ",
    },
    TitleMapping {
        code: 0xa7b5,
        replacement: "Ꞵ",
    },
    TitleMapping {
        code: 0xa7b7,
        replacement: "Ꞷ",
    },
    TitleMapping {
        code: 0xa7b9,
        replacement: "Ꞹ",
    },
    TitleMapping {
        code: 0xa7bb,
        replacement: "Ꞻ",
    },
    TitleMapping {
        code: 0xa7bd,
        replacement: "Ꞽ",
    },
    TitleMapping {
        code: 0xa7bf,
        replacement: "Ꞿ",
    },
    TitleMapping {
        code: 0xa7c1,
        replacement: "Ꟁ",
    },
    TitleMapping {
        code: 0xa7c3,
        replacement: "Ꟃ",
    },
    TitleMapping {
        code: 0xa7c8,
        replacement: "Ꟈ",
    },
    TitleMapping {
        code: 0xa7ca,
        replacement: "Ꟊ",
    },
    TitleMapping {
        code: 0xa7d1,
        replacement: "Ꟑ",
    },
    TitleMapping {
        code: 0xa7d7,
        replacement: "Ꟗ",
    },
    TitleMapping {
        code: 0xa7d9,
        replacement: "Ꟙ",
    },
    TitleMapping {
        code: 0xa7f6,
        replacement: "Ꟶ",
    },
    TitleMapping {
        code: 0xab53,
        replacement: "Ꭓ",
    },
    TitleMapping {
        code: 0xab70,
        replacement: "Ꭰ",
    },
    TitleMapping {
        code: 0xab71,
        replacement: "Ꭱ",
    },
    TitleMapping {
        code: 0xab72,
        replacement: "Ꭲ",
    },
    TitleMapping {
        code: 0xab73,
        replacement: "Ꭳ",
    },
    TitleMapping {
        code: 0xab74,
        replacement: "Ꭴ",
    },
    TitleMapping {
        code: 0xab75,
        replacement: "Ꭵ",
    },
    TitleMapping {
        code: 0xab76,
        replacement: "Ꭶ",
    },
    TitleMapping {
        code: 0xab77,
        replacement: "Ꭷ",
    },
    TitleMapping {
        code: 0xab78,
        replacement: "Ꭸ",
    },
    TitleMapping {
        code: 0xab79,
        replacement: "Ꭹ",
    },
    TitleMapping {
        code: 0xab7a,
        replacement: "Ꭺ",
    },
    TitleMapping {
        code: 0xab7b,
        replacement: "Ꭻ",
    },
    TitleMapping {
        code: 0xab7c,
        replacement: "Ꭼ",
    },
    TitleMapping {
        code: 0xab7d,
        replacement: "Ꭽ",
    },
    TitleMapping {
        code: 0xab7e,
        replacement: "Ꭾ",
    },
    TitleMapping {
        code: 0xab7f,
        replacement: "Ꭿ",
    },
    TitleMapping {
        code: 0xab80,
        replacement: "Ꮀ",
    },
    TitleMapping {
        code: 0xab81,
        replacement: "Ꮁ",
    },
    TitleMapping {
        code: 0xab82,
        replacement: "Ꮂ",
    },
    TitleMapping {
        code: 0xab83,
        replacement: "Ꮃ",
    },
    TitleMapping {
        code: 0xab84,
        replacement: "Ꮄ",
    },
    TitleMapping {
        code: 0xab85,
        replacement: "Ꮅ",
    },
    TitleMapping {
        code: 0xab86,
        replacement: "Ꮆ",
    },
    TitleMapping {
        code: 0xab87,
        replacement: "Ꮇ",
    },
    TitleMapping {
        code: 0xab88,
        replacement: "Ꮈ",
    },
    TitleMapping {
        code: 0xab89,
        replacement: "Ꮉ",
    },
    TitleMapping {
        code: 0xab8a,
        replacement: "Ꮊ",
    },
    TitleMapping {
        code: 0xab8b,
        replacement: "Ꮋ",
    },
    TitleMapping {
        code: 0xab8c,
        replacement: "Ꮌ",
    },
    TitleMapping {
        code: 0xab8d,
        replacement: "Ꮍ",
    },
    TitleMapping {
        code: 0xab8e,
        replacement: "Ꮎ",
    },
    TitleMapping {
        code: 0xab8f,
        replacement: "Ꮏ",
    },
    TitleMapping {
        code: 0xab90,
        replacement: "Ꮐ",
    },
    TitleMapping {
        code: 0xab91,
        replacement: "Ꮑ",
    },
    TitleMapping {
        code: 0xab92,
        replacement: "Ꮒ",
    },
    TitleMapping {
        code: 0xab93,
        replacement: "Ꮓ",
    },
    TitleMapping {
        code: 0xab94,
        replacement: "Ꮔ",
    },
    TitleMapping {
        code: 0xab95,
        replacement: "Ꮕ",
    },
    TitleMapping {
        code: 0xab96,
        replacement: "Ꮖ",
    },
    TitleMapping {
        code: 0xab97,
        replacement: "Ꮗ",
    },
    TitleMapping {
        code: 0xab98,
        replacement: "Ꮘ",
    },
    TitleMapping {
        code: 0xab99,
        replacement: "Ꮙ",
    },
    TitleMapping {
        code: 0xab9a,
        replacement: "Ꮚ",
    },
    TitleMapping {
        code: 0xab9b,
        replacement: "Ꮛ",
    },
    TitleMapping {
        code: 0xab9c,
        replacement: "Ꮜ",
    },
    TitleMapping {
        code: 0xab9d,
        replacement: "Ꮝ",
    },
    TitleMapping {
        code: 0xab9e,
        replacement: "Ꮞ",
    },
    TitleMapping {
        code: 0xab9f,
        replacement: "Ꮟ",
    },
    TitleMapping {
        code: 0xaba0,
        replacement: "Ꮠ",
    },
    TitleMapping {
        code: 0xaba1,
        replacement: "Ꮡ",
    },
    TitleMapping {
        code: 0xaba2,
        replacement: "Ꮢ",
    },
    TitleMapping {
        code: 0xaba3,
        replacement: "Ꮣ",
    },
    TitleMapping {
        code: 0xaba4,
        replacement: "Ꮤ",
    },
    TitleMapping {
        code: 0xaba5,
        replacement: "Ꮥ",
    },
    TitleMapping {
        code: 0xaba6,
        replacement: "Ꮦ",
    },
    TitleMapping {
        code: 0xaba7,
        replacement: "Ꮧ",
    },
    TitleMapping {
        code: 0xaba8,
        replacement: "Ꮨ",
    },
    TitleMapping {
        code: 0xaba9,
        replacement: "Ꮩ",
    },
    TitleMapping {
        code: 0xabaa,
        replacement: "Ꮪ",
    },
    TitleMapping {
        code: 0xabab,
        replacement: "Ꮫ",
    },
    TitleMapping {
        code: 0xabac,
        replacement: "Ꮬ",
    },
    TitleMapping {
        code: 0xabad,
        replacement: "Ꮭ",
    },
    TitleMapping {
        code: 0xabae,
        replacement: "Ꮮ",
    },
    TitleMapping {
        code: 0xabaf,
        replacement: "Ꮯ",
    },
    TitleMapping {
        code: 0xabb0,
        replacement: "Ꮰ",
    },
    TitleMapping {
        code: 0xabb1,
        replacement: "Ꮱ",
    },
    TitleMapping {
        code: 0xabb2,
        replacement: "Ꮲ",
    },
    TitleMapping {
        code: 0xabb3,
        replacement: "Ꮳ",
    },
    TitleMapping {
        code: 0xabb4,
        replacement: "Ꮴ",
    },
    TitleMapping {
        code: 0xabb5,
        replacement: "Ꮵ",
    },
    TitleMapping {
        code: 0xabb6,
        replacement: "Ꮶ",
    },
    TitleMapping {
        code: 0xabb7,
        replacement: "Ꮷ",
    },
    TitleMapping {
        code: 0xabb8,
        replacement: "Ꮸ",
    },
    TitleMapping {
        code: 0xabb9,
        replacement: "Ꮹ",
    },
    TitleMapping {
        code: 0xabba,
        replacement: "Ꮺ",
    },
    TitleMapping {
        code: 0xabbb,
        replacement: "Ꮻ",
    },
    TitleMapping {
        code: 0xabbc,
        replacement: "Ꮼ",
    },
    TitleMapping {
        code: 0xabbd,
        replacement: "Ꮽ",
    },
    TitleMapping {
        code: 0xabbe,
        replacement: "Ꮾ",
    },
    TitleMapping {
        code: 0xabbf,
        replacement: "Ꮿ",
    },
    TitleMapping {
        code: 0xfb00,
        replacement: "Ff",
    },
    TitleMapping {
        code: 0xfb01,
        replacement: "Fi",
    },
    TitleMapping {
        code: 0xfb02,
        replacement: "Fl",
    },
    TitleMapping {
        code: 0xfb03,
        replacement: "Ffi",
    },
    TitleMapping {
        code: 0xfb04,
        replacement: "Ffl",
    },
    TitleMapping {
        code: 0xfb05,
        replacement: "St",
    },
    TitleMapping {
        code: 0xfb06,
        replacement: "St",
    },
    TitleMapping {
        code: 0xfb13,
        replacement: "Մն",
    },
    TitleMapping {
        code: 0xfb14,
        replacement: "Մե",
    },
    TitleMapping {
        code: 0xfb15,
        replacement: "Մի",
    },
    TitleMapping {
        code: 0xfb16,
        replacement: "Վն",
    },
    TitleMapping {
        code: 0xfb17,
        replacement: "Մխ",
    },
    TitleMapping {
        code: 0xff41,
        replacement: "Ａ",
    },
    TitleMapping {
        code: 0xff42,
        replacement: "Ｂ",
    },
    TitleMapping {
        code: 0xff43,
        replacement: "Ｃ",
    },
    TitleMapping {
        code: 0xff44,
        replacement: "Ｄ",
    },
    TitleMapping {
        code: 0xff45,
        replacement: "Ｅ",
    },
    TitleMapping {
        code: 0xff46,
        replacement: "Ｆ",
    },
    TitleMapping {
        code: 0xff47,
        replacement: "Ｇ",
    },
    TitleMapping {
        code: 0xff48,
        replacement: "Ｈ",
    },
    TitleMapping {
        code: 0xff49,
        replacement: "Ｉ",
    },
    TitleMapping {
        code: 0xff4a,
        replacement: "Ｊ",
    },
    TitleMapping {
        code: 0xff4b,
        replacement: "Ｋ",
    },
    TitleMapping {
        code: 0xff4c,
        replacement: "Ｌ",
    },
    TitleMapping {
        code: 0xff4d,
        replacement: "Ｍ",
    },
    TitleMapping {
        code: 0xff4e,
        replacement: "Ｎ",
    },
    TitleMapping {
        code: 0xff4f,
        replacement: "Ｏ",
    },
    TitleMapping {
        code: 0xff50,
        replacement: "Ｐ",
    },
    TitleMapping {
        code: 0xff51,
        replacement: "Ｑ",
    },
    TitleMapping {
        code: 0xff52,
        replacement: "Ｒ",
    },
    TitleMapping {
        code: 0xff53,
        replacement: "Ｓ",
    },
    TitleMapping {
        code: 0xff54,
        replacement: "Ｔ",
    },
    TitleMapping {
        code: 0xff55,
        replacement: "Ｕ",
    },
    TitleMapping {
        code: 0xff56,
        replacement: "Ｖ",
    },
    TitleMapping {
        code: 0xff57,
        replacement: "Ｗ",
    },
    TitleMapping {
        code: 0xff58,
        replacement: "Ｘ",
    },
    TitleMapping {
        code: 0xff59,
        replacement: "Ｙ",
    },
    TitleMapping {
        code: 0xff5a,
        replacement: "Ｚ",
    },
    TitleMapping {
        code: 0x10428,
        replacement: "𐐀",
    },
    TitleMapping {
        code: 0x10429,
        replacement: "𐐁",
    },
    TitleMapping {
        code: 0x1042a,
        replacement: "𐐂",
    },
    TitleMapping {
        code: 0x1042b,
        replacement: "𐐃",
    },
    TitleMapping {
        code: 0x1042c,
        replacement: "𐐄",
    },
    TitleMapping {
        code: 0x1042d,
        replacement: "𐐅",
    },
    TitleMapping {
        code: 0x1042e,
        replacement: "𐐆",
    },
    TitleMapping {
        code: 0x1042f,
        replacement: "𐐇",
    },
    TitleMapping {
        code: 0x10430,
        replacement: "𐐈",
    },
    TitleMapping {
        code: 0x10431,
        replacement: "𐐉",
    },
    TitleMapping {
        code: 0x10432,
        replacement: "𐐊",
    },
    TitleMapping {
        code: 0x10433,
        replacement: "𐐋",
    },
    TitleMapping {
        code: 0x10434,
        replacement: "𐐌",
    },
    TitleMapping {
        code: 0x10435,
        replacement: "𐐍",
    },
    TitleMapping {
        code: 0x10436,
        replacement: "𐐎",
    },
    TitleMapping {
        code: 0x10437,
        replacement: "𐐏",
    },
    TitleMapping {
        code: 0x10438,
        replacement: "𐐐",
    },
    TitleMapping {
        code: 0x10439,
        replacement: "𐐑",
    },
    TitleMapping {
        code: 0x1043a,
        replacement: "𐐒",
    },
    TitleMapping {
        code: 0x1043b,
        replacement: "𐐓",
    },
    TitleMapping {
        code: 0x1043c,
        replacement: "𐐔",
    },
    TitleMapping {
        code: 0x1043d,
        replacement: "𐐕",
    },
    TitleMapping {
        code: 0x1043e,
        replacement: "𐐖",
    },
    TitleMapping {
        code: 0x1043f,
        replacement: "𐐗",
    },
    TitleMapping {
        code: 0x10440,
        replacement: "𐐘",
    },
    TitleMapping {
        code: 0x10441,
        replacement: "𐐙",
    },
    TitleMapping {
        code: 0x10442,
        replacement: "𐐚",
    },
    TitleMapping {
        code: 0x10443,
        replacement: "𐐛",
    },
    TitleMapping {
        code: 0x10444,
        replacement: "𐐜",
    },
    TitleMapping {
        code: 0x10445,
        replacement: "𐐝",
    },
    TitleMapping {
        code: 0x10446,
        replacement: "𐐞",
    },
    TitleMapping {
        code: 0x10447,
        replacement: "𐐟",
    },
    TitleMapping {
        code: 0x10448,
        replacement: "𐐠",
    },
    TitleMapping {
        code: 0x10449,
        replacement: "𐐡",
    },
    TitleMapping {
        code: 0x1044a,
        replacement: "𐐢",
    },
    TitleMapping {
        code: 0x1_044b,
        replacement: "𐐣",
    },
    TitleMapping {
        code: 0x1044c,
        replacement: "𐐤",
    },
    TitleMapping {
        code: 0x1044d,
        replacement: "𐐥",
    },
    TitleMapping {
        code: 0x1044e,
        replacement: "𐐦",
    },
    TitleMapping {
        code: 0x1044f,
        replacement: "𐐧",
    },
    TitleMapping {
        code: 0x104d8,
        replacement: "𐒰",
    },
    TitleMapping {
        code: 0x104d9,
        replacement: "𐒱",
    },
    TitleMapping {
        code: 0x104da,
        replacement: "𐒲",
    },
    TitleMapping {
        code: 0x104db,
        replacement: "𐒳",
    },
    TitleMapping {
        code: 0x104dc,
        replacement: "𐒴",
    },
    TitleMapping {
        code: 0x104dd,
        replacement: "𐒵",
    },
    TitleMapping {
        code: 0x104de,
        replacement: "𐒶",
    },
    TitleMapping {
        code: 0x104df,
        replacement: "𐒷",
    },
    TitleMapping {
        code: 0x104e0,
        replacement: "𐒸",
    },
    TitleMapping {
        code: 0x104e1,
        replacement: "𐒹",
    },
    TitleMapping {
        code: 0x104e2,
        replacement: "𐒺",
    },
    TitleMapping {
        code: 0x104e3,
        replacement: "𐒻",
    },
    TitleMapping {
        code: 0x104e4,
        replacement: "𐒼",
    },
    TitleMapping {
        code: 0x104e5,
        replacement: "𐒽",
    },
    TitleMapping {
        code: 0x104e6,
        replacement: "𐒾",
    },
    TitleMapping {
        code: 0x104e7,
        replacement: "𐒿",
    },
    TitleMapping {
        code: 0x104e8,
        replacement: "𐓀",
    },
    TitleMapping {
        code: 0x104e9,
        replacement: "𐓁",
    },
    TitleMapping {
        code: 0x104ea,
        replacement: "𐓂",
    },
    TitleMapping {
        code: 0x104eb,
        replacement: "𐓃",
    },
    TitleMapping {
        code: 0x104ec,
        replacement: "𐓄",
    },
    TitleMapping {
        code: 0x104ed,
        replacement: "𐓅",
    },
    TitleMapping {
        code: 0x104ee,
        replacement: "𐓆",
    },
    TitleMapping {
        code: 0x104ef,
        replacement: "𐓇",
    },
    TitleMapping {
        code: 0x104f0,
        replacement: "𐓈",
    },
    TitleMapping {
        code: 0x104f1,
        replacement: "𐓉",
    },
    TitleMapping {
        code: 0x104f2,
        replacement: "𐓊",
    },
    TitleMapping {
        code: 0x104f3,
        replacement: "𐓋",
    },
    TitleMapping {
        code: 0x104f4,
        replacement: "𐓌",
    },
    TitleMapping {
        code: 0x104f5,
        replacement: "𐓍",
    },
    TitleMapping {
        code: 0x104f6,
        replacement: "𐓎",
    },
    TitleMapping {
        code: 0x104f7,
        replacement: "𐓏",
    },
    TitleMapping {
        code: 0x104f8,
        replacement: "𐓐",
    },
    TitleMapping {
        code: 0x104f9,
        replacement: "𐓑",
    },
    TitleMapping {
        code: 0x104fa,
        replacement: "𐓒",
    },
    TitleMapping {
        code: 0x104fb,
        replacement: "𐓓",
    },
    TitleMapping {
        code: 0x10597,
        replacement: "𐕰",
    },
    TitleMapping {
        code: 0x10598,
        replacement: "𐕱",
    },
    TitleMapping {
        code: 0x10599,
        replacement: "𐕲",
    },
    TitleMapping {
        code: 0x1059a,
        replacement: "𐕳",
    },
    TitleMapping {
        code: 0x1059b,
        replacement: "𐕴",
    },
    TitleMapping {
        code: 0x1059c,
        replacement: "𐕵",
    },
    TitleMapping {
        code: 0x1059d,
        replacement: "𐕶",
    },
    TitleMapping {
        code: 0x1059e,
        replacement: "𐕷",
    },
    TitleMapping {
        code: 0x1059f,
        replacement: "𐕸",
    },
    TitleMapping {
        code: 0x105a0,
        replacement: "𐕹",
    },
    TitleMapping {
        code: 0x105a1,
        replacement: "𐕺",
    },
    TitleMapping {
        code: 0x105a3,
        replacement: "𐕼",
    },
    TitleMapping {
        code: 0x105a4,
        replacement: "𐕽",
    },
    TitleMapping {
        code: 0x105a5,
        replacement: "𐕾",
    },
    TitleMapping {
        code: 0x105a6,
        replacement: "𐕿",
    },
    TitleMapping {
        code: 0x105a7,
        replacement: "𐖀",
    },
    TitleMapping {
        code: 0x105a8,
        replacement: "𐖁",
    },
    TitleMapping {
        code: 0x105a9,
        replacement: "𐖂",
    },
    TitleMapping {
        code: 0x105aa,
        replacement: "𐖃",
    },
    TitleMapping {
        code: 0x105ab,
        replacement: "𐖄",
    },
    TitleMapping {
        code: 0x105ac,
        replacement: "𐖅",
    },
    TitleMapping {
        code: 0x105ad,
        replacement: "𐖆",
    },
    TitleMapping {
        code: 0x105ae,
        replacement: "𐖇",
    },
    TitleMapping {
        code: 0x105af,
        replacement: "𐖈",
    },
    TitleMapping {
        code: 0x105b0,
        replacement: "𐖉",
    },
    TitleMapping {
        code: 0x105b1,
        replacement: "𐖊",
    },
    TitleMapping {
        code: 0x105b3,
        replacement: "𐖌",
    },
    TitleMapping {
        code: 0x105b4,
        replacement: "𐖍",
    },
    TitleMapping {
        code: 0x105b5,
        replacement: "𐖎",
    },
    TitleMapping {
        code: 0x105b6,
        replacement: "𐖏",
    },
    TitleMapping {
        code: 0x105b7,
        replacement: "𐖐",
    },
    TitleMapping {
        code: 0x105b8,
        replacement: "𐖑",
    },
    TitleMapping {
        code: 0x105b9,
        replacement: "𐖒",
    },
    TitleMapping {
        code: 0x105bb,
        replacement: "𐖔",
    },
    TitleMapping {
        code: 0x105bc,
        replacement: "𐖕",
    },
    TitleMapping {
        code: 0x10cc0,
        replacement: "𐲀",
    },
    TitleMapping {
        code: 0x10cc1,
        replacement: "𐲁",
    },
    TitleMapping {
        code: 0x10cc2,
        replacement: "𐲂",
    },
    TitleMapping {
        code: 0x10cc3,
        replacement: "𐲃",
    },
    TitleMapping {
        code: 0x10cc4,
        replacement: "𐲄",
    },
    TitleMapping {
        code: 0x10cc5,
        replacement: "𐲅",
    },
    TitleMapping {
        code: 0x10cc6,
        replacement: "𐲆",
    },
    TitleMapping {
        code: 0x10cc7,
        replacement: "𐲇",
    },
    TitleMapping {
        code: 0x10cc8,
        replacement: "𐲈",
    },
    TitleMapping {
        code: 0x10cc9,
        replacement: "𐲉",
    },
    TitleMapping {
        code: 0x10cca,
        replacement: "𐲊",
    },
    TitleMapping {
        code: 0x10ccb,
        replacement: "𐲋",
    },
    TitleMapping {
        code: 0x10ccc,
        replacement: "𐲌",
    },
    TitleMapping {
        code: 0x10ccd,
        replacement: "𐲍",
    },
    TitleMapping {
        code: 0x10cce,
        replacement: "𐲎",
    },
    TitleMapping {
        code: 0x10ccf,
        replacement: "𐲏",
    },
    TitleMapping {
        code: 0x10cd0,
        replacement: "𐲐",
    },
    TitleMapping {
        code: 0x10cd1,
        replacement: "𐲑",
    },
    TitleMapping {
        code: 0x10cd2,
        replacement: "𐲒",
    },
    TitleMapping {
        code: 0x10cd3,
        replacement: "𐲓",
    },
    TitleMapping {
        code: 0x10cd4,
        replacement: "𐲔",
    },
    TitleMapping {
        code: 0x10cd5,
        replacement: "𐲕",
    },
    TitleMapping {
        code: 0x10cd6,
        replacement: "𐲖",
    },
    TitleMapping {
        code: 0x10cd7,
        replacement: "𐲗",
    },
    TitleMapping {
        code: 0x10cd8,
        replacement: "𐲘",
    },
    TitleMapping {
        code: 0x10cd9,
        replacement: "𐲙",
    },
    TitleMapping {
        code: 0x10cda,
        replacement: "𐲚",
    },
    TitleMapping {
        code: 0x10cdb,
        replacement: "𐲛",
    },
    TitleMapping {
        code: 0x10cdc,
        replacement: "𐲜",
    },
    TitleMapping {
        code: 0x10cdd,
        replacement: "𐲝",
    },
    TitleMapping {
        code: 0x10cde,
        replacement: "𐲞",
    },
    TitleMapping {
        code: 0x10cdf,
        replacement: "𐲟",
    },
    TitleMapping {
        code: 0x10ce0,
        replacement: "𐲠",
    },
    TitleMapping {
        code: 0x10ce1,
        replacement: "𐲡",
    },
    TitleMapping {
        code: 0x10ce2,
        replacement: "𐲢",
    },
    TitleMapping {
        code: 0x10ce3,
        replacement: "𐲣",
    },
    TitleMapping {
        code: 0x10ce4,
        replacement: "𐲤",
    },
    TitleMapping {
        code: 0x10ce5,
        replacement: "𐲥",
    },
    TitleMapping {
        code: 0x10ce6,
        replacement: "𐲦",
    },
    TitleMapping {
        code: 0x10ce7,
        replacement: "𐲧",
    },
    TitleMapping {
        code: 0x10ce8,
        replacement: "𐲨",
    },
    TitleMapping {
        code: 0x10ce9,
        replacement: "𐲩",
    },
    TitleMapping {
        code: 0x10cea,
        replacement: "𐲪",
    },
    TitleMapping {
        code: 0x10ceb,
        replacement: "𐲫",
    },
    TitleMapping {
        code: 0x10cec,
        replacement: "𐲬",
    },
    TitleMapping {
        code: 0x10ced,
        replacement: "𐲭",
    },
    TitleMapping {
        code: 0x10cee,
        replacement: "𐲮",
    },
    TitleMapping {
        code: 0x10cef,
        replacement: "𐲯",
    },
    TitleMapping {
        code: 0x10cf0,
        replacement: "𐲰",
    },
    TitleMapping {
        code: 0x10cf1,
        replacement: "𐲱",
    },
    TitleMapping {
        code: 0x10cf2,
        replacement: "𐲲",
    },
    TitleMapping {
        code: 0x118c0,
        replacement: "𑢠",
    },
    TitleMapping {
        code: 0x118c1,
        replacement: "𑢡",
    },
    TitleMapping {
        code: 0x118c2,
        replacement: "𑢢",
    },
    TitleMapping {
        code: 0x118c3,
        replacement: "𑢣",
    },
    TitleMapping {
        code: 0x118c4,
        replacement: "𑢤",
    },
    TitleMapping {
        code: 0x118c5,
        replacement: "𑢥",
    },
    TitleMapping {
        code: 0x118c6,
        replacement: "𑢦",
    },
    TitleMapping {
        code: 0x118c7,
        replacement: "𑢧",
    },
    TitleMapping {
        code: 0x118c8,
        replacement: "𑢨",
    },
    TitleMapping {
        code: 0x118c9,
        replacement: "𑢩",
    },
    TitleMapping {
        code: 0x118ca,
        replacement: "𑢪",
    },
    TitleMapping {
        code: 0x118cb,
        replacement: "𑢫",
    },
    TitleMapping {
        code: 0x118cc,
        replacement: "𑢬",
    },
    TitleMapping {
        code: 0x118cd,
        replacement: "𑢭",
    },
    TitleMapping {
        code: 0x118ce,
        replacement: "𑢮",
    },
    TitleMapping {
        code: 0x118cf,
        replacement: "𑢯",
    },
    TitleMapping {
        code: 0x118d0,
        replacement: "𑢰",
    },
    TitleMapping {
        code: 0x118d1,
        replacement: "𑢱",
    },
    TitleMapping {
        code: 0x118d2,
        replacement: "𑢲",
    },
    TitleMapping {
        code: 0x118d3,
        replacement: "𑢳",
    },
    TitleMapping {
        code: 0x118d4,
        replacement: "𑢴",
    },
    TitleMapping {
        code: 0x118d5,
        replacement: "𑢵",
    },
    TitleMapping {
        code: 0x118d6,
        replacement: "𑢶",
    },
    TitleMapping {
        code: 0x118d7,
        replacement: "𑢷",
    },
    TitleMapping {
        code: 0x118d8,
        replacement: "𑢸",
    },
    TitleMapping {
        code: 0x118d9,
        replacement: "𑢹",
    },
    TitleMapping {
        code: 0x118da,
        replacement: "𑢺",
    },
    TitleMapping {
        code: 0x118db,
        replacement: "𑢻",
    },
    TitleMapping {
        code: 0x118dc,
        replacement: "𑢼",
    },
    TitleMapping {
        code: 0x118dd,
        replacement: "𑢽",
    },
    TitleMapping {
        code: 0x118de,
        replacement: "𑢾",
    },
    TitleMapping {
        code: 0x118df,
        replacement: "𑢿",
    },
    TitleMapping {
        code: 0x16e60,
        replacement: "𖹀",
    },
    TitleMapping {
        code: 0x16e61,
        replacement: "𖹁",
    },
    TitleMapping {
        code: 0x16e62,
        replacement: "𖹂",
    },
    TitleMapping {
        code: 0x16e63,
        replacement: "𖹃",
    },
    TitleMapping {
        code: 0x16e64,
        replacement: "𖹄",
    },
    TitleMapping {
        code: 0x16e65,
        replacement: "𖹅",
    },
    TitleMapping {
        code: 0x16e66,
        replacement: "𖹆",
    },
    TitleMapping {
        code: 0x16e67,
        replacement: "𖹇",
    },
    TitleMapping {
        code: 0x16e68,
        replacement: "𖹈",
    },
    TitleMapping {
        code: 0x16e69,
        replacement: "𖹉",
    },
    TitleMapping {
        code: 0x16e6a,
        replacement: "𖹊",
    },
    TitleMapping {
        code: 0x16e6b,
        replacement: "𖹋",
    },
    TitleMapping {
        code: 0x16e6c,
        replacement: "𖹌",
    },
    TitleMapping {
        code: 0x16e6d,
        replacement: "𖹍",
    },
    TitleMapping {
        code: 0x16e6e,
        replacement: "𖹎",
    },
    TitleMapping {
        code: 0x16e6f,
        replacement: "𖹏",
    },
    TitleMapping {
        code: 0x16e70,
        replacement: "𖹐",
    },
    TitleMapping {
        code: 0x16e71,
        replacement: "𖹑",
    },
    TitleMapping {
        code: 0x16e72,
        replacement: "𖹒",
    },
    TitleMapping {
        code: 0x16e73,
        replacement: "𖹓",
    },
    TitleMapping {
        code: 0x16e74,
        replacement: "𖹔",
    },
    TitleMapping {
        code: 0x16e75,
        replacement: "𖹕",
    },
    TitleMapping {
        code: 0x16e76,
        replacement: "𖹖",
    },
    TitleMapping {
        code: 0x16e77,
        replacement: "𖹗",
    },
    TitleMapping {
        code: 0x16e78,
        replacement: "𖹘",
    },
    TitleMapping {
        code: 0x16e79,
        replacement: "𖹙",
    },
    TitleMapping {
        code: 0x16e7a,
        replacement: "𖹚",
    },
    TitleMapping {
        code: 0x16e7b,
        replacement: "𖹛",
    },
    TitleMapping {
        code: 0x16e7c,
        replacement: "𖹜",
    },
    TitleMapping {
        code: 0x16e7d,
        replacement: "𖹝",
    },
    TitleMapping {
        code: 0x16e7e,
        replacement: "𖹞",
    },
    TitleMapping {
        code: 0x16e7f,
        replacement: "𖹟",
    },
    TitleMapping {
        code: 0x1e922,
        replacement: "𞤀",
    },
    TitleMapping {
        code: 0x1e923,
        replacement: "𞤁",
    },
    TitleMapping {
        code: 0x1e924,
        replacement: "𞤂",
    },
    TitleMapping {
        code: 0x1e925,
        replacement: "𞤃",
    },
    TitleMapping {
        code: 0x1e926,
        replacement: "𞤄",
    },
    TitleMapping {
        code: 0x1e927,
        replacement: "𞤅",
    },
    TitleMapping {
        code: 0x1e928,
        replacement: "𞤆",
    },
    TitleMapping {
        code: 0x1e929,
        replacement: "𞤇",
    },
    TitleMapping {
        code: 0x1e92a,
        replacement: "𞤈",
    },
    TitleMapping {
        code: 0x1e92b,
        replacement: "𞤉",
    },
    TitleMapping {
        code: 0x1e92c,
        replacement: "𞤊",
    },
    TitleMapping {
        code: 0x1e92d,
        replacement: "𞤋",
    },
    TitleMapping {
        code: 0x1e92e,
        replacement: "𞤌",
    },
    TitleMapping {
        code: 0x1e92f,
        replacement: "𞤍",
    },
    TitleMapping {
        code: 0x1e930,
        replacement: "𞤎",
    },
    TitleMapping {
        code: 0x1e931,
        replacement: "𞤏",
    },
    TitleMapping {
        code: 0x1e932,
        replacement: "𞤐",
    },
    TitleMapping {
        code: 0x1e933,
        replacement: "𞤑",
    },
    TitleMapping {
        code: 0x1e934,
        replacement: "𞤒",
    },
    TitleMapping {
        code: 0x1e935,
        replacement: "𞤓",
    },
    TitleMapping {
        code: 0x1e936,
        replacement: "𞤔",
    },
    TitleMapping {
        code: 0x1e937,
        replacement: "𞤕",
    },
    TitleMapping {
        code: 0x1e938,
        replacement: "𞤖",
    },
    TitleMapping {
        code: 0x1e939,
        replacement: "𞤗",
    },
    TitleMapping {
        code: 0x1e93a,
        replacement: "𞤘",
    },
    TitleMapping {
        code: 0x1e93b,
        replacement: "𞤙",
    },
    TitleMapping {
        code: 0x1e93c,
        replacement: "𞤚",
    },
    TitleMapping {
        code: 0x1e93d,
        replacement: "𞤛",
    },
    TitleMapping {
        code: 0x1e93e,
        replacement: "𞤜",
    },
    TitleMapping {
        code: 0x1e93f,
        replacement: "𞤝",
    },
    TitleMapping {
        code: 0x1e940,
        replacement: "𞤞",
    },
    TitleMapping {
        code: 0x1e941,
        replacement: "𞤟",
    },
    TitleMapping {
        code: 0x1e942,
        replacement: "𞤠",
    },
    TitleMapping {
        code: 0x1e943,
        replacement: "𞤡",
    },
];
