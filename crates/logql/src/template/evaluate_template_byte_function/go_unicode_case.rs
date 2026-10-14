// Simple case ranges from pinned Go 1.26.5 unicode/tables.go (Unicode 15.0).
// https://github.com/golang/go/blob/go1.26.5/src/unicode/tables.go
// Source SHA256: 6a88d48746c4b2c471cc04ae40559694b734770212e6f5ea5f1962adb0c0bdb1
// Copyright 2013 The Go Authors. All rights reserved.
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
// * Redistributions of source code must retain the above copyright notice,
//   this list of conditions and the following disclaimer.
// * Redistributions in binary form must reproduce the above copyright notice,
//   this list of conditions and the following disclaimer in the documentation
//   and/or other materials provided with the distribution.
// * Neither the name of Google LLC nor the names of its contributors may be
//   used to endorse or promote products derived from this software without
//   specific prior written permission.
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

#[derive(Clone, Copy)]
struct CaseRange {
    start: u32,
    end: u32,
    deltas: [i32; 3],
}

const RANGES: &[CaseRange] = &[
    CaseRange {
        start: 0x0041,
        end: 0x005A,
        deltas: [0, 32, 0],
    },
    CaseRange {
        start: 0x0061,
        end: 0x007A,
        deltas: [-32, 0, -32],
    },
    CaseRange {
        start: 0x00B5,
        end: 0x00B5,
        deltas: [743, 0, 743],
    },
    CaseRange {
        start: 0x00C0,
        end: 0x00D6,
        deltas: [0, 32, 0],
    },
    CaseRange {
        start: 0x00D8,
        end: 0x00DE,
        deltas: [0, 32, 0],
    },
    CaseRange {
        start: 0x00E0,
        end: 0x00F6,
        deltas: [-32, 0, -32],
    },
    CaseRange {
        start: 0x00F8,
        end: 0x00FE,
        deltas: [-32, 0, -32],
    },
    CaseRange {
        start: 0x00FF,
        end: 0x00FF,
        deltas: [121, 0, 121],
    },
    CaseRange {
        start: 0x0100,
        end: 0x012F,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x0130,
        end: 0x0130,
        deltas: [0, -199, 0],
    },
    CaseRange {
        start: 0x0131,
        end: 0x0131,
        deltas: [-232, 0, -232],
    },
    CaseRange {
        start: 0x0132,
        end: 0x0137,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x0139,
        end: 0x0148,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x014A,
        end: 0x0177,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x0178,
        end: 0x0178,
        deltas: [0, -121, 0],
    },
    CaseRange {
        start: 0x0179,
        end: 0x017E,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x017F,
        end: 0x017F,
        deltas: [-300, 0, -300],
    },
    CaseRange {
        start: 0x0180,
        end: 0x0180,
        deltas: [195, 0, 195],
    },
    CaseRange {
        start: 0x0181,
        end: 0x0181,
        deltas: [0, 210, 0],
    },
    CaseRange {
        start: 0x0182,
        end: 0x0185,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x0186,
        end: 0x0186,
        deltas: [0, 206, 0],
    },
    CaseRange {
        start: 0x0187,
        end: 0x0188,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x0189,
        end: 0x018A,
        deltas: [0, 205, 0],
    },
    CaseRange {
        start: 0x018B,
        end: 0x018C,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x018E,
        end: 0x018E,
        deltas: [0, 79, 0],
    },
    CaseRange {
        start: 0x018F,
        end: 0x018F,
        deltas: [0, 202, 0],
    },
    CaseRange {
        start: 0x0190,
        end: 0x0190,
        deltas: [0, 203, 0],
    },
    CaseRange {
        start: 0x0191,
        end: 0x0192,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x0193,
        end: 0x0193,
        deltas: [0, 205, 0],
    },
    CaseRange {
        start: 0x0194,
        end: 0x0194,
        deltas: [0, 207, 0],
    },
    CaseRange {
        start: 0x0195,
        end: 0x0195,
        deltas: [97, 0, 97],
    },
    CaseRange {
        start: 0x0196,
        end: 0x0196,
        deltas: [0, 211, 0],
    },
    CaseRange {
        start: 0x0197,
        end: 0x0197,
        deltas: [0, 209, 0],
    },
    CaseRange {
        start: 0x0198,
        end: 0x0199,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x019A,
        end: 0x019A,
        deltas: [163, 0, 163],
    },
    CaseRange {
        start: 0x019C,
        end: 0x019C,
        deltas: [0, 211, 0],
    },
    CaseRange {
        start: 0x019D,
        end: 0x019D,
        deltas: [0, 213, 0],
    },
    CaseRange {
        start: 0x019E,
        end: 0x019E,
        deltas: [130, 0, 130],
    },
    CaseRange {
        start: 0x019F,
        end: 0x019F,
        deltas: [0, 214, 0],
    },
    CaseRange {
        start: 0x01A0,
        end: 0x01A5,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x01A6,
        end: 0x01A6,
        deltas: [0, 218, 0],
    },
    CaseRange {
        start: 0x01A7,
        end: 0x01A8,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x01A9,
        end: 0x01A9,
        deltas: [0, 218, 0],
    },
    CaseRange {
        start: 0x01AC,
        end: 0x01AD,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x01AE,
        end: 0x01AE,
        deltas: [0, 218, 0],
    },
    CaseRange {
        start: 0x01AF,
        end: 0x01B0,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x01B1,
        end: 0x01B2,
        deltas: [0, 217, 0],
    },
    CaseRange {
        start: 0x01B3,
        end: 0x01B6,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x01B7,
        end: 0x01B7,
        deltas: [0, 219, 0],
    },
    CaseRange {
        start: 0x01B8,
        end: 0x01B9,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x01BC,
        end: 0x01BD,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x01BF,
        end: 0x01BF,
        deltas: [56, 0, 56],
    },
    CaseRange {
        start: 0x01C4,
        end: 0x01C4,
        deltas: [0, 2, 1],
    },
    CaseRange {
        start: 0x01C5,
        end: 0x01C5,
        deltas: [-1, 1, 0],
    },
    CaseRange {
        start: 0x01C6,
        end: 0x01C6,
        deltas: [-2, 0, -1],
    },
    CaseRange {
        start: 0x01C7,
        end: 0x01C7,
        deltas: [0, 2, 1],
    },
    CaseRange {
        start: 0x01C8,
        end: 0x01C8,
        deltas: [-1, 1, 0],
    },
    CaseRange {
        start: 0x01C9,
        end: 0x01C9,
        deltas: [-2, 0, -1],
    },
    CaseRange {
        start: 0x01CA,
        end: 0x01CA,
        deltas: [0, 2, 1],
    },
    CaseRange {
        start: 0x01CB,
        end: 0x01CB,
        deltas: [-1, 1, 0],
    },
    CaseRange {
        start: 0x01CC,
        end: 0x01CC,
        deltas: [-2, 0, -1],
    },
    CaseRange {
        start: 0x01CD,
        end: 0x01DC,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x01DD,
        end: 0x01DD,
        deltas: [-79, 0, -79],
    },
    CaseRange {
        start: 0x01DE,
        end: 0x01EF,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x01F1,
        end: 0x01F1,
        deltas: [0, 2, 1],
    },
    CaseRange {
        start: 0x01F2,
        end: 0x01F2,
        deltas: [-1, 1, 0],
    },
    CaseRange {
        start: 0x01F3,
        end: 0x01F3,
        deltas: [-2, 0, -1],
    },
    CaseRange {
        start: 0x01F4,
        end: 0x01F5,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x01F6,
        end: 0x01F6,
        deltas: [0, -97, 0],
    },
    CaseRange {
        start: 0x01F7,
        end: 0x01F7,
        deltas: [0, -56, 0],
    },
    CaseRange {
        start: 0x01F8,
        end: 0x021F,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x0220,
        end: 0x0220,
        deltas: [0, -130, 0],
    },
    CaseRange {
        start: 0x0222,
        end: 0x0233,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x023A,
        end: 0x023A,
        deltas: [0, 10_795, 0],
    },
    CaseRange {
        start: 0x023B,
        end: 0x023C,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x023D,
        end: 0x023D,
        deltas: [0, -163, 0],
    },
    CaseRange {
        start: 0x023E,
        end: 0x023E,
        deltas: [0, 10_792, 0],
    },
    CaseRange {
        start: 0x023F,
        end: 0x0240,
        deltas: [10_815, 0, 10_815],
    },
    CaseRange {
        start: 0x0241,
        end: 0x0242,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x0243,
        end: 0x0243,
        deltas: [0, -195, 0],
    },
    CaseRange {
        start: 0x0244,
        end: 0x0244,
        deltas: [0, 69, 0],
    },
    CaseRange {
        start: 0x0245,
        end: 0x0245,
        deltas: [0, 71, 0],
    },
    CaseRange {
        start: 0x0246,
        end: 0x024F,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x0250,
        end: 0x0250,
        deltas: [10_783, 0, 10_783],
    },
    CaseRange {
        start: 0x0251,
        end: 0x0251,
        deltas: [10_780, 0, 10_780],
    },
    CaseRange {
        start: 0x0252,
        end: 0x0252,
        deltas: [10_782, 0, 10_782],
    },
    CaseRange {
        start: 0x0253,
        end: 0x0253,
        deltas: [-210, 0, -210],
    },
    CaseRange {
        start: 0x0254,
        end: 0x0254,
        deltas: [-206, 0, -206],
    },
    CaseRange {
        start: 0x0256,
        end: 0x0257,
        deltas: [-205, 0, -205],
    },
    CaseRange {
        start: 0x0259,
        end: 0x0259,
        deltas: [-202, 0, -202],
    },
    CaseRange {
        start: 0x025B,
        end: 0x025B,
        deltas: [-203, 0, -203],
    },
    CaseRange {
        start: 0x025C,
        end: 0x025C,
        deltas: [42_319, 0, 42_319],
    },
    CaseRange {
        start: 0x0260,
        end: 0x0260,
        deltas: [-205, 0, -205],
    },
    CaseRange {
        start: 0x0261,
        end: 0x0261,
        deltas: [42_315, 0, 42_315],
    },
    CaseRange {
        start: 0x0263,
        end: 0x0263,
        deltas: [-207, 0, -207],
    },
    CaseRange {
        start: 0x0265,
        end: 0x0265,
        deltas: [42_280, 0, 42_280],
    },
    CaseRange {
        start: 0x0266,
        end: 0x0266,
        deltas: [42_308, 0, 42_308],
    },
    CaseRange {
        start: 0x0268,
        end: 0x0268,
        deltas: [-209, 0, -209],
    },
    CaseRange {
        start: 0x0269,
        end: 0x0269,
        deltas: [-211, 0, -211],
    },
    CaseRange {
        start: 0x026A,
        end: 0x026A,
        deltas: [42_308, 0, 42_308],
    },
    CaseRange {
        start: 0x026B,
        end: 0x026B,
        deltas: [10_743, 0, 10_743],
    },
    CaseRange {
        start: 0x026C,
        end: 0x026C,
        deltas: [42_305, 0, 42_305],
    },
    CaseRange {
        start: 0x026F,
        end: 0x026F,
        deltas: [-211, 0, -211],
    },
    CaseRange {
        start: 0x0271,
        end: 0x0271,
        deltas: [10_749, 0, 10_749],
    },
    CaseRange {
        start: 0x0272,
        end: 0x0272,
        deltas: [-213, 0, -213],
    },
    CaseRange {
        start: 0x0275,
        end: 0x0275,
        deltas: [-214, 0, -214],
    },
    CaseRange {
        start: 0x027D,
        end: 0x027D,
        deltas: [10_727, 0, 10_727],
    },
    CaseRange {
        start: 0x0280,
        end: 0x0280,
        deltas: [-218, 0, -218],
    },
    CaseRange {
        start: 0x0282,
        end: 0x0282,
        deltas: [42_307, 0, 42_307],
    },
    CaseRange {
        start: 0x0283,
        end: 0x0283,
        deltas: [-218, 0, -218],
    },
    CaseRange {
        start: 0x0287,
        end: 0x0287,
        deltas: [42_282, 0, 42_282],
    },
    CaseRange {
        start: 0x0288,
        end: 0x0288,
        deltas: [-218, 0, -218],
    },
    CaseRange {
        start: 0x0289,
        end: 0x0289,
        deltas: [-69, 0, -69],
    },
    CaseRange {
        start: 0x028A,
        end: 0x028B,
        deltas: [-217, 0, -217],
    },
    CaseRange {
        start: 0x028C,
        end: 0x028C,
        deltas: [-71, 0, -71],
    },
    CaseRange {
        start: 0x0292,
        end: 0x0292,
        deltas: [-219, 0, -219],
    },
    CaseRange {
        start: 0x029D,
        end: 0x029D,
        deltas: [42_261, 0, 42_261],
    },
    CaseRange {
        start: 0x029E,
        end: 0x029E,
        deltas: [42_258, 0, 42_258],
    },
    CaseRange {
        start: 0x0345,
        end: 0x0345,
        deltas: [84, 0, 84],
    },
    CaseRange {
        start: 0x0370,
        end: 0x0373,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x0376,
        end: 0x0377,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x037B,
        end: 0x037D,
        deltas: [130, 0, 130],
    },
    CaseRange {
        start: 0x037F,
        end: 0x037F,
        deltas: [0, 116, 0],
    },
    CaseRange {
        start: 0x0386,
        end: 0x0386,
        deltas: [0, 38, 0],
    },
    CaseRange {
        start: 0x0388,
        end: 0x038A,
        deltas: [0, 37, 0],
    },
    CaseRange {
        start: 0x038C,
        end: 0x038C,
        deltas: [0, 64, 0],
    },
    CaseRange {
        start: 0x038E,
        end: 0x038F,
        deltas: [0, 63, 0],
    },
    CaseRange {
        start: 0x0391,
        end: 0x03A1,
        deltas: [0, 32, 0],
    },
    CaseRange {
        start: 0x03A3,
        end: 0x03AB,
        deltas: [0, 32, 0],
    },
    CaseRange {
        start: 0x03AC,
        end: 0x03AC,
        deltas: [-38, 0, -38],
    },
    CaseRange {
        start: 0x03AD,
        end: 0x03AF,
        deltas: [-37, 0, -37],
    },
    CaseRange {
        start: 0x03B1,
        end: 0x03C1,
        deltas: [-32, 0, -32],
    },
    CaseRange {
        start: 0x03C2,
        end: 0x03C2,
        deltas: [-31, 0, -31],
    },
    CaseRange {
        start: 0x03C3,
        end: 0x03CB,
        deltas: [-32, 0, -32],
    },
    CaseRange {
        start: 0x03CC,
        end: 0x03CC,
        deltas: [-64, 0, -64],
    },
    CaseRange {
        start: 0x03CD,
        end: 0x03CE,
        deltas: [-63, 0, -63],
    },
    CaseRange {
        start: 0x03CF,
        end: 0x03CF,
        deltas: [0, 8, 0],
    },
    CaseRange {
        start: 0x03D0,
        end: 0x03D0,
        deltas: [-62, 0, -62],
    },
    CaseRange {
        start: 0x03D1,
        end: 0x03D1,
        deltas: [-57, 0, -57],
    },
    CaseRange {
        start: 0x03D5,
        end: 0x03D5,
        deltas: [-47, 0, -47],
    },
    CaseRange {
        start: 0x03D6,
        end: 0x03D6,
        deltas: [-54, 0, -54],
    },
    CaseRange {
        start: 0x03D7,
        end: 0x03D7,
        deltas: [-8, 0, -8],
    },
    CaseRange {
        start: 0x03D8,
        end: 0x03EF,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x03F0,
        end: 0x03F0,
        deltas: [-86, 0, -86],
    },
    CaseRange {
        start: 0x03F1,
        end: 0x03F1,
        deltas: [-80, 0, -80],
    },
    CaseRange {
        start: 0x03F2,
        end: 0x03F2,
        deltas: [7, 0, 7],
    },
    CaseRange {
        start: 0x03F3,
        end: 0x03F3,
        deltas: [-116, 0, -116],
    },
    CaseRange {
        start: 0x03F4,
        end: 0x03F4,
        deltas: [0, -60, 0],
    },
    CaseRange {
        start: 0x03F5,
        end: 0x03F5,
        deltas: [-96, 0, -96],
    },
    CaseRange {
        start: 0x03F7,
        end: 0x03F8,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x03F9,
        end: 0x03F9,
        deltas: [0, -7, 0],
    },
    CaseRange {
        start: 0x03FA,
        end: 0x03FB,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x03FD,
        end: 0x03FF,
        deltas: [0, -130, 0],
    },
    CaseRange {
        start: 0x0400,
        end: 0x040F,
        deltas: [0, 80, 0],
    },
    CaseRange {
        start: 0x0410,
        end: 0x042F,
        deltas: [0, 32, 0],
    },
    CaseRange {
        start: 0x0430,
        end: 0x044F,
        deltas: [-32, 0, -32],
    },
    CaseRange {
        start: 0x0450,
        end: 0x045F,
        deltas: [-80, 0, -80],
    },
    CaseRange {
        start: 0x0460,
        end: 0x0481,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x048A,
        end: 0x04BF,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x04C0,
        end: 0x04C0,
        deltas: [0, 15, 0],
    },
    CaseRange {
        start: 0x04C1,
        end: 0x04CE,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x04CF,
        end: 0x04CF,
        deltas: [-15, 0, -15],
    },
    CaseRange {
        start: 0x04D0,
        end: 0x052F,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x0531,
        end: 0x0556,
        deltas: [0, 48, 0],
    },
    CaseRange {
        start: 0x0561,
        end: 0x0586,
        deltas: [-48, 0, -48],
    },
    CaseRange {
        start: 0x10A0,
        end: 0x10C5,
        deltas: [0, 7264, 0],
    },
    CaseRange {
        start: 0x10C7,
        end: 0x10C7,
        deltas: [0, 7264, 0],
    },
    CaseRange {
        start: 0x10CD,
        end: 0x10CD,
        deltas: [0, 7264, 0],
    },
    CaseRange {
        start: 0x10D0,
        end: 0x10FA,
        deltas: [3008, 0, 0],
    },
    CaseRange {
        start: 0x10FD,
        end: 0x10FF,
        deltas: [3008, 0, 0],
    },
    CaseRange {
        start: 0x13A0,
        end: 0x13EF,
        deltas: [0, 38_864, 0],
    },
    CaseRange {
        start: 0x13F0,
        end: 0x13F5,
        deltas: [0, 8, 0],
    },
    CaseRange {
        start: 0x13F8,
        end: 0x13FD,
        deltas: [-8, 0, -8],
    },
    CaseRange {
        start: 0x1C80,
        end: 0x1C80,
        deltas: [-6254, 0, -6254],
    },
    CaseRange {
        start: 0x1C81,
        end: 0x1C81,
        deltas: [-6253, 0, -6253],
    },
    CaseRange {
        start: 0x1C82,
        end: 0x1C82,
        deltas: [-6244, 0, -6244],
    },
    CaseRange {
        start: 0x1C83,
        end: 0x1C84,
        deltas: [-6242, 0, -6242],
    },
    CaseRange {
        start: 0x1C85,
        end: 0x1C85,
        deltas: [-6243, 0, -6243],
    },
    CaseRange {
        start: 0x1C86,
        end: 0x1C86,
        deltas: [-6236, 0, -6236],
    },
    CaseRange {
        start: 0x1C87,
        end: 0x1C87,
        deltas: [-6181, 0, -6181],
    },
    CaseRange {
        start: 0x1C88,
        end: 0x1C88,
        deltas: [35_266, 0, 35_266],
    },
    CaseRange {
        start: 0x1C90,
        end: 0x1CBA,
        deltas: [0, -3008, 0],
    },
    CaseRange {
        start: 0x1CBD,
        end: 0x1CBF,
        deltas: [0, -3008, 0],
    },
    CaseRange {
        start: 0x1D79,
        end: 0x1D79,
        deltas: [35_332, 0, 35_332],
    },
    CaseRange {
        start: 0x1D7D,
        end: 0x1D7D,
        deltas: [3814, 0, 3814],
    },
    CaseRange {
        start: 0x1D8E,
        end: 0x1D8E,
        deltas: [35_384, 0, 35_384],
    },
    CaseRange {
        start: 0x1E00,
        end: 0x1E95,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x1E9B,
        end: 0x1E9B,
        deltas: [-59, 0, -59],
    },
    CaseRange {
        start: 0x1E9E,
        end: 0x1E9E,
        deltas: [0, -7615, 0],
    },
    CaseRange {
        start: 0x1EA0,
        end: 0x1EFF,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x1F00,
        end: 0x1F07,
        deltas: [8, 0, 8],
    },
    CaseRange {
        start: 0x1F08,
        end: 0x1F0F,
        deltas: [0, -8, 0],
    },
    CaseRange {
        start: 0x1F10,
        end: 0x1F15,
        deltas: [8, 0, 8],
    },
    CaseRange {
        start: 0x1F18,
        end: 0x1F1D,
        deltas: [0, -8, 0],
    },
    CaseRange {
        start: 0x1F20,
        end: 0x1F27,
        deltas: [8, 0, 8],
    },
    CaseRange {
        start: 0x1F28,
        end: 0x1F2F,
        deltas: [0, -8, 0],
    },
    CaseRange {
        start: 0x1F30,
        end: 0x1F37,
        deltas: [8, 0, 8],
    },
    CaseRange {
        start: 0x1F38,
        end: 0x1F3F,
        deltas: [0, -8, 0],
    },
    CaseRange {
        start: 0x1F40,
        end: 0x1F45,
        deltas: [8, 0, 8],
    },
    CaseRange {
        start: 0x1F48,
        end: 0x1F4D,
        deltas: [0, -8, 0],
    },
    CaseRange {
        start: 0x1F51,
        end: 0x1F51,
        deltas: [8, 0, 8],
    },
    CaseRange {
        start: 0x1F53,
        end: 0x1F53,
        deltas: [8, 0, 8],
    },
    CaseRange {
        start: 0x1F55,
        end: 0x1F55,
        deltas: [8, 0, 8],
    },
    CaseRange {
        start: 0x1F57,
        end: 0x1F57,
        deltas: [8, 0, 8],
    },
    CaseRange {
        start: 0x1F59,
        end: 0x1F59,
        deltas: [0, -8, 0],
    },
    CaseRange {
        start: 0x1F5B,
        end: 0x1F5B,
        deltas: [0, -8, 0],
    },
    CaseRange {
        start: 0x1F5D,
        end: 0x1F5D,
        deltas: [0, -8, 0],
    },
    CaseRange {
        start: 0x1F5F,
        end: 0x1F5F,
        deltas: [0, -8, 0],
    },
    CaseRange {
        start: 0x1F60,
        end: 0x1F67,
        deltas: [8, 0, 8],
    },
    CaseRange {
        start: 0x1F68,
        end: 0x1F6F,
        deltas: [0, -8, 0],
    },
    CaseRange {
        start: 0x1F70,
        end: 0x1F71,
        deltas: [74, 0, 74],
    },
    CaseRange {
        start: 0x1F72,
        end: 0x1F75,
        deltas: [86, 0, 86],
    },
    CaseRange {
        start: 0x1F76,
        end: 0x1F77,
        deltas: [100, 0, 100],
    },
    CaseRange {
        start: 0x1F78,
        end: 0x1F79,
        deltas: [128, 0, 128],
    },
    CaseRange {
        start: 0x1F7A,
        end: 0x1F7B,
        deltas: [112, 0, 112],
    },
    CaseRange {
        start: 0x1F7C,
        end: 0x1F7D,
        deltas: [126, 0, 126],
    },
    CaseRange {
        start: 0x1F80,
        end: 0x1F87,
        deltas: [8, 0, 8],
    },
    CaseRange {
        start: 0x1F88,
        end: 0x1F8F,
        deltas: [0, -8, 0],
    },
    CaseRange {
        start: 0x1F90,
        end: 0x1F97,
        deltas: [8, 0, 8],
    },
    CaseRange {
        start: 0x1F98,
        end: 0x1F9F,
        deltas: [0, -8, 0],
    },
    CaseRange {
        start: 0x1FA0,
        end: 0x1FA7,
        deltas: [8, 0, 8],
    },
    CaseRange {
        start: 0x1FA8,
        end: 0x1FAF,
        deltas: [0, -8, 0],
    },
    CaseRange {
        start: 0x1FB0,
        end: 0x1FB1,
        deltas: [8, 0, 8],
    },
    CaseRange {
        start: 0x1FB3,
        end: 0x1FB3,
        deltas: [9, 0, 9],
    },
    CaseRange {
        start: 0x1FB8,
        end: 0x1FB9,
        deltas: [0, -8, 0],
    },
    CaseRange {
        start: 0x1FBA,
        end: 0x1FBB,
        deltas: [0, -74, 0],
    },
    CaseRange {
        start: 0x1FBC,
        end: 0x1FBC,
        deltas: [0, -9, 0],
    },
    CaseRange {
        start: 0x1FBE,
        end: 0x1FBE,
        deltas: [-7205, 0, -7205],
    },
    CaseRange {
        start: 0x1FC3,
        end: 0x1FC3,
        deltas: [9, 0, 9],
    },
    CaseRange {
        start: 0x1FC8,
        end: 0x1FCB,
        deltas: [0, -86, 0],
    },
    CaseRange {
        start: 0x1FCC,
        end: 0x1FCC,
        deltas: [0, -9, 0],
    },
    CaseRange {
        start: 0x1FD0,
        end: 0x1FD1,
        deltas: [8, 0, 8],
    },
    CaseRange {
        start: 0x1FD8,
        end: 0x1FD9,
        deltas: [0, -8, 0],
    },
    CaseRange {
        start: 0x1FDA,
        end: 0x1FDB,
        deltas: [0, -100, 0],
    },
    CaseRange {
        start: 0x1FE0,
        end: 0x1FE1,
        deltas: [8, 0, 8],
    },
    CaseRange {
        start: 0x1FE5,
        end: 0x1FE5,
        deltas: [7, 0, 7],
    },
    CaseRange {
        start: 0x1FE8,
        end: 0x1FE9,
        deltas: [0, -8, 0],
    },
    CaseRange {
        start: 0x1FEA,
        end: 0x1FEB,
        deltas: [0, -112, 0],
    },
    CaseRange {
        start: 0x1FEC,
        end: 0x1FEC,
        deltas: [0, -7, 0],
    },
    CaseRange {
        start: 0x1FF3,
        end: 0x1FF3,
        deltas: [9, 0, 9],
    },
    CaseRange {
        start: 0x1FF8,
        end: 0x1FF9,
        deltas: [0, -128, 0],
    },
    CaseRange {
        start: 0x1FFA,
        end: 0x1FFB,
        deltas: [0, -126, 0],
    },
    CaseRange {
        start: 0x1FFC,
        end: 0x1FFC,
        deltas: [0, -9, 0],
    },
    CaseRange {
        start: 0x2126,
        end: 0x2126,
        deltas: [0, -7517, 0],
    },
    CaseRange {
        start: 0x212A,
        end: 0x212A,
        deltas: [0, -8383, 0],
    },
    CaseRange {
        start: 0x212B,
        end: 0x212B,
        deltas: [0, -8262, 0],
    },
    CaseRange {
        start: 0x2132,
        end: 0x2132,
        deltas: [0, 28, 0],
    },
    CaseRange {
        start: 0x214E,
        end: 0x214E,
        deltas: [-28, 0, -28],
    },
    CaseRange {
        start: 0x2160,
        end: 0x216F,
        deltas: [0, 16, 0],
    },
    CaseRange {
        start: 0x2170,
        end: 0x217F,
        deltas: [-16, 0, -16],
    },
    CaseRange {
        start: 0x2183,
        end: 0x2184,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x24B6,
        end: 0x24CF,
        deltas: [0, 26, 0],
    },
    CaseRange {
        start: 0x24D0,
        end: 0x24E9,
        deltas: [-26, 0, -26],
    },
    CaseRange {
        start: 0x2C00,
        end: 0x2C2F,
        deltas: [0, 48, 0],
    },
    CaseRange {
        start: 0x2C30,
        end: 0x2C5F,
        deltas: [-48, 0, -48],
    },
    CaseRange {
        start: 0x2C60,
        end: 0x2C61,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x2C62,
        end: 0x2C62,
        deltas: [0, -10_743, 0],
    },
    CaseRange {
        start: 0x2C63,
        end: 0x2C63,
        deltas: [0, -3814, 0],
    },
    CaseRange {
        start: 0x2C64,
        end: 0x2C64,
        deltas: [0, -10_727, 0],
    },
    CaseRange {
        start: 0x2C65,
        end: 0x2C65,
        deltas: [-10_795, 0, -10_795],
    },
    CaseRange {
        start: 0x2C66,
        end: 0x2C66,
        deltas: [-10_792, 0, -10_792],
    },
    CaseRange {
        start: 0x2C67,
        end: 0x2C6C,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x2C6D,
        end: 0x2C6D,
        deltas: [0, -10_780, 0],
    },
    CaseRange {
        start: 0x2C6E,
        end: 0x2C6E,
        deltas: [0, -10_749, 0],
    },
    CaseRange {
        start: 0x2C6F,
        end: 0x2C6F,
        deltas: [0, -10_783, 0],
    },
    CaseRange {
        start: 0x2C70,
        end: 0x2C70,
        deltas: [0, -10_782, 0],
    },
    CaseRange {
        start: 0x2C72,
        end: 0x2C73,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x2C75,
        end: 0x2C76,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x2C7E,
        end: 0x2C7F,
        deltas: [0, -10_815, 0],
    },
    CaseRange {
        start: 0x2C80,
        end: 0x2CE3,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x2CEB,
        end: 0x2CEE,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x2CF2,
        end: 0x2CF3,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0x2D00,
        end: 0x2D25,
        deltas: [-7264, 0, -7264],
    },
    CaseRange {
        start: 0x2D27,
        end: 0x2D27,
        deltas: [-7264, 0, -7264],
    },
    CaseRange {
        start: 0x2D2D,
        end: 0x2D2D,
        deltas: [-7264, 0, -7264],
    },
    CaseRange {
        start: 0xA640,
        end: 0xA66D,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0xA680,
        end: 0xA69B,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0xA722,
        end: 0xA72F,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0xA732,
        end: 0xA76F,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0xA779,
        end: 0xA77C,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0xA77D,
        end: 0xA77D,
        deltas: [0, -35_332, 0],
    },
    CaseRange {
        start: 0xA77E,
        end: 0xA787,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0xA78B,
        end: 0xA78C,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0xA78D,
        end: 0xA78D,
        deltas: [0, -42_280, 0],
    },
    CaseRange {
        start: 0xA790,
        end: 0xA793,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0xA794,
        end: 0xA794,
        deltas: [48, 0, 48],
    },
    CaseRange {
        start: 0xA796,
        end: 0xA7A9,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0xA7AA,
        end: 0xA7AA,
        deltas: [0, -42_308, 0],
    },
    CaseRange {
        start: 0xA7AB,
        end: 0xA7AB,
        deltas: [0, -42_319, 0],
    },
    CaseRange {
        start: 0xA7AC,
        end: 0xA7AC,
        deltas: [0, -42_315, 0],
    },
    CaseRange {
        start: 0xA7AD,
        end: 0xA7AD,
        deltas: [0, -42_305, 0],
    },
    CaseRange {
        start: 0xA7AE,
        end: 0xA7AE,
        deltas: [0, -42_308, 0],
    },
    CaseRange {
        start: 0xA7B0,
        end: 0xA7B0,
        deltas: [0, -42_258, 0],
    },
    CaseRange {
        start: 0xA7B1,
        end: 0xA7B1,
        deltas: [0, -42_282, 0],
    },
    CaseRange {
        start: 0xA7B2,
        end: 0xA7B2,
        deltas: [0, -42_261, 0],
    },
    CaseRange {
        start: 0xA7B3,
        end: 0xA7B3,
        deltas: [0, 928, 0],
    },
    CaseRange {
        start: 0xA7B4,
        end: 0xA7C3,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0xA7C4,
        end: 0xA7C4,
        deltas: [0, -48, 0],
    },
    CaseRange {
        start: 0xA7C5,
        end: 0xA7C5,
        deltas: [0, -42_307, 0],
    },
    CaseRange {
        start: 0xA7C6,
        end: 0xA7C6,
        deltas: [0, -35_384, 0],
    },
    CaseRange {
        start: 0xA7C7,
        end: 0xA7CA,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0xA7D0,
        end: 0xA7D1,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0xA7D6,
        end: 0xA7D9,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0xA7F5,
        end: 0xA7F6,
        deltas: [1_114_112, 1_114_112, 1_114_112],
    },
    CaseRange {
        start: 0xAB53,
        end: 0xAB53,
        deltas: [-928, 0, -928],
    },
    CaseRange {
        start: 0xAB70,
        end: 0xABBF,
        deltas: [-38_864, 0, -38_864],
    },
    CaseRange {
        start: 0xFF21,
        end: 0xFF3A,
        deltas: [0, 32, 0],
    },
    CaseRange {
        start: 0xFF41,
        end: 0xFF5A,
        deltas: [-32, 0, -32],
    },
    CaseRange {
        start: 0x1_0400,
        end: 0x1_0427,
        deltas: [0, 40, 0],
    },
    CaseRange {
        start: 0x1_0428,
        end: 0x1_044F,
        deltas: [-40, 0, -40],
    },
    CaseRange {
        start: 0x1_04B0,
        end: 0x1_04D3,
        deltas: [0, 40, 0],
    },
    CaseRange {
        start: 0x1_04D8,
        end: 0x1_04FB,
        deltas: [-40, 0, -40],
    },
    CaseRange {
        start: 0x1_0570,
        end: 0x1_057A,
        deltas: [0, 39, 0],
    },
    CaseRange {
        start: 0x1_057C,
        end: 0x1_058A,
        deltas: [0, 39, 0],
    },
    CaseRange {
        start: 0x1_058C,
        end: 0x1_0592,
        deltas: [0, 39, 0],
    },
    CaseRange {
        start: 0x1_0594,
        end: 0x1_0595,
        deltas: [0, 39, 0],
    },
    CaseRange {
        start: 0x1_0597,
        end: 0x1_05A1,
        deltas: [-39, 0, -39],
    },
    CaseRange {
        start: 0x1_05A3,
        end: 0x1_05B1,
        deltas: [-39, 0, -39],
    },
    CaseRange {
        start: 0x1_05B3,
        end: 0x1_05B9,
        deltas: [-39, 0, -39],
    },
    CaseRange {
        start: 0x1_05BB,
        end: 0x1_05BC,
        deltas: [-39, 0, -39],
    },
    CaseRange {
        start: 0x1_0C80,
        end: 0x1_0CB2,
        deltas: [0, 64, 0],
    },
    CaseRange {
        start: 0x1_0CC0,
        end: 0x1_0CF2,
        deltas: [-64, 0, -64],
    },
    CaseRange {
        start: 0x1_18A0,
        end: 0x1_18BF,
        deltas: [0, 32, 0],
    },
    CaseRange {
        start: 0x1_18C0,
        end: 0x1_18DF,
        deltas: [-32, 0, -32],
    },
    CaseRange {
        start: 0x1_6E40,
        end: 0x1_6E5F,
        deltas: [0, 32, 0],
    },
    CaseRange {
        start: 0x1_6E60,
        end: 0x1_6E7F,
        deltas: [-32, 0, -32],
    },
    CaseRange {
        start: 0x1_E900,
        end: 0x1_E921,
        deltas: [0, 34, 0],
    },
    CaseRange {
        start: 0x1_E922,
        end: 0x1_E943,
        deltas: [-34, 0, -34],
    },
];

pub(super) fn convert(ch: char, case: usize) -> char {
    let rune = u32::from(ch);
    let index = RANGES.partition_point(|range| range.end < rune);
    let Some(range) = RANGES.get(index).filter(|range| range.start <= rune) else {
        return ch;
    };
    let delta = range.deltas[case];
    let converted = if delta > 0x10_ffff {
        range.start + ((rune - range.start) & !1) + u32::from(case == 1)
    } else {
        rune.wrapping_add_signed(delta)
    };
    char::from_u32(converted).expect("Go case table retains scalar values")
}
