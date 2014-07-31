// Copyright (c) 2012, Cornell University
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of Chronos nor the names of its contributors may be
//       used to endorse or promote products derived from this software without
//       specific prior written permission.
//
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

// Chronos
#include "chronos_cmp_encode.h"

uint8_t
chronos_cmp_to_byte(chronos_cmp c)
{
    switch (c)
    {
        case CHRONOS_HAPPENS_BEFORE:
            return '<';
        case CHRONOS_HAPPENS_AFTER:
            return '>';
        case CHRONOS_CONCURRENT:
            return '?';
        case CHRONOS_WOULDLOOP:
            return 'O';
        case CHRONOS_NOEXIST:
            return 'X';
        default:
            return 'E';
    }
}

chronos_cmp
byte_to_chronos_cmp(uint8_t b)
{
    switch (b)
    {
        case '<':
            return CHRONOS_HAPPENS_BEFORE;
        case '>':
            return CHRONOS_HAPPENS_AFTER;
        case '?':
            return CHRONOS_CONCURRENT;
        case 'O':
            return CHRONOS_WOULDLOOP;
        case 'X':
        default:
            return CHRONOS_NOEXIST;
    }
}
